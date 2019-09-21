import csv
import random
import time
from collections import defaultdict
from datetime import datetime

from services import DriveHandler, FusionTableHandler
from services import HttpError

STRTM_FMT = '%Y-%m-%dT%H:%M:%S.%f%z'

table = '1xNi2C5Jfxz8QMkvVitUOgLumz3GTewm5t29hrkUF' # jacks ft id
ft: FusionTableHandler = None

def loadJacksData(add_LastSeen: bool = False):
    ''' TODO: replace with read from live fusiontable (and associated processing) '''
    def convertTimestampToMillis(jd: dict):
        '''Add a naive-tz datetime to each record (for easier human consumption), and creates a 'LastSeen' field
        for equivalence with MHCC datasets
        '''
        for r in jd:
            ms = r['timestamp']
            dt = datetime.utcfromtimestamp(ms//1000).replace(microsecond=ms%1000*1000).strftime(STRTM_FMT)
            r['LastSeen'] = ms
            r['timestamp'] = dt

    path = r'C:\Users\tehhowch\Downloads\MH Latest Crowns.csv'
    with open(path, newline='') as file:
        jacks_data = list(csv.reader(file))
    headers = jacks_data.pop(0)
    # Annotate the data (list of dicts instead of list of lists)
    # TODO: *flexibly* coerce to typed data
    jd = [dict(zip(headers, [str(x[0]), int(x[1]) * 1000, int(x[2]), int(x[3]), int(x[4])] )) for x in jacks_data]
    if add_LastSeen:
        convertTimestampToMillis(jd)
    return jd

def getMHCCMembersInJacks(users: list, jd: dict):
    mapper = {uid: name for (name, uid) in users}
    mhcc_jd = [x for x in jd if x['snuid'] in mapper]
    for m in mhcc_jd:
        m['name'] = mapper.get(m['snuid'], '')
    return mhcc_jd


def get_sql(headers, tableId: str, order: str,
            criteria_key: str = None, start: str = None, end: str = None):
    '''Generate the appropriate select statement to obtain records that should be checked for data regressions'''
    parts = ['SELECT', ', '.join(headers), f'FROM {tableId}']

    # Add any optional WHERE arguments
    if criteria_key and any((start, end)):
        parts.append('WHERE')
        # For any input time strings, convert to the corresponding UTC millis value.
        if start:
            ts = datetime.strptime(start, STRTM_FMT).timestamp() * 1000
            parts.append(f'{criteria_key} > {ts}')
        if end:
            te = datetime.strptime(end, STRTM_FMT).timestamp() * 1000
            if start:
                parts.append('and')
            parts.append(f'{criteria_key} < {te}')

    # Add the ordering instruction
    parts.append(f'ORDER BY {order}')

    return ' '.join(parts)

def get_table_data(service: FusionTableHandler, tableId: str, sql: str):
    '''Obtain annotated table data as determined from the input SQL'''
    try:
        data = service.get_query_result(query=sql, kb_row_size=0.2)
    except HttpError:
        print('Unable to obtain ROWIDs in bulk query')
        byte_data = service.query.sqlGet_media(sql=sql).execute()
        data = service.bytestring_to_queryresult(byte_data)
    # Convert list of list to list of dicts
    headers = data['columns']
    output = (dict(zip(headers, x)) for x in data['rows'])
    return coerce_to_typed_info(tableId, output)

def coerce_to_typed_info(tableId: str, data):
        '''Converts str-only data elements to str, int, or float, in accordance with the FusionTable's
        formatPattern and type for the given column'''
        converter = get_column_mappings(tableId)
        coerced = [{k: converter[k](v) for k, v in x.items()} for x in data]
        return coerced

def get_as_int(val):
    ''' Function which coerces the input value to an int (or None, if NaN was given) '''
    try:
        return int(val)
    except ValueError:
        try:
            return int(float(val))
        except ValueError as err:
            if val == 'NaN':
                return None
            raise err
    raise TypeError(f'Unknown or unhandled conversion of {val}')

def get_column_mappings(tableId):
    '''Query the table and determine the appropriate str/int/float type coercion for each column.'''

    cols = ft.table.get(tableId=tableId, fields='columns(columnId,name,type,formatPattern)').execute()['columns']
    maps = {'rowid': str}
    for c in cols:
        patt = c['formatPattern']
        cType = c['type'] # NUMBER or STRING (in future, maybe DATETIME)
        if cType == 'NUMBER':
            if patt == 'NUMBER_INTEGER':
                f_func = get_as_int
            else:
                f_func = float
        else:
            f_func = str
        assert c['name'] not in maps # column name must be unique
        maps[c['name']] = f_func
    return maps

def deannotate(headers, record):
    ''' Convert the input record back to a list of lists '''
    output = []
    for col in headers:
        output.append(record[col])
    return output


def sort_by_ranktime(record):
    return record['RankTime']
def sort_by_lasttouched(record):
    return record['LastTouched']

def get_first_decrease(records: list, key: str):
    ''' Find the first record in which the value of the given key decreases.
    Implicitly assumes that the records are appropriately sorted.
    Implicitly assumes that comparing all input records is sensible.
    '''
    if records:
        last_key_value = None
        start = 0
        while last_key_value is None and start < len(records):
            last_key_value = records[start][key]
            start += 1
        if last_key_value is None:
            return None
        for i, record in enumerate(records[start:], start):
            current_key_value = record[key]
            if(last_key_value > current_key_value):
                return i
            else:
                last_key_value = current_key_value
    return None

def get_first_correct(records: list, key: str, start: int):
    '''Find the first record in which the value of the given key returns to "normal"
    Implicitly assumes that the records are appropriately sorted.
    Implicitly assumes that comparing all input records is sensible.
    '''
    if records:
        assert start < len(records), f'Inspection index {start} exceeds maximum dimension {len(records)-1}'
        assert start > 0, f'Unable to obtain required comparison value due to invalid starting index {start}'
        last_key_value = records[start - 1][key]
        for i, record in enumerate(records[start:], start):
            if record[key] >= last_key_value:
                return i
    return None

def get_prune_range(sorted_records: list, based_on_col: str='LastSeen'):
    '''Return a tuple with the range of the bad data in the input list.
    Implicitly assumes the input records are sorted.
    '''
    # Find the first record where the column decreases
    first_bad = get_first_decrease(sorted_records, based_on_col)
    if first_bad is None:
        return None
    # Find the next record that restores the required "<column> is increasing" property
    first_good = get_first_correct(sorted_records, based_on_col, first_bad)
    if first_good is None:
        print(f'All records after {first_bad} are bad ({len(sorted_records)} total records).')
    else:
        assert first_good < len(sorted_records), f'First good index exceeds allowable dimension'
    return (first_bad, first_good)


def _perform_deletion(service: FusionTableHandler, tableId: str, target_rowids: list):
    ''' Delete the rows with the given ROWIDs '''
    num_rows = service.count_rows(tableId)
    target_rows = num_rows - len(target_rowids)
    deleted = service.delete_records_by_rowid(tableId, target_rowids)
    new_count = service.count_rows(tableId)
    if new_count >= num_rows:
        print('Table {tableId} does not have fewer rows', new_count, num_rows)
    if new_count != target_rows:
        print(f'Expected table {tableId} to have {target_rows}, but it has {new_count}')
    print(f'Deleted {deleted} rows from {tableId}')

def clean_rank_regression(service: FusionTableHandler, uids: list, start: str, end: str, filename='bad_rank_data.csv', tableId=''):
    global ft
    ft = service

    print(f'Collecting rank data in range {start} - {end}')
    ranks = get_table_data(service, tableId,
                           get_sql(headers=('rowid', 'Member', 'UID', 'LastSeen', 'RankTime', 'Rank', '\'MHCC Crowns\''),
                                   tableId=tableId, order='UID ASC, RankTime ASC',
                                   criteria_key='RankTime', start=start, end=end))
    print(f'Indexing {len(ranks)} by UID')
    indexed_ranks = defaultdict(list)
    for record in ranks:
        indexed_ranks[record['UID']].append(record)

    collected_bad_ranks = []
    for uid in uids:
        member_ranks: list = indexed_ranks[uid]

        member_ranks.sort(key=sort_by_ranktime)
        indices = get_prune_range(member_ranks)
        while indices is not None:
            to_prune = member_ranks[indices[0] : indices[1]]
            collected_bad_ranks.extend(to_prune)

            # It is possible there is more than one set of bad data. Remove all of them.
            del member_ranks[indices[0] : indices[1]]
            indices = get_prune_range(member_ranks)
        # Update the stored data to reflect the fixed representation.
        indexed_ranks[uid] = member_ranks

    if collected_bad_ranks:
        # Write this data to disk (allow avoiding an expensive requery of the table)
        with open(filename, 'w', encoding='utf-8', newline='') as file:
            dw = csv.DictWriter(file, fieldnames=list(collected_bad_ranks[0].keys()), quoting=csv.QUOTE_NONNUMERIC)
            dw.writeheader()
            dw.writerows(collected_bad_ranks)

        min_ms = min(x["RankTime"] for x in collected_bad_ranks if x['MHCC Crowns'] > 0)
        print(f'Earliest rank regression was on {datetime.utcfromtimestamp(min_ms//1000).replace(microsecond=min_ms%1000*1000).strftime(STRTM_FMT)}')

        # Create a backup of the rank table
        if ft.backup_table(tableId, await_clone=True):
            _perform_deletion(ft, tableId, [x['rowid'] for x in collected_bad_ranks])
        else:
            print('Skipped rank data deletion due to failed backup')
    else:
        print('No detected regressions')

def compute_count_regression_dates(service: FusionTableHandler, start: str, end: str, filename='bad_count_data.csv', tableId=''):
    '''Inspects the given table's data to determine the first instance of a crown total count decreasing, within the window provided.
    Also reports the first spike in totals that corresponds to a restoration of valid data.
    '''
    global ft
    ft = service
    print(f'Collecting crown data in range {start} - {end}')
    crowns = get_table_data(service, tableId,
                            get_sql(headers=('UID', 'LastSeen', 'LastCrown', 'LastTouched', 'Bronze', 'Silver', 'Gold'),
                                    tableId=tableId, order='UID ASC, LastTouched ASC',
                                    criteria_key='LastTouched', start=start, end=end))
    print(f'Indexing {len(crowns)} by UID')
    indexed_counts=defaultdict(list)
    for record in crowns:
        total_crowns = record['Bronze'] + record['Silver'] + record['Gold']
        indexed_counts[record['UID']].append({ 'UID': record['UID'], 'LastSeen': record['LastSeen'], 'LastCrown': record['LastCrown'], 'LastTouched': record['LastTouched'], 'total': total_crowns })

    start_list = []
    for uid in indexed_counts:
        member_rows: list = indexed_counts[uid]
        member_rows.sort(key=sort_by_lasttouched)
        first_bad = get_first_decrease(member_rows, 'total')
        if first_bad is not None:
            start_list.append(member_rows[first_bad])

    print(f'{len(start_list)} members affected')
    first_report = min(x["LastTouched"] for x in start_list)
    print(f'First occurrence: {first_report}\nLast occurrence: {max(x["LastTouched"] for x in start_list)}')

    # Write this data to disk (allow avoiding an expensive requery of the table)
    with open(filename, 'w', encoding='utf-8', newline='') as file:
        dw = csv.DictWriter(file, fieldnames=list(start_list[0].keys()), quoting=csv.QUOTE_NONNUMERIC)
        dw.writeheader()
        dw.writerows(start_list)

    # Report how many rows each member has after the start
    affected_row_count = service.query.sqlGet(sql=f'SELECT COUNT() FROM {tableId} WHERE LastTouched >= {first_report}').execute()
    print(affected_row_count)

def clean_crown_regression(service: FusionTableHandler, uids: list, start: str, end: str, filename='bad_crown_data.csv', tableId=''):
    '''Bad data may have additionally accumulated in the Crowns DB that does not quite correspond to that visible via the Rank DB
    For example, if information from all data sources is added, then the recorded information toggles between the two, but only the most
    recently added would be presented for inclusion in the Rank DB.
    '''
    global ft
    ft = service

    print(f'Collecting crown data in range {start} - {end}')
    crowns = get_table_data(service, tableId,
                            get_sql(headers=('rowid', 'Member', 'UID', 'LastSeen', 'LastCrown', 'LastTouched', 'Bronze', 'Silver', 'Gold', 'MHCC', 'Squirrel'),
                                    tableId=tableId, order='UID ASC, LastTouched ASC',
                                    criteria_key='LastTouched', start=start, end=end))
    print(f'Indexing {len(crowns)} by UID')
    indexed_crowns = defaultdict(list)
    for record in crowns:
        indexed_crowns[record['UID']].append(record)

    crown_header_order = [x['name'] for x in ft.get_all_columns(tableId)['columns']]
    lastcrown_recalculations = []

    collected_bad_crowns = []
    for uid in uids:
        member_crowns: list = indexed_crowns[uid]
        member_crowns.sort(key=sort_by_lasttouched)
        lastcrown_modifications = []

        indices = get_prune_range(member_crowns)
        while indices is not None:
            to_prune = member_crowns[indices[0] : indices[1]]
            collected_bad_crowns.extend(to_prune)

            # Recalculate the "LastCrown" column
            if indices[1] is not None:
                # There is a record to compute with.
                reference = member_crowns[indices[0] - 1]
                modified = member_crowns[indices[1]]
                if all(modified[key] == reference[key] for key in ('Silver', 'Gold', 'MHCC')):
                    modified['LastCrown'] = reference['LastCrown']
                    lastcrown_modifications.append({'rowid': modified['rowid'],
                                                    'new_record': deannotate(crown_header_order, modified)})
                elif modified['LastCrown'] != modified['LastSeen']:
                    print(f'Has new MHCC crowns but not new LastCrown. Updating from {modified["LastCrown"]} to {modified["LastSeen"]}')
                    modified['LastCrown'] = modified['LastSeen']
                    lastcrown_modifications.append({'rowid': modified['rowid'],
                                                    'new_record': deannotate(crown_header_order, modified)})
            # It is possible there is more than one set of bad data. Remove all of them.
            del member_crowns[indices[0] : indices[1]]
            indices = get_prune_range(member_crowns)
        # Update the stored data to reflect the fixed representation.
        indexed_crowns[uid] = member_crowns
        kept_rowids = set(x['rowid'] for x in member_crowns)
        lastcrown_recalculations.extend([x for x in lastcrown_modifications if x['rowid'] in kept_rowids])

    if collected_bad_crowns:
        # Write this data to disk (allow avoiding an expensive requery of the table)
        with open(filename, 'w', encoding='utf-8', newline='') as file:
            dw = csv.DictWriter(file, fieldnames=list(collected_bad_crowns[0].keys()), quoting=csv.QUOTE_NONNUMERIC)
            dw.writeheader()
            dw.writerows(collected_bad_crowns)

        min_ms = min(x["LastTouched"] for x in collected_bad_crowns if x['MHCC'] > 0)
        print(f'Earliest data regression was on {datetime.utcfromtimestamp(min_ms//1000).replace(microsecond=min_ms%1000*1000).strftime(STRTM_FMT)}')

        # Create a backup of the crowns table
        if ft.backup_table(tableId, await_clone=True):
            _perform_deletion(ft, tableId, [x['rowid'] for x in collected_bad_crowns])

            # Update the associated LastCrown records
            ft.delete_records_by_rowid(tableId, [x['rowid'] for x in lastcrown_recalculations])
            added = ft.import_rows(tableId, [x['new_record'] for x in lastcrown_recalculations])
            print(f'{added} of {len(lastcrown_recalculations)} rows with corrected LastCrown values were uploaded.')
        else:
            print('Skipped crown data deletion due to failed backup')
    else:
        print('No detected regressions')
