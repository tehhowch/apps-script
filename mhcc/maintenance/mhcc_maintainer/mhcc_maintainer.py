'''
Script which performs maintenance functions for the MHCC FusionTable and
initiates a resumable upload to handle the large datasetimport csv
'''
import csv
import random
import time

from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from services import DriveHandler, FusionTableHandler
from services import HttpError
from services import print_progress_bar as ppb
from services import _write_as_csv as save

LOCAL_KEYS = {}
TABLE_LIST = {}

SCOPES = ['https://www.googleapis.com/auth/fusiontables',
          'https://www.googleapis.com/auth/drive']

def initialize(keys: dict, tables: dict):
    '''Read in the FusionTable IDs and any saved OAuth data/tokens
    '''
    with open('auth.txt', 'r', newline='') as f:
        for line in csv.reader(f, quoting=csv.QUOTE_ALL):
            if line:
                keys[line[0]] = line[1]
    with open('tables.txt', 'r', newline='') as f:
        for line in csv.reader(f, quoting=csv.QUOTE_ALL):
            if line:
                tables[line[0]] = line[1]

    print('Initialized. Found tables: ')
    for key in tables:
        print('\t' + key)



def authorize(local_keys: dict) -> 'Dict[str, GoogleService]':
    '''Authenticate the requested Google API scopes for a single user.
    '''
    def save_credentials(credentials, keys):
        '''Save the given access and refresh tokens to the local disk.
        '''
        keys['access_token'] = credentials.token
        keys['refresh_token'] = credentials.refresh_token
        with open('auth.txt', 'w', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            for key, value in keys.items():
                writer.writerow([key, value])

    print('Checking authorization status...', end='')
    creds = None
    try:
        creds = Credentials(
            local_keys['access_token'],
            refresh_token=local_keys['refresh_token'],
            token_uri="https://accounts.google.com/o/oauth2/token",
            client_id=local_keys['client_id'],
            client_secret=local_keys['client_secret'],
            scopes=SCOPES)
    except KeyError:
        pass
    iapp_flow: InstalledAppFlow = InstalledAppFlow.from_client_secrets_file("client_secret_MHCC.json", SCOPES)
    if not creds or creds.expired or not creds.valid:
        iapp_flow.run_local_server(authorization_prompt_message='opening browser for OAuth flow.')
        creds = iapp_flow.credentials
        save_credentials(creds, local_keys)
    else:
        print('... Credentials OK!')

    drive = DriveHandler(creds)
    print('\nVerifying Drive access by requesting storage quota and user object.')
    drive.verify_drive_service()

    fusiontables = FusionTableHandler(creds)
    print('\nVerifying FusionTables access by requesting tables you\'ve accessed.')
    fusiontables.verify_ft_service()
 
    print('Authorization & service verification completed successfully.')
    return {'FusionTables': fusiontables, 'Drive': drive}



def prune_ranks(tableId: str, ft: FusionTableHandler):
    """Routine which prunes out boring Rank DB data.
    # [Member, UID, LastSeen, RankTime, Rank, MHCC]
    Multiple approaches are possible:
        1) Keep the first and last Ranks for a given LastSeen
        2) Keep the first and last records at a given chronologically observed Rank
        3) Keep the first record for each LastSeen-Rank pairing.
    These approaches differ data removal, and also for whom they target.
        Option 1 will prune many Rank DB records from users who rarely refresh their profile data.
        Option 2 will prune many records from members with relatively fixed rank positions, even
            if they frequently refresh their crown counts
        Option 3 is not as aggressive as options 1 and 2, and is the one implemented.
    """
    def get_records_with_criteria(sql_parts: dict, criteria_values: list, ft: FusionTableHandler) -> dict:
        """Get a set of records that need analysis

    Returns the collective fusiontables#sqlresponse for the set of queries that need to be performed.
    @params
        sql_parts: dict, contains the respective sql clauses, including the logic for assembly with format_map
        criteria_values: list, the collection that needs to be iterated and included.
        ft: FusionTableHandler, an authenticated service handler
    @return: dict, the fusiontables#sqlresponse to the collective query set.
        """
        if not (isinstance(sql_parts, dict) and isinstance(criteria_values, list) and isinstance(ft, FusionTableHandler)):
            raise TypeError('Invalid argument types')
        if not sql_parts or 'assembly' not in sql_parts:
            raise AttributeError('Input sql definition has no reassembly instructions')
        if isinstance(criteria_values[0], list):
            raise NotImplementedError('Paired criteria restriction is not supported.')

        result = {'kind':'fusiontables#sqlresponse', 'is_complete': False}
        margin = ft.remaining_query_length(sql_parts['assembly'].format_map(sql_parts))
        est_size = 0.25 # kB per row estimate
        # Show a progress bar (in case of a slow connection, large query, etc.).
        progress_parameters = {'total': len(criteria_values),
                               'prefix': 'Member data retrieval: ',
                               'length': 50}
        ppb(iteration=0, **progress_parameters)
        while criteria_values:
            queried_criteria = []
            where_str = ','.join(queried_criteria)
            while criteria_values and len(where_str) < margin:
                queried_criteria.append(criteria_values.pop())
                where_str = ','.join(queried_criteria)
            sql_parts['where_values'] = where_str
            response = ft.get_query_result(sql_parts['assembly'].format_map(sql_parts), kb_row_size=est_size)
            ppb(progress_parameters['total'] - len(criteria_values), **progress_parameters)
            if not response: # HttpError, so an API issue or other. Already retried the query twice.
                return result
            if 'columns' not in result: # Write columns once.
                result['columns'] = response['columns']
            if 'rows' not in response or not response['rows']:
                print('Warning: no results for query "{}"'.format(
                    sql_parts['assembly'].format_map(sql_parts)))
            else:
                result.setdefault('rows', []).extend(response['rows'])
                est_size = get_size_estimate(result['rows']) * 2048 / len(result['rows'])

        # Send the dict back to the callee.
        result['is_complete'] = True
        return result

    def select_interesting_rank_records(records: list, rowids: list, indices: dict, tracker: dict) -> list:
        """Add the rowid of interesting records into input, and return a copy with only the interesting records"""
        kept_records = []
        for record in records:
            uid = record[indices['uid']].__str__()
            try:
                ls = int(record[indices['ls']]).__str__()
            except ValueError:
                continue;
            rank = int(record[indices['rank']]).__str__()
            rt = int(record[indices['rt']]).__str__()
            if uid not in tracker:
                tracker[uid] = dict([(ls, {rank})])
            elif ls not in tracker[uid]:
                tracker[uid][ls] = {rank}
            elif rank not in tracker[uid][ls]:
                tracker[uid][ls].add(rank)
            else:
                continue
            rowids.append(record[indices['rowid']].__str__())
            kept_records.append(record[:])
        return kept_records

    def validate_retained_rank_records(tableId: str, records: list, members: list) -> bool:
        """Ensure that the input records do not delete all of any members' data
        """

        if not (isinstance(records, list) and isinstance(records[0], list)):
            return False

        # Get metadata from the target table.
        remote_columns = ft.get_all_columns(tableId)
        remote_row_counts = ft.get_query_result(
            f'SELECT UID, COUNT() FROM {tableId} GROUP BY UID', 0.05)['rows']
        if len(records) < len(remote_row_counts):
            return False

        # Some UIDs may have .0 at the end from the initial column type being Number.
        # Coerce the remote UID columns where this is true to the proper representation.
        for row in remote_row_counts:
            if '.' in row[0]:
                row[0] = row[0].__str__().partition('.')[0]
        assert not [x for x in remote_row_counts if '.' in x[0]]

        mhcc_members = set([x[1] for x in members])
        local_row_counts = {}
        rows_with_errors = []
        coerced = set()
        for row in records:
            # Check that the row is well-formed.
            if len(row) != remote_columns['total']:
                rows_with_errors.append(row)
            # Coerce the UID columns to not have :
            if '.' in row[1]:
                coerced.add(row[1])
                row[1] = row[1].__str__().partition('.')[0]

            # Increment the member's row count.
            try:
                local_row_counts[str(row[1])] += 1
            except KeyError:
                local_row_counts[str(row[1])] = 1
            
        if rows_with_errors:
            print(f'{len(rows_with_errors)} rows had incorrect column lengths:')
            print(rows_with_errors)
        if coerced:
            print(f'{len(coerced)} UIDs were repartitioned to remove decimals.')
            print(coerced)

        # Inspect the remote table data and ensure each remaining member is represented.
        has_valid_dataset = True
        for row in remote_row_counts:
            if row[0] not in mhcc_members:
                continue
            elif row[0] not in local_row_counts:
                print(f'Unable to find member with UID=\'{row[0]}\' in data to upload')
                has_valid_dataset = False
            elif int(row[1]) < local_row_counts[row[0]]:
                print(f'More rows in upload data than source data for member UID=\'{row[0]}\'')
                has_valid_dataset = False
        revalidation = select_interesting_rank_records(records, rowids=[], tracker={}, indices= {
            'uid': 1, 'ls': 2, 'rt': 3, 'rank':4, 'rowid': 0})
        if len(revalidation) != len(records):
            print(f'Reanalysis of upload data yielded {len(records) - len(revalidation)} non-interesting rows.')
            has_valid_dataset = False
        return has_valid_dataset


    if not tableId or not isinstance(tableId, str):
        return

    # Download only the columns necessary to pick records to keep.
    criteria_sql = {'assembly': '{select} {from} {where_start}{where_values}{where_end} {order}',
                    'select': "SELECT ROWID, UID, Rank, LastSeen, RankTime",
                    'from': "FROM " + tableId,
                    'where_start': "WHERE UID IN (",
                    'where_values': '',
                    'where_end': ")",
                    'order': "ORDER BY UID ASC, LastSeen ASC, RankTime ASC"}
    members = ft.get_user_batch()

    # Always requery the target table for rowid data.
    print('Pruning ranks for {} members'.format(len(members)))
    uids = [x[1] for x in members]
    criteria_result = get_records_with_criteria(criteria_sql, uids, ft)
    if not criteria_result['is_complete']:
        print('Criteria querying exited prior to full retrieval. Aborting prune...')
        return
    try:
        criteria_records = criteria_result['rows']
    except KeyError:
        print('No records matching criteria')
        return


    # Analyse the records to get the desired rowids
    rowids = []
    seen = {}
    # Index the columns of the criteria query.
    criteria_indices = {'rowid': 0, 'uid': 1, 'rank': 2, 'ls': 3, 'rt': 4}
    print('Selecting records of interest...')
    interesting_records = select_interesting_rank_records(criteria_records, rowids, criteria_indices, seen)
    if len(interesting_records) == len(criteria_records):
        print('No redundant data detected.')
        return
    print('Found {:,} records to remove from {:,} total records.'.format(
        len(criteria_records) - len(interesting_records), len(criteria_records)))
    
    # Download the records to be kept.
    local_filename = ft.get_filename_for_table(tableId)
    table_data = ([] if not ft.can_use_local_data(tableId, local_filename, handlers['Drive'])
                  else ft.read_local_data(local_filename))
    # Verify record validity
    data_is_valid = (table_data and len(table_data) == len(rowids)
                     and validate_retained_rank_records(tableId, table_data, members))
    if not data_is_valid:
        print('Downloading full records...')
        table_data = ft.get_records_by_rowid(rowids, tableId)
        data_is_valid = validate_retained_rank_records(tableId, table_data, members)

    if not data_is_valid:
        print('Unable to obtain validated data')
        save(table_data, 'invalid_rank_data_snapshot.csv')
        return

    backup = ft.backup_table(tableId)
    if not backup:
        print('Failed to create table backup. Aborting prune...')
        return

    # Do the actual replacement.
    ft.replace_rows(tableId, table_data)
    print('Ranks have been successfully pruned.')



def prune_crowns(tableId: str):
    """
    Routine which prunes out duplicated Crown DB data.
    """
    if not tableId or not isinstance(tableId, str):
        return
    # [Member, UID, LastSeen, LastCrown, LastTouched, B, S, G, MHCC, Squirrel]


def keep_interesting_records(tableId: str):
    '''
    Removes duplicated crown records, keeping each member's records which
    have a new LastSeen value, or a new Rank value.

    @params:
        tableId: str
            The FusionTable ID which should have extraneous records removed.
    '''
    task_start_time = time.perf_counter()
    if not tableId or not isinstance(tableId, str) or len(tableId) != 41:
        raise ValueError('Invalid table id')
    uids = [row[1] for row in get_user_batch(0, 100000)]
    if not uids:
        print('No members returned')
        return
    num_rows = get_total_row_count(tableId)
    rowids = identify_desirable_records(uids, tableId)
    if not rowids:
        print('No rowids received')
        return
    if len(rowids) == num_rows:
        print("All records are interesting")
        return
    local_filename = FusionTableHandler.get_filename_for_table(tableId)
    records_to_keep = ([] if not FusionTableHandler.can_use_local_data(tableId, local_filename)
                       else FusionTableHandler.read_local_data(local_filename))
    have_valid_data = (records_to_keep and len(records_to_keep) == len(rowids)
                       and validate_retrieved_records(records_to_keep, tableId))
    if not have_valid_data:
        # Local data cannot be used, so get the row data associated with rowids.
        records_to_keep = retrieve_whole_records(rowids, tableId)
        # Validate this downloaded data.
        have_valid_data = validate_retrieved_records(records_to_keep, tableId)

    if have_valid_data:
        # Back up the table before we do anything crazy.
        backup_table(tableId)
        # Do something crazy.
        replace_table(tableId, records_to_keep)
    print('Completed "KeepInterestingRecords" task in %s sec.'
          % time.perf_counter() - task_start_time)



def get_size_estimate(values: list, sample_count=5) -> float:
    '''Estimate the bytes in an array by sampling its rows.

    Averages sample_count different rows to improve result statistics.
    Calls __str__() on each element of the sampled rows.

    @params:
        values: list, a list of lists of stringifiable data to be sized
        sample_count: int, the number of array rows to sample.

    @returns: float, the estimated size of the input array, in MB.
    '''
    def __get_row_size(row: list):
        s1 = repr(row).encode('utf-8')
        s2 = ','.join(col.__str__() for col in row).encode('utf-8')
        return max(len(s1), len(s2))

    number_of_rows = len(values)
    if sample_count >= .1 * number_of_rows:
        sample_count = round(.03 * number_of_rows)
    sampled_rows = random.sample(range(number_of_rows), sample_count)
    size_estimate = 0.
    for row_index in sampled_rows:
        size_estimate += __get_row_size(values[row_index])
    size_estimate *= float(number_of_rows) / (1000 * 1000 * sample_count)
    return size_estimate



def replace_table(tableId: str, rows_to_upload: list):
    '''
    Replaces the contents of the identified FusionTable with the new values provided.
    @params:
        tableId: str
            The FusionTable ID of the upload target.

        rows_to_upload: list
            A list of lists of values to upload (e.g. column data).
    '''
    if not isinstance(rows_to_upload, list):
        raise TypeError('Expected value array as list of lists.')
    elif not rows_to_upload:
        raise ValueError('Received empty value array.')
    elif not isinstance(rows_to_upload[0], list):
        raise TypeError('Expected value array as list of lists.')
    if not isinstance(tableId, str):
        raise TypeError('Expected string table id.')
    elif len(tableId) != 41:
        raise ValueError('Table id is not of sufficient length.')
    # Estimate the upload size by averaging the size of several random rows.
    print('Replacing table with id =', tableId)
    approx_size = get_size_estimate(rows_to_upload, 10)
    print('Approx. new upload size =', approx_size, ' MB.')

    start = time.perf_counter()
    rows_to_upload.sort()
    print('Replacement completed in' if replace_rows(tableId, rows_to_upload)
          else 'Replacement failed after', round(time.perf_counter() - start, 1), 'sec.')



def pick_table() -> str:
    '''
    Request user input to determine the FusionTable to operate on.
    @return: str
        Returns the FusionTable id that was chosen by the user.
    '''
    def is_valid_fusiontable(id: str) -> bool:
        try:
            handlers['FusionTables'].table.get(tableId=id).execute()
            return True
        except HttpError as err:
            print(err)
            return False

    choice = None
    while choice is None:
        typed = input("Enter the table name from above, or a table id: ")
        if len(typed) == 41 and is_valid_fusiontable(typed):
            choice = typed
        elif typed in TABLE_LIST:
            choice = TABLE_LIST[typed]
        else:
            print("Unable to use your input.")
    return choice



if __name__ == "__main__":
    initialize(LOCAL_KEYS, TABLE_LIST)
    handlers = authorize(LOCAL_KEYS)
    handlers['FusionTables'].verify_known_tables(TABLE_LIST, handlers['Drive'].get_service())
    handlers['FusionTables'].set_user_table(TABLE_LIST['MHCC Members'])
    #print('Pick a table')
    #table = pick_table()
    table = '1xNi2C5Jfxz8QMkvVitUOgLumz3GTewm5t29hrkUF' # jacks ft id
    #print("Select the rank table")
    #prune_ranks(TABLE_LIST['MHCC Rank DB'], handlers['FusionTables'])
    #print('Select the crown table')
    #prune_crowns(TABLE_LIST['MHCC Crown DB'], handlers['FusionTables'])
    # load jd from hdd
    STRTM_FMT = '%Y-%m-%dT%H:%M:%S.%f%z'
    ft = handlers['FusionTables']
    users = ft.get_user_batch()
    uids = [x[1] for x in users]

    from datetime import datetime
    def loadJacksData():
        path = r'C:\Users\tehhowch\Downloads\MH Latest Crowns.csv'
        with open(path, newline='') as file:
            jacks_data = list(csv.reader(file))
        jacks_cols = jacks_data.pop(0)
        jd = [dict(zip(jacks_cols, [str(x[0]), int(x[1]) * 1000, int(x[2]), int(x[3]), int(x[4])] )) for x in jacks_data]
        return jd

    def getMHCCMembersInJacks(jd: dict):
        mhcc_jd = [x for x in jd if x['snuid'] in uids]
        for m in mhcc_jd:
            name = [x[0] for x in users if x[1] == m['snuid']][0]
            m['name'] = name
        return mhcc_jd

    def parseTimestamps(jd: dict):
        '''Add a naive-tz datetime to each record (for easier human consumption)'''
        for r in jd:
            ms = r['timestamp']
            dt = datetime.utcfromtimestamp(ms//1000).replace(microsecond=ms%1000*1000).strftime(STRTM_FMT)
            r['LastSeen'] = ms
            r['timestamp'] = dt

    def parseMHCCTimes(mhcc: list):
        '''Add a naive-tz datetime to each MHCC record for each MHCC millisecond time value (for easier human consumption)'''
        transformables = set(['LastSeen', 'LastCrown', 'LastTouched', 'RankTime'])
        keys = transformables.intersection(mhcc[0].keys())
        for row in mhcc:
            for k in keys:
                ms = row[k]
                dt = datetime.utcfromtimestamp(ms//1000).replace(microsecond=ms%1000*1000).strftime(STRTM_FMT)
                row[f'{k}_TS'] = dt


    def coerce_to_typed_info(tableId: str, data):
        '''Converts str-only data elements to str, int, or float, in accordance with the FusionTable's
        formatPattern and type for the given column'''
        converter = get_column_mappings(tableId)
        coerced = [{k: converter[k](v) for k, v in x.items()} for x in data]
        return coerced

    def get_rank_data(ranktime_start: str = None, ranktime_end: str = None, uid: str = None):
        ''' For the given restrictions, obtain the corresponding MHCC Rank DB records '''
        tid = TABLE_LIST['MHCC Rank DB']
        sql = f'SELECT rowid, Member, UID, LastSeen, RankTime, Rank, \'MHCC Crowns\' FROM {tid}'
        if any((ranktime_start, ranktime_end, uid)):
            sql += ' WHERE '
        # For any input time strings, convert to the corresponding UTC millis value.
        if ranktime_start:
            ts = datetime.strptime(ranktime_start, STRTM_FMT).timestamp() * 1000
            sql += f'RankTime > {ts}'
        if ranktime_end:
            te = datetime.strptime(ranktime_end, STRTM_FMT).timestamp() * 1000
            if ranktime_start:
                sql += ' and '
            sql += f'RankTime < {te}'
        if uid:
            if ranktime_start or ranktime_end:
                sql += ' and '
            sql += f'UID = {uid}'
        sql += ' ORDER BY UID ASC, RankTime ASC'

        # Download the subset of data from FusionTables
        try:
            rank_data = ft.get_query_result(query=sql, kb_row_size=0.25)
        except HttpError:
            print('Unable to obtain ROWIDs in bulk rank query')
            rank_bytes = ft.query.sqlGet_media(sql=sql).execute()
            rank_data = ft.bytestring_to_queryresult(rank_bytes)
        # Convert from list of lists to list of dicts
        headers = rank_data['columns']
        output = (dict(zip(headers, x)) for x in rank_data['rows'])
        return coerce_to_typed_info(tid, output)

    def get_crown_data(crowntime_start: str = None, crowntime_end: str = None, uid: str = None):
        ''' For the given restrictions, obtain the corresponding MHCC Crown DB records '''
        tid = TABLE_LIST['MHCC Crowns DB']
        sql = f'SELECT rowid, Member, UID, LastSeen, LastCrown, LastTouched, Bronze, Silver, Gold, MHCC, Squirrel FROM {tid}'
        if any((crowntime_start, crowntime_end, uid)):
            sql += ' WHERE '
        # For any input time strings, convert to the corresponding UTC millis value.
        if crowntime_start:
            ts = datetime.strptime(crowntime_start, STRTM_FMT).timestamp() * 1000
            sql += f'LastTouched > {ts}'
        if crowntime_end:
            te = datetime.strptime(crowntime_end, STRTM_FMT).timestamp() * 1000
            if crowntime_start:
                sql += ' and '
            sql += f'LastTouched < {te}'
        if uid:
            if crowntime_start or crowntime_end:
                sql += ' and '
            sql += f'UID = {uid}'
        sql += ' ORDER BY UID ASC, LastTouched ASC'

        # Download the subset of data from FusionTables
        try:
            crown_data = ft.get_query_result(query=sql, kb_row_size=0.25)
        except HttpError:
            print('Unable to obtain ROWIDs in bulk crown query')
            crown_bytes = ft.query.sqlGet_media(sql=sql).execute()
            crown_data = ft.bytestring_to_queryresult(crown_bytes)
        # Convert from list of lists to list of dicts
        headers = crown_data['columns']
        output = (dict(zip(headers, x)) for x in crown_data['rows'])
        return coerce_to_typed_info(tid, output)


    def get_column_mappings(id):
        '''Query the table and determine the appropriate str/int/float type coercion for each column.'''
        def get_as_int(val):
            try:
                return int(val)
            except ValueError:
                try:
                    return int(float(val))
                except ValueError as err:
                    if val == 'NaN':
                        return None
                    raise err

        cols = ft.table.get(tableId=id, fields='columns(columnId,name,type,formatPattern)').execute()['columns']
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
        ''' Find the first record for the given UID in which the value of the given key decreases '''
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

    def get_first_correct(ranks: list, key: str, start: int):
        ''' Find the first record for the given UID in which the value of the given key returns to "normal" '''
        if ranks:
            assert start < len(ranks)
            assert start > 0
            last_key_value = ranks[start - 1][key]
            for i, record in enumerate(ranks[start:], start):
                if record[key] >= last_key_value:
                    return i
        return None

    def get_prune_range(sorted_records: list):
        ''' Return a tuple with the range of the bad data in the input list '''
        # Find the first record where LastSeen decreases
        first_bad = get_first_decrease(sorted_records, 'LastSeen')
        if first_bad is None:
            return None
        # Find the next record that restores the required "LastSeen increasing" property
        first_good = get_first_correct(sorted_records, 'LastSeen', first_bad)
        if first_good is None:
            print(f'All records after {first_bad} are bad ({len(sorted_records)} total records).')
        else:
            assert first_good < len(sorted_records), f'First good index exceeds allowable dimension'
        return (first_bad, first_good)


    def delete_records_by_rowid(service: FusionTableHandler, tableId: str, rowids: list):
        ''' Delete the given records from the given FusionTable. Does not back up the table first.'''
        raw_sql = 'DELETE FROM ' + tableId + ' WHERE ROWID IN ({})'
        max_query_length = service.MAX_GET_QUERY_LENGTH * .75 - 2 * max(len(str(x)) for x in rowids)
        deleted = 0
        while rowids:
            query_ids = [rowids.pop()]
            while rowids and len(raw_sql.format(','.join(query_ids))) < max_query_length:
                query_ids.append(rowids.pop())
            query = raw_sql.format(','.join(query_ids))
            assert len(query) <= service.MAX_GET_QUERY_LENGTH, f'Query too long {len(query)} vs {service.MAX_GET_QUERY_LENGTH}'
            print(f'Estimated remaining time of {len(rowids) / len(query_ids)} sec.')
            resp = service.query.sql(sql=query).execute(num_retries=2)
            deleted += int(resp['rows'][0][0])
            time.sleep(1)
        print(f'Deleted {deleted} rows.')
        return deleted

    def delete_records_by_criteria(service: FusionTableHandler, tableId: str, records):
        ''' Delete the given records by specifying various criteria other than rowid.
        E.g. all bad records for a given UID have the same LastSeen and fall in a certain range of RankTimes / LastTouched'''
        pass


    at_risk = getMHCCMembersInJacks(loadJacksData())
    time_start = '2018-05-29T12:00:00.000000+0000'
    time_end = '2018-11-30T12:00:00.000000+0000'
    ranks = get_rank_data(time_start, time_end)
    #parseMHCCTimes(ranks)
    problem_uids = set(x['snuid'] for x in at_risk)
    print(f'Indexing {len(ranks)} by UID')
    indexed_ranks = {}
    for record in ranks:
        indexed_ranks.setdefault(record['UID'], []).append(record)

    collected_bad_ranks = []
    for i, uid in enumerate(uids):
        member_ranks = indexed_ranks.get(uid, [])
        #member_ranks = get_rank_data(uid=uid) # results in per-user quota violation

        member_ranks.sort(key=sort_by_ranktime)
        indices = get_prune_range(member_ranks)
        while indices is not None:
            # Verify assumption that only members with entries in Jack's Crown DB have bad data
            assert uid in problem_uids, f'{uid} is not in Jacks db'
            to_prune = member_ranks[indices[0] : indices[1]]
            assert len(set(x['LastSeen'] for x in to_prune)) == 1
            print(f'{i *100 / len(uids):>6.2f}%: Found {len(to_prune)} rank records to prune for {uid}')
            collected_bad_ranks.extend(to_prune)
            # It is possible there is more than one set of bad data. Remove all of them.
            del member_ranks[indices[0] : indices[1]]
            indices = get_prune_range(member_ranks)
            if indices is not None:
                print(f'Additional bad ranges found for {member_ranks[0]["Member"]}')
        # Update the stored data to reflect the fixed representation.
        indexed_ranks[uid] = member_ranks

    indexed_bad_data = {}
    if collected_bad_ranks:
        # Write this data to disk (allow avoiding an expensive requery of the table)
        with open('bad_rank_data.csv', 'w', encoding='utf-8', newline='') as file:
            dw = csv.DictWriter(file, fieldnames=list(collected_bad_ranks[0].keys()), quoting=csv.QUOTE_NONNUMERIC)
            dw.writeheader()
            dw.writerows(collected_bad_ranks)

        min_ms = min(x["RankTime"] for x in collected_bad_ranks if x['MHCC Crowns'] > 0)
        print(f'Earliest rank regression was on {datetime.utcfromtimestamp(min_ms//1000).replace(microsecond=min_ms%1000*1000).strftime(STRTM_FMT)}')
        # Create a backup of the rank table
        #backup = ft.backup_table(TABLE_LIST['MHCC Rank DB'])
        #if backup:
            #rowids = [x['rowid'] for x in collected_bad_ranks]
            # Delete the rows with the given ROWIDs

    # Bad data may have additionally accumulated in the Crowns DB that does not quite correspond to that visible via the Rank DB
    # For example, if information from all data sources is added, then the recorded information toggles between the two, but only the most
    # recently added would be presented for inclusion in the Rank DB.
    print(f'Collecting crown data in range {time_start}-{time_end}')
    crowns = get_crown_data(time_start, time_end)
    crown_header_order = [x['name'] for x in ft.get_all_columns(TABLE_LIST['MHCC Crowns DB'])['columns']]
    #parseMHCCTimes(crowns)
    collected_bad_crowns = []
    lastcrown_recalculations = []
    indexed_crowns = {}
    for record in crowns:
        indexed_crowns.setdefault(record['UID'], []).append(record)
    for i, uid in enumerate(uids):
        member_crowns = indexed_crowns.get(uid, [])
        member_crowns.sort(key=sort_by_lasttouched)
        lastcrown_modifications = []

        indices = get_prune_range(member_crowns)
        while indices is not None:
            # Verify assumption that only members with entries in Jack's Crown DB have bad data
            assert uid in problem_uids, f'{uid} is not in Jacks db'
            to_prune = member_crowns[indices[0] : indices[1]]
            assert len(set(x['LastSeen'] for x in to_prune)) == 1
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
        with open('bad_crown_data.csv', 'w', encoding='utf-8', newline='') as file:
            dw = csv.DictWriter(file, fieldnames=list(collected_bad_crowns[0].keys()), quoting=csv.QUOTE_NONNUMERIC)
            dw.writeheader()
            dw.writerows(collected_bad_crowns)

        min_ms = min(x["LastTouched"] for x in collected_bad_crowns if x['MHCC'] > 0)
        print(f'Earliest data regression was on {datetime.utcfromtimestamp(min_ms//1000).replace(microsecond=min_ms%1000*1000).strftime(STRTM_FMT)}')
        
        # Create a backup of the crowns table
        tid = TABLE_LIST['MHCC Crowns DB']
        backup = ft.backup_table(tid)
        if backup:
            # Avoid deletions while the backup is cloning
            while True:
                tasks = ft.get_tasks(backup['tableId'])
                if tasks:
                    print(tasks[0]['type'], tasks[0]['progress'])
                    time.sleep(5)
                else:
                    break

            rowids = [x['rowid'] for x in collected_bad_crowns]
            # Delete the rows with the given ROWIDs
            #delete_records_by_rowid(ft, tid, rowids)

            # Update the associated LastCrown records
            rowids = [x['rowid'] for x in lastcrown_recalculations]
            #delete_records_by_rowid(ft, tid, rowids)
            #ft.import_rows(tid, [x['new_record'] for x in lastcrown_recalculations])
    print('waiting for you to do stuff')
