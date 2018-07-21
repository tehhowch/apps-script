'''
Script which performs maintenance functions for the MHCC FusionTable and
initiates a resumable upload to handle the large datasetimport csv
'''
import csv
import random
import time

from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from services import DriveHandler
from services import FusionTableHandler
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
    """
    Routine which prunes out boring Rank DB data.
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
            "SELECT UID, COUNT() FROM {} GROUP BY UID".format(tableId), 0.05)['rows']
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
            print('{} rows had incorrect column lengths:'.format(len(rows_with_errors)))
            print(rows_with_errors)
        if coerced:
            print('{} UIDs were repartitioned to remove decimals.'.format(len(coerced)))
            print(coerced)

        # Inspect the remote table data and ensure each remaining member is represented.
        has_valid_dataset = True
        for row in remote_row_counts:
            if row[0] not in mhcc_members:
                continue
            elif row[0] not in local_row_counts:
                print('Unable to find member with UID=\'{}\' in data to upload'.format(row[0]))
                has_valid_dataset = False
            elif int(row[1]) < local_row_counts[row[0]]:
                print('More rows in upload data than source data for member UID=\'{}\''.format(row[0]))
                has_valid_dataset = False
        revalidation = select_interesting_rank_records(records, rowids=[], tracker={}, indices= {
            'uid': 1, 'ls': 2, 'rt': 3, 'rank':4, 'rowid': 0})
        if len(revalidation) != len(records):
            print('Reanalysis of upload data yielded {} non-interesting rows.'.format(len(records) - len(revalidation)))
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
            handlers['FusionTable'].table.get(tableId=id).execute()
            return True
        except Exception as err:
            print(err)
            return False

    choice = None
    while choice is None:
        typed = input("Enter the table name from above, or a table id: ")
        if len(typed) == 41 and is_valid_fusiontable(typed):
            choice = typed
        elif typed in TABLE_LIST.keys():
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
    #print("Select the rank table")
    #prune_ranks(TABLE_LIST['MHCC Rank DB'], handlers['FusionTables'])
    #print('Select the crown table')
    #prune_crowns(TABLE_LIST['MHCC Crown DB'], handlers['FusionTables'])
