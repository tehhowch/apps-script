'''
Script which performs maintenance functions for the MHCC FusionTable and
initiates a resumable upload to handle the large datasetimport csv
'''
import csv
import datetime
import json
import os
import random
import time

from googleapiclient.discovery import build
from googleapiclient.http import HttpRequest
from googleapiclient.http import HttpError
from googleapiclient.http import MediaFileUpload
from httplib2 import HttpLib2Error
from oauth2client import tools
from oauth2client.file import Storage
from oauth2client.client import OAuth2WebServerFlow

LOCAL_KEYS = {}
TABLE_LIST = {}

SCOPES = ['https://www.googleapis.com/auth/fusiontables',
          'https://www.googleapis.com/auth/drive']
FusionTables = None
Drive = None

def initialize():
    '''
    Read in the FusionTable IDs and the data for OAuth
    '''
    global LOCAL_KEYS, TABLE_LIST
    strip_chars = ' "\n\r'
    with open('auth.txt') as f:
        for line in f:
            (key, val) = line.split('=')
            LOCAL_KEYS[key.strip(strip_chars)] = val.strip(strip_chars)
    with open('tables.txt', 'r', newline='') as f:
        for line in csv.reader(f, quoting=csv.QUOTE_ALL):
            TABLE_LIST[line[0]] = line[1]

    print('Initialized. Found tables: ')
    for key in TABLE_LIST:
        print('\t' + key)



def authorize():
    '''
    Authenticate the requested Google API scopes for a single user.
    '''
    flow = OAuth2WebServerFlow(LOCAL_KEYS['client_id'], LOCAL_KEYS['client_secret'], SCOPES)
    storage = Storage('credentials.dat')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        print('Reauthorization required... Launching auth flow')
        credentials = tools.run_flow(flow, storage, tools.argparser.parse_args())
    else:
        print('Valid Credentials.')
    global FusionTables
    print('Authorizing...', end='')
    FusionTables = build('fusiontables', 'v2', credentials=credentials)
    if FusionTables is None:
        raise EnvironmentError('FusionTables not authenticated or built as a service.')
    global Drive
    Drive = build('drive', 'v3', credentials=credentials)
    if Drive is None:
        raise EnvironmentError('Drive not authenticated or built as a service.')
    print('Authorized.')



def verify_known_tables():
    '''
    Inspect every table in the tableList dict, to ensure that the table key is valid.
    Invalid table keys (such as those belonging to deleted or trashed files) are removed.
    If a FusionTable is in the trash and owned by the executing user, it is deleted.
    '''
    if not TABLE_LIST or not TABLE_LIST.items():
        print("No known list of tables")
        return

    def read_drive_response(_, response: dict, exception: Exception):
        '''
        Inspects the response from Google Drive API to determine if the table id used references an
        actual existing table.
        '''
        def validate_table(table: dict):
            '''
            A valid table is not trashed.
                @param table a FusionTable resource
                @returns bool
            '''
            # If the table is not trashed, keep it.
            if table['trashed'] is False:
                return True
            # If the table can be deleted, delete it.
            if table['ownedByMe'] is True and table['capabilities']['canDelete'] is True:
                nonlocal batch_delete_requests
                batch_delete_requests.add(FusionTables.table().delete(tableId=table['id']))
            return False

        nonlocal validated_tables, batch_delete_requests
        if exception is None and validate_table(response):
            validated_tables[response['name']] = response['id']

    validated_tables = {}
    kwargs = {
        'fileId': None,
        'fields': 'id,name,trashed,ownedByMe,capabilities/canDelete'}
    # Collect the data requests in a batch request.
    batch_get_requests = Drive.new_batch_http_request(callback=read_drive_response)
    batch_delete_requests = FusionTables.new_batch_http_request(callback=None)
    for table_id in TABLE_LIST.values():
        # Obtain the file as known to Drive.
        kwargs['fileId'] = table_id
        batch_get_requests.add(Drive.files().get(**kwargs))
    batch_get_requests.execute()
    # Delete any of the user's trashed FusionTables.
    batch_delete_requests.execute()
    if validated_tables.items() and len(validated_tables.items()) < len(TABLE_LIST.items()):
        # Rewrite the tables.txt file as CSV.
        print('Rewriting list of known tables (invalid tables have been removed).')
        with open('tables.txt', 'w', newline='') as f:
            tables_to_write = []
            for table_name, table_id in validated_tables.items():
                tables_to_write.append([table_name, table_id])
            csv.writer(f, quoting=csv.QUOTE_ALL).writerows(tables_to_write)



def get_modified_info(file_id: str) -> dict:
    '''
    Acquire the modifiedTime of the referenced table via the Drive service. The version will change
    when almost any part of the table is changed.

    @params:
        file_id: str
            The file ID (table ID) of a file known to Google Drive

    @return: dict
            A dictionary containing the RFC3339 modified timestamp from Google Drive, the
            equivalent tz-aware datetime object, and some minimal file metadata.
    '''
    if not isinstance(file_id, str):
        raise TypeError('Expected string table ID.')
    elif len(file_id) != 41:
        raise ValueError('Received invalid table ID.')
    kwargs = {'fileId': file_id, 'fields': 'id,mimeType,modifiedTime,version'}
    modification_info = {
        'modifiedString': None,
        'modifiedDatetime': None,
        'file': None}
    request = Drive.files().get(**kwargs)
    try:
        fusiontable_file_resource = request.execute()
        file_datetime = datetime.datetime.strptime(
            fusiontable_file_resource['modifiedTime'][:-1] + '+0000', '%Y-%m-%dT%H:%M:%S.%f%z')
    except HttpError as err:
        print('Acquisition of modification info for file id=\'%s\' failed.' % file_id)
        print(err)
    except ValueError as err:
        print('Unable to parse modifiedTime string \'%s\' to tz-aware datetime'
              % fusiontable_file_resource['modifiedTime'])
        print(err)
    else:
        modification_info['modifiedDatetime'] = file_datetime
        modification_info['modifiedString'] = fusiontable_file_resource['modifiedTime']
        modification_info['file'] = fusiontable_file_resource

    return modification_info



def get_query_result(query: str, kb_row_size=1., offset_start=0, max_rows_received=float("inf")) -> dict:
    '''
    Perform a FusionTable query and return the fusiontables#sqlresponse object. If the response
    would be larger than 10 MB, this will perform several requests. The query is assumed to be
    complete, i.e. specifying the target table, and also appendable (i.e. has no existing LIMIT
    or OFFSET parameters).

    @params:
        query: str
            The SQL GET statement (Show, Select, Describe) to execute.

        kb_row_size: float, optional
            The expected size of an individual returned row, in kB.

        offset_start: int, optional
            The global offset into the desired query result.
            i.e. the value normally written after a SQL "OFFSET" descriptor.

        max_rows_received: int, optional
            The global maximum number of records the query should return.
            i.e. the value normally written after a SQL "LIMIT" descriptor.

    @return: dict
            A dictionary conforming to fusiontables#sqlresponse formatting, equivalent to what
            would be returned as though only a single query were made.
    '''
    if not validate_query_is_get(query):
        return {}

    # Multi-query parameters.
    subquery_limit_value = int(9.5 * 1024 / kb_row_size)
    subquery_offset_value = offset_start
    # Eventual return value.
    query_result = {'kind': "fusiontables#sqlresponse"}
    collected_row_data = []
    done = False
    while not done:
        tail = ' '.join(['OFFSET', subquery_offset_value.__str__(),
                         'LIMIT', subquery_limit_value.__str__()])
        request = FusionTables.query().sqlGet(sql=query + ' ' + tail)
        try:
            response = request.execute(num_retries=2)
        except HttpLib2Error as err:
            print('Transport error: ', err, '\nRetrying query.')
        except HttpError as err:
            rq_as_json = json.loads(request.to_json())
            print('Error during query', err, rq_as_json)
            return {}
        else:
            subquery_offset_value += subquery_limit_value
            if 'rows' in response.keys():
                collected_row_data.extend(response['rows'])
            if ('rows' not in response.keys()
                    or len(response['rows']) < subquery_limit_value
                    or len(collected_row_data) >= max_rows_received):
                done = True
            if 'columns' not in query_result.keys() and 'columns' in response.keys():
                query_result['columns'] = response['columns']

    # Ensure that the requested maximum return count is obeyed.
    while len(collected_row_data) > max_rows_received:
        collected_row_data.pop()

    query_result['rows'] = collected_row_data
    return query_result



def validate_query_is_get(query: str) -> bool:
    '''
    Inspect the given query to ensure it is actually a SQL GET query and includes a table.

    @params:
        query: str
            The SQL to be sent to FusionTables via FusionTables.query().sqlGet / .sqlGet_media.

    @return: bool
            Whether or not it is a GET request (vs UPDATE, DELETE, etc.) and includes a table id.
    '''
    l = query.lower()
    if ('select' not in l) and ('show' not in l) and ('describe' not in l):
        return False
    if 'from' not in l:
        return False
    return True



def validate_query_result(query_result: dict) -> bool:
    '''
    Checks the returned query result from FusionTables to ensure it has the minimum value.
    bytestring: a newline separator (i.e. header and a value).
    dictionary: 'rows', 'columns', or 'kind' property (i.e. fusiontables#sqlresponse).

    @params:
        queryResult: bytes, dict
            A response given by a SQL request to FusionTables.query()

    @return: bool
            Whether or not this query result has parsable data.
    '''
    if not query_result:
        return False
    if isinstance(query_result, dict):
        query_keys = query_result.keys()
        # A dictionary query result should have either 'kind', 'rows', or 'columns' keys.
        if 'kind' not in query_keys and 'rows' not in query_keys and 'columns' not in query_keys:
            return False
    # A bytestring query result should have at least one header and one value.
    if isinstance(query_result, bytes) and not query_result.splitlines():
        return False
    return True



def bytestring_to_queryresult(media_input: bytes) -> dict:
    '''
    Convert the received bytestring from an alt=media request into a FusionTables JSON response.

    @params:
        queryResult: bytes
            A bytestring of rectangular input data, with the first row containing column headers.

    @return: dict
            A dictionary conforming to fusiontables#sqlresponse
    '''
    if not media_input or not isinstance(media_input, bytes):
        raise TypeError('Expected bytestring input data')

    query_result = {'kind': "fusiontables#sqlresponse"}
    separator = ','
    result_string = media_input.decode()
    collected_row_data = []
    for row in result_string.splitlines():
        collected_row_data.append(row.split(separator))

    query_result['columns'] = collected_row_data.pop(0)
    query_result['rows'] = collected_row_data
    return query_result



def replace_rows(tableId: str, new_row_data: list):
    '''
    Performs a FusionTables.tables().replaceRows() call to the input table, replacing its contents
    with the input rows.

    @params:
        tableId: str
            The FusionTable to update (String)

        new_row_data: list
            The values to overwrite the FusionTable with (list of lists)

    @return: bool
            Whether or not the indicated FusionTable's rows were replaced with the input rows.
    '''
    if not tableId or not new_row_data:
        return False

    # Create a resumable MediaFileUpload with the "interesting" data to retain.
    sep = ','
    upload = make_media_file(new_row_data, 'staging.csv', True, sep)
    kwargs = {
        'tableId': tableId,
        'media_body': upload,
        'media_mime_type': 'application/octet-stream',
        'encoding': 'UTF-8',
        'delimiter': sep}
    # Try the upload twice (which requires creating a new request).
    if upload and upload.resumable():
        try:
            if not step_upload(FusionTables.table().replaceRows(**kwargs)):
                return step_upload(FusionTables.table().replaceRows(**kwargs))
        except HttpError as err:
            if (err.resp.status in [417]
                    and 'Table will exceed allowed maximum size' in err.__str__()):
                # The goal is to replace the table's rows, so every existing row will be deleted
                # anyway. If the table's current data is too large, such that old + new >= 250,
                # then Error 417 is returned.  Handle this by explicitly deleting the rows first.
                if delete_all_rows(tableId):
                    return step_upload(FusionTables.table().importRows(**kwargs))
                return False

            raise err
    elif upload:
        if not send_whole_upload(FusionTables.table().replaceRows(**kwargs)):
            return send_whole_upload(FusionTables.table().replaceRows(**kwargs))
    return True



def delete_all_rows(tableId: str):
    '''
    Performs a FusionTables.tables().sql(sql=DELETE) operation, with an empty value array.

    @params:
        tableId: str
            The ID of the FusionTable which should have all rows deleted.

    @return: bool
            Whether or not the delete operation succeeded.
    '''
    if not tableId or len(tableId) != 41:
        return False
    kwargs = {'sql': "DELETE FROM " + tableId}
    try:
        response = FusionTables.query().sql(**kwargs).execute()
    except (HttpLib2Error | HttpError) as err:
        print('Error during table deletion', err)
        print(kwargs, response)
        return False

    while True:
        tasks = get_all_tasks(tableId)
        if tasks:
            print(tasks[0]['type'], "progress:", tasks[0]['progress'])
            time.sleep(1)
        else:
            break
    print("Deleted rows:", response['rows'][0][0])
    return True



def get_all_tasks(tableId: str) -> list:
    '''
    Performs as many FusionTables.task().list() queries as is needed to obtain all active tasks for
    the given FusionTable.

    @params:
        tableId: str
            The ID of the FusionTable to be queried for running tasks (such as row deletion).

    @return: list
            A list of all fusiontable#task dicts that are running or scheduled to run.
    '''
    if not isinstance(tableId, str):
        raise TypeError('Expected string table ID.')
    elif len(tableId) != 41:
        raise ValueError('Received invalid table ID \'' + tableId + '\'')
    table_tasks = []
    request = FusionTables.task().list(tableId=tableId)
    while request is not None:
        response = request.execute()
        if 'items' in response.keys():
            table_tasks.extend(response['items'])
            print("Querying tasks.", len(table_tasks), "found so far...")
        request = FusionTables.task().list_next(request, response)
    return table_tasks



def send_whole_upload(request: HttpRequest = None) -> bool:
    '''
    Upload a non-resumable media file.

    @params:
        request: HttpRequest
            An HttpRequest whose MediaUpload might not support next_chunk().

    @return: bool
            Whether or not the upload succeeded.
    '''
    if not request or not isinstance(request, HttpRequest):
        return False

    try:
        request.execute(num_retries=2)
        return True
    except (HttpLib2Error | HttpError) as err:
        print('Upload failed:', err)
        return False



def step_upload(request: HttpRequest = None) -> bool:
    '''
    Print the percentage complete for a given upload while it is executing.

    @params:
        request: HttpRequest
            An HttpRequest that supports next_chunk() (i.e., is resumable).

    @return: bool
            Whether or not the upload succeeded.

    @throws: HttpError 417.
            This error indicates if the FusionTable size limit will be exceeded.
    '''
    if not request or not isinstance(request, HttpRequest):
        return False

    done = None
    fails = 0
    while done is None:
        try:
            status, done = request.next_chunk()
        except HttpLib2Error as err:
            print('Transport error: ', err)
        except HttpError as err:
            print()
            if err.resp.status in [404]:
                return False
            elif err.resp.status in [500, 502, 503, 504] and fails < 5:
                time.sleep(2 ^ fails)
                fails += 1
            elif (err.resp.status in [417] and fails < 5
                  and 'Table will exceed allowed maximum size' in err.__str__()):
                raise err
            else:
                print('Upload failed:', err)
                return False
        else:
            print_progress_bar(status.progress() if status else 1., 1., 'Uploading...', length=50)

    print()
    return True



def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    Refs https://stackoverflow.com/a/34325723
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar_fill = fill * filled_length + '-' * (length - filled_length)
    print('\r%s |%s| %s%% %s' % (prefix, bar_fill, percent, suffix), end='\r')
    # Print New Line on Complete
    if iteration >= total:
        print()



def get_user_batch(start=0, limit=10000) -> list:
    '''
    Get a set of members and their internal MHCC identifiers.

    @params:
        start: int, optional
            The first table row index from which to return user information. The rows are
            sorted ascending by member name.

        limit: int, optional
            The number of MHCC members to be returned in the query.

    @return: list
            A list of lists, with the string values [Member, UID]
    '''
    sql = 'SELECT Member, UID FROM ' + TABLE_LIST['MHCC Members'] + ' ORDER BY Member ASC'
    member_info = get_query_result(sql, .03, start, limit)
    if member_info and 'rows' in member_info:
        print('Fetched basic info for', len(member_info['rows']), 'MHCC members.')
        return member_info['rows']

    print('Received no data from user fetch query.')
    return []



def get_total_row_count(tableId: str) -> int:
    '''
    Queries the size of a table, in terms of rows.

    @params:
        tableId: str
            The identifier of a FusionTable for which to count rows.

    @return: int
            The number of rows in the FusionTable.
    '''
    if not isinstance(tableId, str):
        raise TypeError("Expected string FusionTable identifier")

    count_sql = 'select COUNT() from ' + tableId
    row_count_result = get_query_result(count_sql)
    if row_count_result and 'rows' in row_count_result:
        print('Table with id=%s has %d rows.' % tableId, int(row_count_result['rows'][0][0]))
        return int(row_count_result['rows'][0][0])

    print('Row count query failed for table %s' % tableId)
    return int(0)



def retrieve_whole_records(rowids: list, tableId: str) -> list:
    '''
    Returns a list of lists (i.e. 2D array) corresponding to the full records
    associated with the requested rowids in the specified table.

    @params:
        rowids: list
            The rows in the table to be fully obtained

        tableId: str
            The identifier of a FusionTable to obtain records from

    @return: list
            Complete records from the indicated FusionTable.
    '''
    if not isinstance(rowids, list):
        raise TypeError('Expected list of rowids.')
    elif not rowids:
        raise ValueError('Received empty list of rowids to retrieve.')
    if not isinstance(tableId, str):
        raise TypeError('Expected string table ID.')
    elif len(tableId) != 41:
        raise ValueError('Received invalid table ID.')
    records = []
    num_rows = len(rowids)
    rowids.reverse()
    sql_prefix = 'SELECT * FROM ' + tableId + ' WHERE ROWID IN ('
    print('Retrieving', num_rows, 'records:')
    query_start_time = time.perf_counter()
    print_progress_bar(num_rows - len(rowids), num_rows, "Record retrieval: ", "", 1, 50)
    while rowids:
        sql_suffix = ''
        queried_rowids = []
        query_batch_time = time.monotonic()
        while rowids and (len(sql_prefix + sql_suffix) <= 8000):
            queried_rowids.append(rowids.pop())
            sql_suffix = ','.join(queried_rowids) + ')'
        # Fetch the batch of records.
        resp = get_query_result(''.join([sql_prefix, sql_suffix]), 0.3)
        try:
            records.extend(resp['rows'])
        except KeyError as err:
            print("Received response has no rows:", resp, err)
            return []
        elapsed = time.monotonic() - query_batch_time
        # Rate Limit
        if elapsed < .75:
            time.sleep(.75 - elapsed)
        print_progress_bar(num_rows - len(rowids), num_rows, "Record retrieval: ", "", 1, 50)

    if len(records) != num_rows:
        raise LookupError('Obtained different number of records than specified')
    print('Retrieved', num_rows, 'records in', time.perf_counter() - query_start_time, 'sec.')
    return records



def identify_desirable_records(uids: list, tableId: str) -> list:
    '''
    Returns a list of rowids for which the LastSeen values are unique, or the rank is unique
    (for a given LastSeen), for the given members. Uses SQLGet since only GET is done.

    @params:
        uids: list
            Member identifiers (for whom records should be retrieved).

        tableId: str
            The ID of the FusionTable with records to retrieve.

    @return: list
            The ROWIDs of records which should be retrieved.
    '''
    rowids = []
    if not tableId or not isinstance(tableId, str) or len(tableId) != 41:
        raise ValueError('Invalid table ID received.')
    if not uids:
        raise ValueError('Invalid UIDs received.')

    uids.reverse()
    num_removable_rows = 0
    member_count = len(uids)
    # Index the column headers to find the indices of interest.
    ROWID_INDEX = 0
    UID_INDEX = 2
    LASTSEEN_INDEX = 3
    RANK_INDEX = 4

    sql_prefix = 'SELECT ROWID, Member, UID, LastSeen, Rank FROM ' + tableId + ' WHERE UID IN ('
    print('Sifting through %i member\'s stored data.' % member_count)
    print_progress_bar(member_count - len(uids), member_count, "Members sifted: ", "", 1, 50)
    while uids:
        sql_suffix = ''
        query_uids = []
        query_batch_start = time.monotonic()
        while uids and (len(sql_prefix + sql_suffix) <= 8000):
            query_uids.append(uids.pop())
            sql_suffix = ','.join(query_uids) + ') ORDER BY LastSeen ASC'
        resp = get_query_result(''.join([sql_prefix, sql_suffix]), 0.1)
        try:
            query_record_count = len(resp['rows'])
        except KeyError:
            print()
            print('No data in received response', resp)
            return []
        # Each rowid should only occur once (i.e.  a list should be
        # sufficient), but use a Set container just to be sure.
        kept = set()
        seen = {}
        for row in resp['rows']:
            if is_interesting_record(row, UID_INDEX, LASTSEEN_INDEX, RANK_INDEX, seen):
                kept.add(row[ROWID_INDEX])

        # Store these interesting rows for later retrieval.
        rowids.extend(kept)
        num_removable_rows += query_record_count - len(kept)
        elapsed = time.monotonic() - query_batch_start
        if elapsed < .75:
            time.sleep(.75 - elapsed)
        print_progress_bar(member_count - len(uids), member_count, "Members sifted: ", "", 1, 50)

    print('Found %i values to trim from %i records'
          % num_removable_rows, num_removable_rows + len(rowids))
    return rowids



def is_interesting_record(record: list, uidIndex: int, lsIndex: int, rankIndex: int, tracker: dict) -> bool:
    '''
    Identifies which rows of the input are "interesting" and should be kept. Modifies the passed
    "tracker" dictionary to support repeated calls (i.e. while querying to get the input).

    @params:
        record: list
            An individual crown record (or subset) (list)

        uidIndex: int
            The column index for the UID (unsigned)

        lsIndex: int
            The column index for the LastSeen datestamp (unsigned)

        rankIndex: int
            The column index for the Rank value (unsigned)

        tracker: dict
            An object that tracks members, seen dates, and ranks.

    @return: bool
            Whether or not the input record should be uploaded.
    '''
    if not record:
        return False
    if len({uidIndex, lsIndex, rankIndex}) < 3:
        raise ValueError('Different properties given same column index.')

    try:
        uid = record[uidIndex].__str__()
        ls = int(record[lsIndex]).__str__()
        rank = record[rankIndex].__str__()
    except IndexError as err:
        print('Invalid access into record:\n', record, '\n', uidIndex, lsIndex, rankIndex, '\n')
        print(err)
        return False

    # We should keep this record if:
    # 1) It belongs to an as-yet unseen member.
    if uid not in tracker:
        tracker[uid] = dict([(ls, {rank})])
    # 2) It is (the first) record for a given LastSeen datetime.
    elif ls not in tracker[uid]:
        tracker[uid][ls] = {rank}
    # 3) The member has a different rank than previously seen for that LastSeen datetime.
    elif rank not in tracker[uid][ls]:
        tracker[uid][ls].add(rank)
    # Otherwise, we don't care about it.
    else:
        return False

    return True



def validate_retrieved_records(records: list, sourceTableId: str) -> bool:
    '''
    Inspect the given records to ensure that each member with a record in source table has a record
    in the retrieved records, and that the given records satisfy the same uniqueness criteria that
    was used to obtain them from the source table.

    @params:
        records: list
            All records that are to be uploaded to the FusionTable (list of lists)

        sourceTableId: str
            The FusionTable that will be uploaded to.

    @return: bool
            Whether or not the input records are valid and can be uploaded.
    '''
    if not records or not sourceTableId or len(sourceTableId) != 41:
        return False

    # Determine how many rows exist per user in the source table.  Every member with at least 1
    # row should have at least 1 and at most as many rows in the retrieved records list.
    user_row_counts_sql = 'SELECT UID, COUNT(Gold) FROM ' + sourceTableId + ' GROUP BY UID'
    remote_row_counts = get_query_result(user_row_counts_sql, .05)
    mhcc_members = set([row[1] for row in get_user_batch()])

    # Parse the given records into a dict<str, int> object to simplify dataset comparisons.
    local_row_counts = {}
    rows_with_errors = []
    for row in records:
        if len(row) != 12:
            rows_with_errors.append(row)
        # Always count this row, even if it was erroneously too short or too long.
        # The UID is in the 2nd column.  (Because [] does not default-construct, handling
        # KeyError is necessary, as one will occur for the first record of each member.)
        try:
            local_row_counts[str(row[1])] += 1
        except KeyError:
            local_row_counts[str(row[1])] = 1
    if rows_with_errors:
        print(rows_with_errors)

    # Loop the source counts to verify the individual has data in the supplied records.
    is_valid_recordset = True
    for row in remote_row_counts['rows']:
        if int(row[1]) > 0:
            # Validation rules apply only to current members.
            if row[0] not in mhcc_members:
                continue
            # For current members with remote data, removing all their data is unallowable.
            elif row[0] not in local_row_counts:
                print('No rows in upload data for member with ID=\'%s\', expected 1 - %i'
                      % row[0], row[1])
                is_valid_recordset = False
            # The uploaded data should not have 0 rows, or more rows than the source,
            # for each member.
            elif local_row_counts[row[0]] > int(row[1]) or local_row_counts[row[0]] < 1:
                print('Improper row count in upload data for member with ID=\'%s\'' % row[0])
                is_valid_recordset = False

    # Perform the uniqueness checks on the records (all should be "interesting").
    # Index the column headers to find the indices of interest.
    UID_INDEX = 1
    LASTSEEN_INDEX = 2
    RANK_INDEX = 9
    observed_records = {}
    for row in records:
        if not is_interesting_record(row, UID_INDEX, LASTSEEN_INDEX, RANK_INDEX, observed_records):
            is_valid_recordset = False
            break

    print('Validation of new records', 'completed.' if is_valid_recordset else 'failed.')
    return is_valid_recordset



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

    records_to_keep = ([] if not can_use_local_copy(tableId, 'staging.csv')
                       else read_local_copy('staging.csv'))
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



def backup_table(tableId: str) -> dict:
    '''
    Creates a copy of the existing MHCC CrownRecord Database and logs the new table id.
    Does not delete the previous backup (and thus can result in a space quota exception).

    @params:
        tableId: str
            The ID for a FusionTable which should be copied. (str)

    @return: dict
            The minimal metadata for the copied FusionTable (id, name, description).
    '''
    if not tableId:
        raise ValueError("Missing input table ID.")
    kwargs = {
        'tableId': tableId,
        'copyPresentation': True,
        'fields': 'tableId,name,description'}
    backup = FusionTables.table().copy(**kwargs).execute()
    now = datetime.datetime.utcnow()
    desired_name = ('MHCC_CrownHistory_AsOf_' + '-'.join(
        x.__str__() for x in [now.year, now.month, now.day, now.hour, now.minute]))
    backup['name'] = desired_name
    backup['description'] = 'Automatically generated backup of tableId=' + tableId
    kwargs = {
        'tableId': backup['tableId'],
        'body': backup,
        'fields': 'tableId,name,description'}
    FusionTables.table().patch(**kwargs).execute()
    with open('tables.txt', 'a', newline='') as f:
        csv.writer(f, quoting=csv.QUOTE_ALL).writerows([[desired_name, backup['tableId']]])
    print('Backup of table \'%s\' completed; new table logged to disk.' % tableId)
    return backup



def make_media_file(values: list, path: str, is_resumable=None, delimiter=','):
    '''
    Returns a MediaFile with UTF-8 encoding, for use with FusionTable API calls
    that expect a media_body parameter.
    Also creates a hard disk backup (to facilitate the MediaFile creation).
    '''
    make_local_copy(values, path, 'w', delimiter)
    return MediaFileUpload(path, mimetype='application/octet-stream', resumable=is_resumable)



def make_local_copy(values: list, path: str, file_access_mode='w', delimiter=','):
    '''
    Writes the given values to disk in the given location.
    Example path: 'staging.csv' -> write file 'staging.csv' in the script's directory.
    '''
    if (not values) or (not path):
        raise ValueError('Needed both values to save and a path to save.')
    if file_access_mode == 'r':
        raise ValueError('File mode must be write-capable.')
    with open(path, file_access_mode, newline='', encoding='utf-8') as f:
        csv.writer(f, strict=True, delimiter=delimiter, quoting=csv.QUOTE_NONNUMERIC).writerows(values)



def can_use_local_copy(tableId: str, filename: str) -> bool:
    """
    Checks the local directory for the presence of the input file, and if it is present, determines
    its modification time. This modification time is then compared to the modification time of the
    given FusionTable. If the FusionTable was modified more recently than the local data, then the
    records must be reacquired.

    @params:
        tableId: str
            The FusionTable whose content is to be replaced.
        filename: str
            The name of the file in the local directory with data to upload

    @return: bool
        True - The local data can be used to update the indicated FusionTable.
        False - The local data is missing or out-of-date
    """
    if not (isinstance(tableId, str) and isinstance(filename, str)):
        raise TypeError('Expected string filename and string table ID.')
    elif len(tableId) != 41:
        raise ValueError('Received invalid table ID')
    elif not filename:
        raise ValueError('Received invalid filename.')

    # Ensure the file exists.
    try:
        with open(filename, 'r'):
            pass
    except FileNotFoundError:
        return False
    except Exception as err:
        print(err)
        raise err

    # Obtain the last modified time for the FusionTable.
    info = get_modified_info(tableId)
    # Obtain the last modified time for the local file.
    local_mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(filename), datetime.timezone.utc)
    print('FusionTable last modified:\t%s\nlocal data last modified:\t%s'
          % info['modifiedDatetime'], local_mod_time)
    if local_mod_time > info['modifiedDatetime']:
        print('Local saved data modified more recently than remote FusionTable.',
              'Attempting to use saved data.')
        return True
    print('Remote FusionTable modified more recently than local saved data.',
          'Record analysis and download is required.')
    return False



def read_local_copy(csv_filename: str, delimiter=',') -> list:
    '''
    Attempt to load a CSV from the local directory containing the most recently downloaded data.
    If the file exists and the remote FusionTable has not been modified since the data was acquired,
    upload the data instead of re-performing record inspection and download (since the same result
    would be obtained).

    @params:
        csv_filename: str
            The name of the CSV datafile in the local directory.
            Example: "staging.csv"

        delimiter: str, optional
            The CSV file delimiter that was used to write the file.

    @returns: list
            The data read from the local file, or None if there was an error reading it.
    '''
    if not isinstance(csv_filename, str):
        raise TypeError('Expected string filename')
    values_from_disk = []
    try:
        with open(csv_filename, 'r', newline='', encoding='utf-8') as f:
            data_reader = csv.reader(f, strict=True, delimiter=delimiter, quoting=csv.QUOTE_NONNUMERIC)
            values_from_disk = [row for row in data_reader]
        print("Imported data from disk:", len(values_from_disk), "rows.")
        print("Row length:", len(values_from_disk[0]))
    except FileNotFoundError as err:
        print(err)
        return []
    return values_from_disk



def get_size_estimate(values: list, sample_count=5) -> float:
    '''
    Estimates the upload size of a 2D array without needing to make the actual
    upload file. Averages @numSamples different rows to improve result statistics.

    @params:
        values: list
            A list of lists of data to be sized

        sample_count: int
            The number of list elements to sample the size of.

    @returns: float
            The estimated size of the input array, in MB.
    '''
    number_of_rows = len(values)
    if sample_count >= .1 * number_of_rows:
        sample_count = round(.03 * number_of_rows)
    sampled_rows = random.sample(range(number_of_rows), sample_count)
    size_estimate = 0.
    for row in sampled_rows:
        size_estimate += len((','.join(col.__str__() for col in values[row])).encode('utf-8'))
    size_estimate *= float(number_of_rows) / (1024 * 1024 * sample_count)
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
    choice = None
    while choice is None:
        typed = input("Enter the table name from above, or a table id: ")
        if len(typed) == 41:
            choice = typed
        elif typed in TABLE_LIST.keys():
            choice = TABLE_LIST[typed]
        else:
            print("Unable to use your input.")
    return choice



if __name__ == "__main__":
    initialize()
    authorize()
    verify_known_tables()
    # Choose a table and perform maintenance on it.
    keep_interesting_records(pick_table())
