import csv
import datetime
import json
import os
import time
from pprint import pprint

from googleapiclient.discovery import build
from googleapiclient.discovery import Resource
from googleapiclient.http import BatchHttpRequest
from googleapiclient.http import HttpError
from googleapiclient.http import HttpRequest
from googleapiclient.http import MediaFileUpload
from httplib2 import HttpLib2Error

def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█'):
    """Call in a loop to create terminal progress bar
@params:
    iteration: int, the current step.
    total: int, total number of steps.
    prefix: str, inline text before the bar
    suffix: str, inline text after the bar
    decimals: int, number of decimals in the percentage
    length: int, number of characters in the bar
    fill: str, character that fills the bar
Refs https://stackoverflow.com/a/34325723
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar_fill = fill * filled_length + '-' * (length - filled_length)
    print('\r%s |%s| %s%% %s' % (prefix, bar_fill, percent, suffix), end='\r')
    # Print New Line on Complete
    if iteration >= total:
        print()



def _send_whole_upload(request: HttpRequest):
    '''Upload a non-resumable media file.

@params:
    request: HttpRequest, a MediaUpload object.

@return: tuple(bool, whether or not the upload succeeded
               response, the result of the executed request (or None)
    '''
    if not request or not isinstance(request, HttpRequest):
        return (False, None)

    try:
        resp = request.execute(num_retries=2)
        return (True, resp)
    except (HttpLib2Error | HttpError) as err:
        print('Upload failed:', err)
    return (False, None)



def _step_upload(request: HttpRequest):
    '''Print the percentage complete for a given upload while it is executing.

@params:
    request: HttpRequest, supporting next_chunk() (i.e., is resumable).

@return: tuple(bool, whether or not the upload succeeded
               response, the result of the executed request (or None)

@raises: HttpError 417.
        This error indicates if the FusionTable's self size limit will be exceeded.
    '''
    if not request or not isinstance(request, HttpRequest):
        return (False, None)

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
                return (False, None)
            if err.resp.status in [500, 502, 503, 504] and fails < 5:
                time.sleep(2 ^ fails)
                fails += 1
            elif (err.resp.status in [417] and fails < 5
                    and 'Table will exceed allowed maximum size' in err.__str__()):
                raise err
            else:
                print('Upload failed:', err)
                return (False, None)
        else:
            print_progress_bar(status.progress() if status else 1., 1., 'Uploading...', length=50)

    print()
    return (True, done)



def _make_media_file(values: list, path: str, is_resumable=None, delimiter=','):
    '''Returns a MediaFileUpload with UTF-8 encoding.
    
Also creates a hard disk backup (to facilitate the MediaFile creation).
If the upload fails, the backup can be used to avoid re-downloading the input.
    '''
    _write_as_csv(values, path, 'w', delimiter)
    return MediaFileUpload(path, mimetype='application/octet-stream', resumable=is_resumable)



def _write_as_csv(values: list, path: str, file_access_mode='w', delimiter=','):
    '''Writes the given values to disk in the given location.

Example path: 'staging.csv' -> write file 'staging.csv' in the script's directory.
    '''
    if not (values and path):
        raise ValueError('Needed both values to save and a path to save.')
    if not (isinstance(values, list) and isinstance(values[0], list)):
        raise NotImplementedError('Input values must be a list of lists, i.e. a 2D array.')
    if file_access_mode == 'r':
        raise ValueError('File mode must be write-capable.')
    with open(path, file_access_mode, newline='', encoding='utf-8') as f:
        csv.writer(f, strict=True, delimiter=delimiter, quoting=csv.QUOTE_NONNUMERIC).writerows(values)




class GoogleService():
    """Basic authenticated Google API"""

    def __init__(self, API_NAME: str, API_VERSION: str, credentials: 'google.oauth2.credentials.Credentials'):
        self.__service: Resource = build(API_NAME, API_VERSION, credentials=credentials) # type: googleapiclient.discovery.Resource
        self.__API_NAME: str = API_NAME
        self.__API_VERSION: str = API_VERSION
        self.__credentials: google.auth.credentials.Credentials = credentials
        self.__scopes: list = credentials.scopes

    def get_service(self) -> Resource:
        return self.__service
    
    def get_credentials(self):
        return self.__credentials

    def get_scopes(self) -> list:
        return self.__scopes

    def get_api_summary(self):
        return f'{self.__API_NAME}{self.__API_VERSION}'

    def new_batch_http_request(self, **kwargs) -> BatchHttpRequest:
        return self.__service.new_batch_http_request(**kwargs)


class DriveHandler(GoogleService):
    """Authenticated Drive service instance with appropriate methods for my personal use.

Required scopes for this particular class:
    'https://www.googleapis.com/auth/drive'

Full documentation of the actual service available here:
https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/
    """

    def __init__(self, credentials: 'google.auth.credentials.Credentials'):
        """Constructor for a handler class utilizing the Google Drive API
        """
        super().__init__('drive', 'v3', credentials)
        self.files = self.get_service().files()


    def get_modified_info(self, file_id: str) -> dict:
        '''Acquire basic metadata about the given file.
        
    Reads the modifiedTime of the referenced file via the Drive API. The version attribute
    will change when almost any part of the table is changed.

    @params:
        file_id: str, the file ID of a file known to Google Drive

    @return: dict
            A dictionary containing the RFC3339 modified timestamp from Google Drive, the
            equivalent tz-aware datetime object, and some minimal file metadata.
        '''
        if not isinstance(file_id, str):
            raise TypeError('Expected string table ID.')
        elif len(file_id) != 41:
            raise ValueError('Received invalid table ID.')
        kwargs = {'fileId': file_id, 'fields': 'id,mimeType,modifiedTime,version'}
        modification_info = {'modifiedString': None,
                             'modifiedDatetime': None,
                             'file': None}
        request = self.files.get(**kwargs)
        try:
            fusiontable_file_resource = request.execute()
            file_datetime = datetime.datetime.strptime(
                fusiontable_file_resource['modifiedTime'][:-1] + '+0000', '%Y-%m-%dT%H:%M:%S.%f%z')
        except HttpError as err:
            print(f'Acquisition of modification info for file id=\'{file_id}\' failed.')
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


    def verify_drive_service(self):
        """Check for read access to Google Drive

    Requests the About() resource.
        """
        about = self.get_service().about().get(fields="user,storageQuota").execute()
        pprint(about)



class FusionTableHandler(GoogleService):
    """Authenticated FusionTables service instance with appropriate methods for my personal use.

Required scopes for this particular class:
    'https://www.googleapis.com/auth/fusiontables',
    'https://www.googleapis.com/auth/drive'
    
Full documentation of the actual service available here:
https://developers.google.com/resources/api-libraries/documentation/fusiontables/v2/python/latest/
    """
    MAX_GET_QUERY_LENGTH = 7900

    def __init__(self, credentials: 'google.auth.credentials.Credentials'):
        super().__init__('fusiontables', 'v2', credentials)
        # Assign handles for the general resources.
        self.column = self.get_service().column()
        self.query = self.get_service().query()
        self.table = self.get_service().table()
        self.task = self.get_service().task()

    @classmethod
    def remaining_query_length(cls, query: str = '') -> int:
        return cls.MAX_GET_QUERY_LENGTH - len(query)

    @staticmethod
    def validate_query_is_get(query: str) -> bool:
        '''Inspect the given query to ensure it is actually a SQL GET query and includes a table.

    @params:
        query: str, the SQL to be sent to FusionTables via FusionTables.query().sqlGet / .sqlGet_media.

    @return: bool, whether or not it is a GET request (vs UPDATE, DELETE, etc.) and includes a table id.
        '''
        lowercased = query.lower()
        if ('select' not in lowercased) and ('show' not in lowercased) and ('describe' not in lowercased):
            return False
        if 'from' not in lowercased:
            return False
        if FusionTableHandler.remaining_query_length(query) + 50 < 0: # its a flexible limit, really.
            return False
        return True


    @staticmethod
    def validate_query_result(query_result) -> bool:
        '''Checks the returned query result to ensure it has some data.

    bytestring: a newline separator (i.e. header and a value).
    dictionary: 'rows', 'columns', or 'kind' property (i.e. fusiontables#sqlresponse).

    @params:
        queryResult: bytes | dict, the response given by a SQL request to FusionTables.query()

    @return: bool, whether or not this query result has parsable data.
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


    @staticmethod
    def bytestring_to_queryresult(media_input: bytes) -> dict:
        '''Convert the received bytestring (from an alt=media request) to JSON response.

    @params:
        queryResult: bytes, rectangular input data, with the first row containing column headers.

    @return: dict, dictionary conforming to fusiontables#sqlresponse
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


    @staticmethod
    def get_filename_for_table(tableId: str, method: str = '') -> str:
        """Returns the name that would be used to save that table data locally"""
        return f'table_{tableId}{method}.csv'

    # Generic data fetch methods
    def verify_ft_service(self):
        """Check for read access to FusionTables.
    Requests and prints list of the user's FusionTables via the tables().list() method.
        """
        all_tables = []
        request = self.table.list(fields="items(name,tableId,columns/name)")
        while request is not None:
            response = request.execute()
            all_tables.extend(response.get('items', []))
            request = self.table.list_next(request, response)

        print(f'Found {len(all_tables)} FusionTables')
        for table in all_tables:
            table['Columns'] = ', '.join(t['name'] for t in table['columns'])
            print('id: "{tableId}"\tName: "{name}"\nColumns: [{Columns}]\n'.format_map(table))


    def count_rows(self, tableId: str) -> int:
        '''Query the size of a table, in terms of rows.

    @params:
        tableId: str, the target FusionTable's id.

    @return: int, the number of rows in the FusionTable.
        '''
        if not (tableId and isinstance(tableId, str)):
            raise TypeError("Expected string FusionTable identifier")

        count_sql = 'select COUNT() from ' + tableId
        row_count_result = self.get_query_result(count_sql)
        if row_count_result and 'rows' in row_count_result:
            return int(row_count_result['rows'][0][0])

        print(f'Row count query failed for table \'{tableId}\'')
        return int(0)


    def get_tasks(self, tableId: str) -> list:
        '''Obtain all active tasks for the given FusionTable.

    @params:
        tableId: str, the ID of the table to query for running tasks (like row deletion).

    @return: list, all fusiontable#task dicts that are running or scheduled to run.
        '''
        if not (tableId and isinstance(tableId, str)):
            raise TypeError('Expected string table ID.')
        table_tasks = []
        request = self.task.list(tableId=tableId)
        while request is not None:
            response = request.execute()
            table_tasks.extend(response.get('items', []))
            request = self.task.list_next(request, response)

        return table_tasks


    def get_all_columns(self, tableId: str) -> dict:
        """Get all columns for the target table

        Returns an ordered list of the column resources for the given table.
        The list is ordered as the columns are displayed.
        """
        columns = {'columns': [], 'total': 0, 'tableId': tableId}
        kwargs = {'tableId': tableId,
                  'fields': 'nextPageToken,totalItems,items(name,type,columnId,description)'}
        request = self.column.list(**kwargs)
        while request is not None:
            response = request.execute(num_retries=1)
            if 'totalItems' in response:
                columns['total'] = response['totalItems']
            columns['columns'].extend(response.get('items', []))
            request = self.column.list_next(request, response)
        assert len(columns['columns']) == columns['total']
        columns['ids'] = {x['columnId']: x for x in columns['columns']}
        columns['headers'] = [x['name'] for x in columns['columns']]
        return columns


    # MHCC-specific data fetch methods
    def get_user_batch(self, start=0, limit=10000) -> list:
        '''Get a set of members and their internal MHCC identifiers.

    @params:
        start: int, the first table row index from which to return user information.
            Rows are sorted ascending by member name.
        limit: int, max number of MHCC members to be returned in the query.

    @return: list, [Member, UID]
        '''
        sql = f'SELECT Member, UID FROM {self._user_table} ORDER BY Member ASC'
        member_info = self.get_query_result(sql, .03, start, limit)
        if member_info and 'rows' in member_info:
            return member_info['rows']

        print('Received no data from user fetch query.')
        return []


    def get_records_by_rowid(self, rowids: list, tableId: str) -> list:
        '''Download the specified rows from the specified table

    Returns a list of lists (i.e. 2D array) corresponding to the full records (SELECT * FROM ...)
    associated with the requested rowids in the specified table. 

    @params:
        rowids: list[str], the ids of rows to acquire.
        tableId: str, the table to download rows from.

    @return: list, the full contents of the indicated rows
        '''
        if not (rowids and isinstance(rowids, list)):
            raise TypeError('Expected list of rowids.')
        if not isinstance(tableId, str):
            raise TypeError('Expected string table ID.')
        elif len(tableId) != 41:
            raise ValueError('Received invalid table ID.')

        data = {'rows': [], 'requested_row_count': len(rowids),
                'queries': {}, 'to_retry': [], 'total errors': 0}
        # Each row is roughly the same size, depending on the name of the member and
        # the length of their UID. Assumption: UTF-8 (~2B per char), all numbers as char
        # String columns:               Numeric columns
        #     UID: 8 - 16 char              Times: 17 char, 2-3
        #     Squirrel/name: 16 - 30 char   Crown/Ranks: 4 char, 4 max
        # = 91 to 131 characters to be retrieved per row means <<< 1kB per row to transfer.
        # Thus rowid transfer does not require guarding against the 10 MB GET ceiling.

        # Since we use a "delete & replace" maintenance method, the rowids will never be so
        # different in length that implementing a max-packing algorithm is worthwhile.
        sql = {'assembly': '{select} {from} {where_start}{where_values}{where_end}',
               'select': 'SELECT *',
               'from': 'FROM ' + tableId,
               'where_start': 'WHERE ROWID IN (',
               'where_values': '',
               'where_end': ')'}

        # Cache the length of the static portion of the query.
        margin = self.remaining_query_length(sql['assembly'].format_map(sql))
        # Cache progress parameters
        progress_parameters = {'total': data['requested_row_count'],
                               'prefix': 'Record retrieval: ',
                               'length': 50}
        
        # Row Collection callback.
        # TODO: alter this? BatchHttpRequests don't seem to work nicely, giving winerr 10053.
        def collect_rows(rq_id: str, response: dict, exception: HttpError):
            """Batch HTTP Callback
            Adds response rows to the output collection and reports completion progress.
            """
            nonlocal data
            if exception is not None or 'columns' not in response:
                data['total errors'] += 1
                data['to_retry'].append(rq_id)
            elif len(data.setdefault('columns', response['columns'])) == len(response['columns']):
                try:
                    data['rows'].extend(response['rows'])
                except KeyError:
                    pass
            else:
                print(f'Incorrect column count in response for requestId {rq_id}')
                print(response)
            print_progress_bar(len(data['rows']), **progress_parameters)

        # Use request_ids of max length 25 char, and map to the real values of the 'where_str'
        # Used if retrying queries made from batch requests.
        request_map = {}

        data['timing'] = time.perf_counter()
        print_progress_bar(0, **progress_parameters)
        while rowids:
            queried_rowids = []
            where_str = ','.join(queried_rowids)
            while rowids and len(where_str) < margin:
                queried_rowids.append(rowids.pop())
                where_str = ','.join(queried_rowids)
            sql['where_values'] = where_str
            # if not doing batch query, do not need to save queries.
            data['queries'][where_str] = sql['assembly'].format_map(sql)
            rq_id = where_str[0:25]
            assert rq_id not in request_map
            request_map[rq_id] = data['queries'][where_str]
            collect_rows(rq_id, self.query.sqlGet(sql=data['queries'][where_str]).execute(), None) # redo this

        print()
        print('\tDid {} queries in {} sec to retrieve {requested_row_count} records:'
              .format(len(data['queries']), time.perf_counter() - data['timing'], **data))

        # Retry failed requests ad nauseum.
        while data['to_retry']:
            print(f'\n\tRetrying {len(data["to_retry"])} failed requests')
            row_repeats = self.new_batch_http_request(callback=collect_rows)
            for rq_id in data['to_retry']:
                row_repeats.add(self.query.sqlGet(sql=request_map[rq_id]), request_id=rq_id)
            data['to_retry'] = []
            row_repeats.execute()

        if len(data['rows']) != data['requested_row_count']:
            raise ValueError('Obtained different number of records than specified')
        #data['timing'] = time.perf_counter() - data['timing']
        #print('\tRetrieved {requested_row_count} records in {timing} sec.'.format_map(data))
        # TODO: dictwriter to save all the data generated?
        return data['rows']


    def get_query_result(self, query: str,
                         kb_row_size=1., offset_start=0, max_rows_received=float("inf")) -> dict:
        '''Perform an arbitrarily-large dataquery

    Perform a FusionTable query and return the fusiontables#sqlresponse object. If the response
    is estimated to be larger than 10 MB, this will perform several requests. The query is assumed
    to be within appropriate length bounds (i.e. less than 8000 characters) and also appendable,
    (i.e. would not be invalidated by adding LIMIT and OFFSET to the end.

    @params:
        query: str, the SQL GET statement (Show, Select, Describe) to execute.
        kb_row_size: float, the expected size of an individual returned row, in kB.
        offset_start: int, the global offset into the desired query result.
                i.e. the value normally written after a SQL "OFFSET" descriptor.
        max_rows_received: int, the global maximum number of records the query should return.
                i.e. the value normally written after a SQL "LIMIT" descriptor.

    @return: dict, conforming to fusiontables#sqlresponse formatting, equivalent to what
            would be returned as though only a single query were made.
        '''
        if not isinstance(query, str):
            raise TypeError('Complex sql recombination should be done by callee.')
        if not self.validate_query_is_get(query):
            print(f'Query is incompatible with sqlGet method:\n{query}')
            return {}

        # Multi-query parameters.
        sql = {'assembly': '{query} OFFSET {offset} LIMIT {limit}',
               'query': query,
               'limit': int(9.5 * 1024 / kb_row_size),
               'offset': offset_start}
        # Eventual return value.
        query_result = {'kind': 'fusiontables#sqlresponse', 'is_complete': False}
        collected_row_data = []
        while True:
            request: HttpRequest = self.query.sqlGet(sql=sql['assembly'].format_map(sql))
            try:
                response = request.execute(num_retries=2)
            except HttpLib2Error as err:
                print('Transport error: ', err, '\nRetrying query.')
            except HttpError as err:
                rq_as_json = json.loads(request.to_json())
                print('Error during query:\n')
                pprint(err)
                pprint(rq_as_json)
                return {}
            else:
                sql['offset'] += sql['limit']
                if 'columns' not in query_result and 'columns' in response:
                    query_result['columns'] = response['columns']
                if 'rows' not in response:
                    break
                collected_row_data.extend(response['rows'])
                if (len(response['rows']) < sql['limit']
                        or len(collected_row_data) >= max_rows_received):
                    break

        # Ensure that the requested maximum return count is obeyed.
        while len(collected_row_data) > max_rows_received:
            collected_row_data.pop()

        # Finalize the output object.
        query_result['rows'] = collected_row_data
        query_result['is_complete'] = True
        return query_result


    # Non-destructive tasks
    def set_user_table(self, tableId: str):
        """Set the table id that corresponds to the MHCC Members FusionTable"""
        self._user_table = tableId


    def backup_table(self, tableId: str) -> dict:
        '''Create a copy of the the input FusionTable
        
    Writes the new name & id to disk as well.
    Does not delete any previous backups (and thus can trigger used space quota exception).

    @params:
        tableId: str, the ID for a FusionTable which should be copied. (str)

    @return: dict, the minimal metadata for the copied FusionTable (id, name, description).
        '''
        if not tableId:
            return {}
        kwargs = {'tableId': tableId,
                  'copyPresentation': True,
                  'fields': 'tableId,name,description'}
        #backup = self._service.table().copy(**kwargs).execute()
        try:
            backup = self.table.copy(**kwargs).execute()
        except HttpError as err:
            print('Backup operation failed due to error:', err)
            return {}

        # Rename the copied table, and provide a better description.
        assert backup['name'].find('Copy of ') > -1, 'Name does not have "Copy of " in it: \'{}\''.format(backup['name'])
        now = datetime.datetime.utcnow()
        backup['name'] = '_'.join(backup['name'][(backup['name'].find('Copy of ') + len('Copy of ')):].split())
        backup['name'] = '{}_AsOf_{}-{!s:0>2}-{!s:0>2} {!s:0>2}:{!s:0>2}'.format(
            backup['name'], now.year, now.month, now.day, now.hour, now.minute)
        backup['description'] = 'Automatically generated backup of tableId=' + tableId
        kwargs = {'tableId': backup['tableId'],
                  'body': backup,
                  'fields': 'tableId,name,description'}
        #self._service.table().patch(**kwargs).execute()
        self.table.patch(**kwargs).execute()
        # Log this new table to disk.
        with open('tables.txt', 'a', newline='') as f:
            csv.writer(f, quoting=csv.QUOTE_ALL).writerows([[backup['name'], backup['tableId']]])
        print(f'Backup of table \'{tableId}\' completed; new table logged to disk.')
        return backup


    # Methods that delete things!
    def restore_table(self, backupId: str, destination: str):
        """Replaces all rows in the destination with those from the backup"""

        data = self.get_query_result("SELECT * FROM " + backupId)
        if not data or data['is_complete'] is False:
            print('Data acquisition failed.')
            return False

        assert self.count_rows(backupId) == len(data['rows'])
        indices = {value.__str__().lower(): i for i, value in enumerate(data['columns'])}
        # Reparse to ensure UIDs are valid.
        if 'uid' in indices:
            coerced = set()
            for row in data['rows']:
                if '.' in row[indices['uid']]:
                    coerced.add(row[indices['uid']])
                    row[indices['uid']] = row[indices['uid']].partition('.')[0]
            if coerced:
                print(f'Updated {len(coerced)} member names to remove \'.0\'')

        # Upload
        print(f'Beginning row replacement of target \'{destination}\' from \'{backupId}\'')
        return self.replace_rows(destination, data['rows'])

    def verify_known_tables(self, known_tables: dict, drive_service):
        '''Check declared tables for validity and desirability
    Inspect every table in the known_tables dict, to ensure that the table key is valid.
    Invalid table keys (such as those belonging to deleted or trashed files) are removed.
    If a FusionTable is in the trash and owned by the executing user, it is then deleted.
        '''
        if not known_tables or not isinstance(known_tables, dict):
            print("No known list of tables")
            return

        def read_drive_response(_, response: dict, exception: HttpError):
            '''Callback for batch requests to the Drive API
        Inspects the response from Google Drive API to determine if the table id used references an
        actual existing table.
            '''
            def validate_table(table: dict):
                '''A valid table is not trashed.
            @param: table, a FusionTable resource
            @return: bool
                '''
                # If the table is not trashed, keep it.
                if table['trashed'] is False:
                    return True
                # If the table can be deleted, delete it.
                if table['ownedByMe'] is True and table['capabilities']['canDelete'] is True:
                    nonlocal batch_delete_requests
                    batch_delete_requests.add(self.table.delete(tableId=table['id']))
                return False

            nonlocal validated_tables, batch_delete_requests
            if exception is None and validate_table(response):
                validated_tables[response['name']] = response['id']

        validated_tables = {}
        kwargs = {'fileId': None,
                  'fields': 'id,name,trashed,ownedByMe,capabilities/canDelete'}
        # Collect the data requests in a batch request.
        batch_get_requests = drive_service.new_batch_http_request(callback=read_drive_response)
        batch_delete_requests = self.new_batch_http_request(callback=None)
        for id in known_tables.values():
            # Obtain the file as known to Drive.
            kwargs['fileId'] = id
            batch_get_requests.add(drive_service.files().get(**kwargs))
        batch_get_requests.execute()

        # Delete any of the user's trashed FusionTables.
        batch_delete_requests.execute()
        if validated_tables and len(validated_tables.items()) < len(known_tables.items()):
            # Rewrite the tables.txt file as CSV.
            print('Rewriting list of known tables (invalid tables have been removed).')
            with open('tables.txt', 'w', newline='') as f:
                tables_to_write = []
                for name, id in validated_tables.items():
                    tables_to_write.append([name, id])
                csv.writer(f, quoting=csv.QUOTE_ALL).writerows(tables_to_write)
                print('Remaining valid tables\n', tables_to_write)


    def delete_all_rows(self, tableId: str) -> bool:
        '''Performs a FusionTables.tables().sql(sql=DELETE) operation.

    @params:
        tableId: str, the ID of the FusionTable which should have all rows deleted.

    @return: bool, hether or not the delete operation succeeded.
        '''
        kwargs = {'sql': "DELETE FROM " + tableId}
        try:
            response = self.query.sql(**kwargs).execute()
        except (HttpLib2Error | HttpError) as err:
            print('Error during table deletion:', err)
            print(kwargs, response)
            return False

        while True:
            tasks = self.get_tasks(tableId)
            if tasks:
                print(tasks[0]['type'], "progress:", tasks[0]['progress'])
                time.sleep(1)
            else:
                break
        print("Deleted rows:", response['rows'][0][0])
        return True


    def replace_rows(self, tableId: str, new_row_data: list, filename='') -> bool:
        '''Upload the new data to the target table

    Performs a FusionTables.tables().replaceRows() call to the input table, replacing its contents
    with the input rows. Does not attempt to back up the table for you.

    @params:
        tableId: str, the FusionTable to update (String)
        new_row_data: list, the values to overwrite the FusionTable with (list of lists)
        filename: str, the name of a file to which the uploaded data should first be serialized.
                Defaults to a unique name based on the target table, in the local directory.

    @return: bool, whether or not the indicated FusionTable's rows were replaced.
        '''
        if not tableId or not new_row_data:
            return False
        if not filename:
            filename = self.get_filename_for_table(tableId, 'replaceRows')
        # Create a resumable MediaFileUpload with the "interesting" data to retain.
        sep = ','
        upload = _make_media_file(new_row_data, filename, True, sep)
        kwargs = {'tableId': tableId,
                  'media_body': upload,
                  'media_mime_type': 'application/octet-stream',
                  'encoding': 'UTF-8',
                  'delimiter': sep}
        # Try the upload twice (which requires creating a new request).
        if upload and upload.resumable():
            try:
                if not _step_upload(self.table.replaceRows(**kwargs))[0]:
                    return _step_upload(self.table.replaceRows(**kwargs))[0]
            except HttpError as err:
                if (err.resp.status in [417]
                        and 'Table will exceed allowed maximum size' in err.__str__()):
                    # The goal is to replace the table's rows, so every existing row will be deleted
                    # anyway. If the table's current data is too large, such that old + new >= 250,
                    # then Error 417 is returned.  Handle this by explicitly deleting the rows first.
                    if self.delete_all_rows(tableId):
                        return _step_upload(self.table.importRows(**kwargs))[0]
                    return False
                raise err

        elif upload:
            if not _send_whole_upload(self.table.replaceRows(**kwargs))[0]:
                return _send_whole_upload(self.table.replaceRows(**kwargs))[0]
        return True


    def import_rows(self, tableId: str, new_row_data: list, filename='') -> int:
        '''Upload the new data into the target table

    Performs a FusionTables.tables().importRows() call to the input table, adding the input rows.
    Does not attempt to back up the table for you.

    @params:
        tableId: str, the FusionTable to update (String)
        new_row_data: list, the values to overwrite the FusionTable with (list of lists)
        filename: str, the name of a file to which the uploaded data should first be serialized.
                Defaults to a unique name based on the target table, in the local directory.

    @return: int, the number of rows added to the table.
        '''
        if not tableId or not new_row_data:
            return False
        if not filename:
            filename = self.get_filename_for_table(tableId, 'importRows')
        # Create a resumable MediaFileUpload with the "interesting" data to retain.
        sep = ','
        upload = _make_media_file(new_row_data, filename, True, sep)
        kwargs = {'tableId': tableId,
                  'media_body': upload,
                  'media_mime_type': 'application/octet-stream',
                  'encoding': 'UTF-8',
                  'delimiter': sep}
        # Try the upload twice (which requires creating a new request).
        result, resp = None, None
        if upload and upload.resumable():
            result, resp = _step_upload(self.table.importRows(**kwargs))
            if not result:
                result, resp = _step_upload(self.table.importRows(**kwargs))
        elif upload:
            result, resp = _send_whole_upload(self.table.importRows(**kwargs))
            if not result:
                result, resp = _send_whole_upload(self.table.importRows(**kwargs))

        return int(resp['numRowsReceived']) if result else 0


    @staticmethod
    def can_use_local_data(tableId: str, filename: str, drive_handler: DriveHandler) -> bool:
        """Check if a local file can be used to update a remote table.
        
    If the named local file is present, compares determines
    its modification time. This modification time is then compared to the modification time of the
    given FusionTable. If the FusionTable was modified more recently than the local data, then the
    records must be reacquired.

    @params:
        tableId: str, the FusionTable whose content is to be replaced.
        filename: str, the name of the file in the local directory with data to upload
        drive_handler: DriveHandler, an authenticated service handler class for Google Drive

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
        except (PermissionError, FileNotFoundError):
            return False

        # Obtain the last modified time for the FusionTable.
        info = drive_handler.get_modified_info(tableId)
        # Obtain the last modified time for the local file.
        local_mod_time = datetime.datetime.fromtimestamp(
            os.path.getmtime(filename), datetime.timezone.utc)
        print(f'FusionTable last modified:\t{info["modifiedDatetime"]}\nlocal data last modified:\t{local_mod_time}')
        if local_mod_time > info['modifiedDatetime']:
            print('Local saved data modified more recently than remote FusionTable.',
                  'Attempting to use saved data.')
            return True
        print('Remote FusionTable modified more recently than local saved data.',
              'Record analysis and download is required.')
        return False


    @staticmethod
    def read_local_data(csv_filename: str, delimiter=',') -> list:
        '''Attempt to load column data from the specified CSV.

    @params:
        csv_filename: str, the name of the CSV datafile in the local directory.
        delimiter: str, the CSV file delimiter that was used to write the file.

    @returns: list, the data read from the local file.
        '''
        if not (csv_filename and isinstance(csv_filename, str)):
            raise TypeError('Expected string filename')
        values_from_disk = []
        try:
            with open(csv_filename, 'r', newline='', encoding='utf-8') as datafile:
                data_reader = csv.reader(datafile, strict=True, delimiter=delimiter, quoting=csv.QUOTE_NONNUMERIC)
                values_from_disk = [row for row in data_reader]
        except (FileNotFoundError, PermissionError) as err:
            print("\n", err)

        return values_from_disk




