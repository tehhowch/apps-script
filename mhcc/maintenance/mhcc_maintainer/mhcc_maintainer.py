#! /usr/bin/python3

import csv
import datetime
import json
import os
import random
import time

from googleapiclient.discovery import build
from googleapiclient.http import HttpError
from googleapiclient.http import HttpRequest
from googleapiclient.http import MediaFileUpload
from oauth2client import tools
from oauth2client.file import Storage
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import OAuth2WebServerFlow

localKeys = {};
tableList = {};

scope = [
	'https://www.googleapis.com/auth/fusiontables',
	'https://www.googleapis.com/auth/drive'
];
FusionTables = None;
Drive = None;

# Script which performs maintenance functions for the MHCC FusionTable and
# initiates a resumable upload to handle the large dataset
def Initialize():
	'''
	Read in the FusionTable IDs and the data for OAuth
	'''
	global localKeys, tableList;
	stripChars = ' "\n\r';
	with open('auth.txt') as f:
		for line in f:
			(key, val) = line.split('=');
			localKeys[key.strip(stripChars)] = val.strip(stripChars);
	with open('tables.txt', 'r', newline = '') as f:
		for line in csv.reader(f, quoting = csv.QUOTE_ALL):
			tableList[line[0]] = line[1];
	
	print('Initialized. Found tables: ');
	for key in tableList.keys():
		print('\t' + key);



def Authorize():
	'''
	Authenticate the requested Google API scopes for a single user.
	'''
	flow = OAuth2WebServerFlow(localKeys['client_id'], localKeys['client_secret'], scope);
	storage = Storage('credentials.dat');
	credentials = storage.get();
	if credentials is None or credentials.invalid:
		print('Reauthorization required... Launching auth flow');
		credentials = tools.run_flow(flow, storage, tools.argparser.parse_args());
	else:
		print('Valid Credentials.');
	global FusionTables;
	print('Authorizing...', end='');
	FusionTables = build('fusiontables', 'v2', credentials=credentials);
	if FusionTables is None:
		raise EnvironmentError('FusionTables not authenticated or built as a service.');
	global Drive;
	Drive = build('drive','v3', credentials = credentials);
	if Drive is None:
		raise EnvironmentError('Drive not authenticated or built as a service.');
	print('Authorized.');
	


def VerifyKnownTables():
	'''
	Inspect every table in the tableList dict, to ensure that the table key is valid.
	Invalid table keys (such as those belonging to deleted or trashed files) are removed.
	If a FusionTable is in the trash and owned by the executing user, it is deleted.
	'''
	if not tableList:
		print("No known list of tables");
		return;
	elif len(tableList.items()) == 0:
		return;
	if not FusionTables or not Drive:
		print("Authorization is required to validate tables.");
		return;

	def ReadDriveResponse(_, response, exception):
		def ValidateTable(table):
			# If the table is not trashed, keep it.
			if table['trashed'] is False:
				return True;
			# If the table can be deleted, delete it.
			if table['ownedByMe'] is True and table['capabilities']['canDelete'] is True:
				nonlocal batchDeletes;
				batchDeletes.add(FusionTables.table().delete(tableId = table['id']));
			return False;

		nonlocal valids, batchDeletes;
		if exception is None and ValidateTable(response):
			valids[response['name']] = response['id'];

	valids = {};
	kwargs = {
		'fileId': None,
		'fields': 'id,name,trashed,ownedByMe,capabilities/canDelete'}
	# Collect the data requests in a batch request.
	batchGets = Drive.new_batch_http_request(callback = ReadDriveResponse);
	batchDeletes = FusionTables.new_batch_http_request(callback = None);
	for name, tableId in tableList.items():
		# Obtain the file as known to Drive.
		kwargs['fileId'] = tableId;
		batchGets.add(Drive.files().get(**kwargs));
	batchGets.execute();
	# Delete any of the user's trashed FusionTables.
	if len(batchDeletes._requests.items()) > 0:
		batchDeletes.execute();
	if len(valids.items()) < len(tableList.items()) and len(valids.items()) > 0:
		# Rewrite the tables.txt file as CSV.
		print('Rewriting list of known tables (invalid tables have been removed).');
		with open('tables.txt', 'w', newline = '') as f:
			toWrite = [];
			for tableName, tableID in valids.items():
				toWrite.append([tableName, tableID]);
			csv.writer(f, quoting = csv.QUOTE_ALL).writerows(toWrite);
		


def GetModifiedInfo(fileId = ''):
	'''
	Acquire the modifiedTime of the referenced table via the Drive service. The version will change when almost any part of the table is changed.
	@params: fileId	- Required	: The file ID (table ID) of a file known to Google Drive
	@return:	dict			: A dictionary containing the RFC3339 modified timestamp from Google Drive, the equivalent aware datetime object, and the file metadata
	'''
	if type(fileId) is not str:
		raise TypeError('Expected string table ID.');
	elif len(fileId) != 41:
		raise ValueError('Received invalid table ID.');
	kwargs = {'fileId': fileId, 'fields': 'id,mimeType,modifiedTime,version'};
	request = Drive.files().get(**kwargs);
	fusionFile = request.execute();
	try:
		return {'modifiedString': fusionFile['modifiedTime'],
		  'modifiedDatetime': datetime.datetime.strptime(fusionFile['modifiedTime'][:-1] + '+0000', '%Y-%m-%dT%H:%M:%S.%f%z'),
		  'file': fusionFile};
	except Exception as e:
		print('Acquisition of modification info failed.', e);
		return {'modifiedString': None, 'modifiedDatetime': None, 'file': None};



def GetQueryResult(query, rowSize = 1., offsetStart = 0, maxReturned = float("inf")):
	'''
	Perform a FusionTable query and return the fusiontables#sqlresponse object.
	If the response would be larger than 10 MB, this will perform several requests.
	The query is assumed to be complete, i.e. specifying the target table, and also
	appendable (i.e. has no existing LIMIT or OFFSET parameters).
	@params:
		query		- Required	: The SQL statement (Show, Select, Describe) to execute (string)
		rowSize		- Optional	: The expected size of an individual returned row, in kB (double)
		offsetStart	- Optional	: The global offset desired, i.e. what would normally be placed after an "OFFSET" descriptor (int)
		maxReturned	- Optional	: The global maximum number of records the query should return, i.e. what would normally be placed after a "LIMIT" descriptor (int)
	@return:	dict			: A dictionary conforming to fusiontables#sqlresponse formatting, equivalent to what would be returned as though only a single query were made.
	'''
	if not ValidateGetQuery(query):
		return {};
	
	# Multi-query parameters.
	limitValue = int(9.5 * 1024 / rowSize);
	offsetValue = offsetStart;
	# Eventual return value.
	ft = {'kind': "fusiontables#sqlresponse"};
	data = [];
	done = False;
	while not done:
		tail = ' '.join(['OFFSET', offsetValue.__str__(), 'LIMIT', limitValue.__str__()]);
		request = FusionTables.query().sqlGet(sql = query + ' ' + tail);
		try:
			response = request.execute(num_retries = 2);
		except HttpError as e:
			r = json.loads(request.to_json());
			print('Body length:', len(''.join([r['uri'], '?', r['body']])));
			print('Start of body:', r['body'][0:200]);
			print(e);
			return {};
		except Exception as e:
			print(e);
			return {};
		else:
			offsetValue += limitValue;
			if 'rows' in response.keys():
				data.extend(response['rows']);
			if 'rows' not in response.keys() or len(response['rows']) < limitValue or len(data) >= maxReturned:
				done = True;
			if 'columns' not in ft.keys() and 'columns' in response.keys():
				ft['columns'] = response['columns'];

	# Ensure that the requested maximum return count is obeyed.
	while len(data) > maxReturned:
		data.pop();
	
	ft['rows'] = data;
	return ft;



def ValidateGetQuery(query = ''):
	'''
	Inspect the given query to ensure it is actually a SQL GET query and includes a table.
	@params: query	- Required	: The SQL to be sent to FusionTables via FusionTables.query().sqlGet / .sqlGet_media	(str)
	@return:	bool			: Whether or not it is a GET request (vs PUT, PATCH, DELETE, etc.) and specifies a target FusionTable.
	'''
	l = query.lower();
	if ('select' not in l) and ('show' not in l) and ('describe' not in l):
		return False;
	if 'from' not in l:
		return False;
	return True;



def ValidateQueryResult(queryResult):
	'''
	Checks the returned query result from FusionTables to ensure it has the minimum value.
	bytestring: a newline separator (i.e. header and a value).
	dictionary: 'rows' object (i.e. fusiontables#sqlresponse)
	@params: queryResult	- Required	: a response given by a SQL request to FusionTables.query()
	@return:	bool					: Whether or not this query result has parsable data.
	'''
	if not queryResult:
		return False;
	# A dictionary query result should have a 'rows' key.
	if type(queryResult) == dict and ('rows' not in queryResult.keys()):
		return False;
	# A bytestring query result should have at least one header and one value.
	if type(queryResult) == type(b'') and len(queryResult.splitlines()) == 0:
		return False;
	return True;



def ExtractQueryResultFromByteString(queryResult = b''):
	'''
	Convert the received bytestring from an alt=media request into a true FusionTables JSON response.
	@params: queryResult	- Required	: A bytestring of input data
	@return:	dict					: A dictionary conforming to fusiontables#sqlresponse
	'''
	if len(queryResult) == 0:
		return {};
	ft = {'kind': "fusiontables#sqlresponse"};
	columnSeparator = ',';
	s = queryResult.decode();
	data = [];
	for i in s.splitlines():
		data.append(i.split(columnSeparator));
	ft['columns'] = data.pop(0);
	ft['rows'] = data;
	return ft;



def ReplaceRows(tableId = '', newRows = []):
	'''
	Performs a FusionTables.tables().replaceRows() call to the input table, replacing its contents with the input rows.
	@params:
		tableId		- Required	: the FusionTable to update (String)
		newRows		- Required	: the values to overwrite the FusionTable with (list of lists)
	@return:	bool			: Whether or not the indicated FusionTable's rows were replaced with the input rows.
	'''
	if not tableId or not newRows:
		return False;
	
	# Create a resumable MediaFileUpload containing the "interesting" data to retain.
	sep = ',';
	upload = MakeMediaFile(newRows, 'staging.csv', True, sep);
	kwargs = {
		'tableId': tableId,
		'media_body': upload,
		'media_mime_type': 'application/octet-stream',
		'encoding': 'UTF-8',
		'delimiter': sep};
	# Try the upload twice (which requires creating a new request).
	if upload and upload.resumable():
		try:
			if not StepUpload(FusionTables.table().replaceRows(**kwargs)):
				return StepUpload(FusionTables.table().replaceRows(**kwargs));
		except HttpError as e:
			if e.resp.status in [417] and e._get_reason() == 'Table will exceed allowed maximum size':
				# The goal is to replace the table's rows, so every existing row will be deleted anyway.
				# If the table's current data is too large, such that old + new >= 250, then Error 417
				# is returned. Handle this by explicitly deleting the rows first, then uploading the data.
				if DeleteAllRows(tableId):
					return StepUpload(FusionTables.table().importRows(**kwargs));
				else:
					return False;
			raise e;
	elif upload:
		if not Upload(FusionTables.table().replaceRows(**kwargs)):
			return Upload(FusionTables.table().replaceRows(**kwargs));
	return True;



def DeleteAllRows(tableId = ''):
	'''
	Performs a FusionTables.tables().sql(sql=DELETE) operation, with an empty value array.
	@params: tableId	- Required	: The ID of the FusionTable which should have all rows deleted.
	@return:	bool				: Whether or not the delete operation succeeded. 
	'''
	if not tableId or len(tableId) != 41:
		return False;
	kwargs = {'sql': "DELETE FROM " + tableId};
	try:
		response = FusionTables.query().sql(**kwargs).execute();
	except Exception as e:
		print(e);
		return False;

	while True:
		tasks = GetAllTasks(tableId);
		if tasks and len(tasks) > 0:
			print(tasks[0]['type'], "progress:", tasks[0]['progress']);
			time.sleep(1);
		else:
			break;
	print("Deleted rows:", response['rows'][0][0]);
	return True;



def GetAllTasks(tableId = ''):
	'''
	Performs as many FusionTables.task().list() queries as is needed to obtain all active tasks for the given FusionTable
	@params: tableId	- Required	: The ID of the FusionTable which should be queried for running tasks (such as row deletion).
	@return:	list				: A list of all fusiontable#task dicts that are running or scheduled to run. 
	'''
	if type(tableId) is not str:
		raise TypeError('Expected string table ID.');
	elif len(tableId) != 41:
		raise ValueError('Received invalid table ID \'' + tableId + '\'');
	taskList = [];
	request = FusionTables.task().list(tableId = tableId);
	while request is not None:
		response = request.execute();
		if 'items' in response.keys():
			taskList.extend(response['items']);
			print("Querying tasks.", len(taskList), "found so far...");
		request = FusionTables.task().list_next(request, response);
	return taskList;



def Upload(request = None):
	'''
	Upload a non-resumable media file.
	@params: request	- Required	: an HttpRequest with an upload Media that might not support next_chunk().
	@return:	bool				: Whether or not the upload succeeded.
	'''
	if not request:
		return False;

	try:
		response = request.execute(num_retries = 2);
		return True;
	except HttpError as e:
		print('Upload failed:', e);
		return False;



def StepUpload(request = None):
	'''
	Print the percentage complete for a given upload while it is executing.
	@params:
		request		- Required	: an HttpRequest that supports next_chunk() (i.e., is resumable).
	@return Bool				: Whether or not the upload succeeded.
	@throws			: HttpError 417. This error indicates if the FusionTable size limit will be exceeded.
	'''
	if not request:
		return False;
	
	done = None;
	fails = 0;
	while done is None:
		try:
			status, done = request.next_chunk();
			printProgressBar(status.progress() if status else 1., 1., 'Uploading...', length = 50);
		except HttpError as e:
			if e.resp.status in [404]:
				print();
				return False;
			elif e.resp.status in [500, 502, 503, 504] and fails < 5:
				time.sleep(2 ^ fails);
				++fails;
			elif e.resp.status in [417] and (fails < 5) and (e._get_reason() == 'Table will exceed allowed maximum size'):
				print();
				raise e;
			else:
				print();
				print('Upload failed:', e);
				return False;
	print();
	return True;



def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█'):
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
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
    # Print New Line on Complete
    if iteration >= total:
        print()



def GetUserBatch(start, limit = 10000):
	'''
	Get a set of members and their internal IDs.
	@params:
		start		- Required	: the first index to return a user for (unsigned).
		limit		- Optional	: the number of data pairs to be returned (unsigned).
	@return:	list[list[Member, UID]]
	'''
	sql = 'SELECT Member, UID FROM ' + tableList['MHCC Members'] + ' ORDER BY Member ASC';
	startTime = time.perf_counter();
	resp = GetQueryResult(sql, .03, start, limit);
	try:
		print('Fetched', len(resp['rows']), 'members in', round(time.perf_counter() - startTime, 1), 'sec. Wanted <=', limit, 'after index', start);
		return resp['rows'];
	except:
		print('Received no data from user fetch query.')
		return [];



def GetTotalRowCount(tableId = ''):
	'''
	Queries the size of a table, in rows.
	@params:	tableId	- Required	: the FusionTable to determine a row count for.
	@return:	long				: The number of rows in the FusionTable.
	'''
	countSQL = 'select COUNT(ROWID) from ' + tableId;
	print('Fetching total row count for table id', tableId);
	start = time.perf_counter();
	allRowIds = GetQueryResult(countSQL);
	try:
		print('Found', int(allRowIds['rows'][0][0]), 'rows in', round(time.perf_counter() - start, 1), 'sec.');
		return int(allRowIds['rows'][0][0]);
	except:
		print('Received no data from row count query.');
		return int(0);



def RetrieveWholeRecords(rowids = [], tableId = ''):
	'''
	Returns a list of lists (i.e. 2D array) corresponding to the full records
	associated with the requested rowids in the specified table.
	@params:
		rowids		- Required	: the rows in the table to be fully obtained (unsigned)
		tableId		- Required	: the FusionTable to obtain records from (String)
	@return:	list			: Complete records from the indicated FusionTable.
	'''
	if type(rowids) is not list:
		raise TypeError('Expected list of rowids.');
	elif not rowids:
		raise ValueError('Received empty list of rowids to retrieve.');
	if type(tableId) is not str:
		raise TypeError('Expected string table ID.');
	elif len(tableId) != 41:
		raise ValueError('Received invalid table ID.');
	records = [];
	numNeeded = len(rowids);
	rowids.reverse();
	baseSQL = 'SELECT * FROM ' + tableId + ' WHERE ROWID IN (';
	print('Retrieving', numNeeded, 'records:');
	startTime = time.perf_counter();
	printProgressBar(numNeeded - len(rowids), numNeeded, "Record retrieval: ", "", 1, 50);
	while rowids:
		tailSQL = '';
		sqlROWIDs = [];
		batchTime = time.monotonic();
		while rowids and (len(baseSQL + tailSQL) <= 8000):
			sqlROWIDs.append(rowids.pop());
			tailSQL = ','.join(sqlROWIDs) + ')';
		# Fetch the batch of records.
		resp = GetQueryResult(''.join([baseSQL, tailSQL]), 0.3);
		try:
			records.extend(resp['rows']);
		except:
			return [];
		elapsed = time.monotonic() - batchTime;
		# Rate Limit
		if elapsed < .75:
			time.sleep(.75 - elapsed);
		printProgressBar(numNeeded - len(rowids), numNeeded, "Record retrieval: ", "", 1, 50);
	
	if len(records) != numNeeded:
		raise LookupError('Obtained different number of records than specified');
	print('Retrieved', numNeeded, 'records in', time.perf_counter() - startTime, 'sec.');
	return records;



def IdentifyDiffSeenAndRankRecords(uids = [], tableId = ''):
	'''
	Returns a list of rowids for which the LastSeen values are unique, or the
	rank is unique (for a given LastSeen), for the given members.
	Uses SQLGet since only GET is done.
	@params:
		uids		- Required	:	Member identifiers (for whom records should be retrieved). (list)
		tableId		- Required	:	The ID of the FusionTable with records to retrieve. (str)
	@return:	list			:	The ROWIDs of records which should be retrieved.
	'''
	rowids = [];
	if not tableId or type(tableId) != str or len(tableId) != 41:
		raise ValueError('Invalid table ID received.');
	if not uids:
		raise ValueError('Invalid UIDs received.');

	uids.reverse();
	savings = 0;
	memberCount = len(uids);
	# Index the column headers to find the indices of interest.
	ROWID_INDEX = 0;
	UID_INDEX = 2;
	LASTSEEN_INDEX = 3;
	RANK_INDEX = 4;

	baseSQL = 'SELECT ROWID, Member, UID, LastSeen, Rank FROM ' + tableId + ' WHERE UID IN (';
	print('Sifting through', memberCount, 'member\'s stored data.');
	printProgressBar(memberCount - len(uids), memberCount, "Members sifted: ", "", 1, 50);
	while uids:
		tailSQL = '';
		sqlUIDs = [];
		batch = time.monotonic();
		while uids and (len(baseSQL + tailSQL) <= 8000):
			sqlUIDs.append(uids.pop());
			tailSQL = ','.join(sqlUIDs) + ') ORDER BY LastSeen ASC';
		resp = GetQueryResult(''.join([baseSQL, tailSQL]), 0.1);
		try:
			totalRecords = len(resp['rows']);
		except Exception as e:
			print();
			print(e);
			return [];
		# Each rowid should only occur once (i.e. a list should be
		# sufficient), but use a Set container just to be sure.
		kept = set();
		seen = {};
		[kept.add(row[ROWID_INDEX]) for row in resp['rows']
			if IsInterestingRecord(row, UID_INDEX, LASTSEEN_INDEX, RANK_INDEX, seen)];
		
		# Store these interesting rows for later retrieval.
		rowids.extend(kept);
		savings += totalRecords - len(kept);
		elapsed = time.monotonic() - batch;
		if elapsed < .75:
			time.sleep(.75 - elapsed);
		printProgressBar(memberCount - len(uids), memberCount, "Members sifted: ", "", 1, 50);
		
	print('Found', savings, 'values to trim from', savings + len(rowids), 'records');
	return rowids;



def IsInterestingRecord(record = [], uidIndex = 0, lsIndex = 0, rankIndex = 0, tracker = {}):
	'''
	Shared method used to identify which rows of the input are interesting and should be kept.
	Modifies the passed tracker dictionary to support repeated calls (i.e. while querying to get the input)
	@params:
		record		- Required	:	An individual crown record (or subset) (list)
		uidIndex	- Required	:	The column index for the UID (unsigned)
		lsIndex		- Required	:	The column index for the LastSeen datestamp (unsigned)
		rankIndex	- Required	:	The column index for the Rank value (unsigned)
		tracker		- Required	:	A dict<uid, dict<LastSeen, set(intStr)>> object that tracks seen members, dates, and ranks.
	@return:	bool			:	Whether or not the input record should be uploaded.
	'''
	if not record:
		return False;
	if len({uidIndex, lsIndex, rankIndex}) < 3:
		raise ValueError('Different properties given same column index.');
	
	try:
		uid = record[uidIndex].__str__();
		ls = int(record[lsIndex]).__str__();
		rank = record[rankIndex].__str__();
	except Exception as e:
		print('Invalid access into record:\n', record, '\n', uidIndex, lsIndex, rankIndex, '\n', e);
		return False;

	# We should keep this record if:
	# 1) It belongs to an as-yet unseen member.
	if uid not in tracker:
		tracker[uid] = dict([(ls, {rank})]);
	# 2) It is (the first) record for a given date.
	elif ls not in tracker[uid]:
		tracker[uid][ls] = {rank};
	# 3) The member has a different rank than previously seen.
	elif rank not in tracker[uid][ls]:
		tracker[uid][ls].add(rank);
	# Otherwise, we don't care about it.
	else:
		return False;

	return True;
		


def ValidateRetrievedRecords(records = [], sourceTableId = ''):
	'''
	Inspect the given records to ensure that each member with a record in source table has a record in the
	retrieved records, and that the given records satisfy the same uniqueness criteria that was used to obtain
	them from the source table.
	@params:
		records		- Required	:	All records that are to be uploaded to the FusionTable (list of lists)
		sourceTableId - Required:	The FusionTable that will be uploaded to (str)
	@return:	bool			:	Whether or not the input records are valid and can be uploaded.
	'''
	if not records or not sourceTableId or len(sourceTableId) != 41:
		return False;
	# Determine how many rows exist per user in the source table. Every member with at least 1 row should
	# have at least 1 and at most as many rows in the retrieved records list.
	perUserSQL = 'SELECT UID, COUNT(Gold) FROM ' + sourceTableId + ' GROUP BY UID';
	sourceRowCounts = GetQueryResult(perUserSQL, .05);
	currentMembers = set([row[1] for row in GetUserBatch(0)]);

	# Parse the given records into a dict<str, int> object to simplify comparison between the two datasets.
	recordRowCounts = {};
	errorRows = [];
	for row in records:
		if len(row) != 12:
			errorRows.append(row);
		# Always count this row, even if it was erroneously too short or too long. The UID is
		# in the 2nd column. (Because [] does not default-construct, handling KeyError is
		# necessary, as one will occur for the first record of each member.)
		try:
			recordRowCounts[str(row[1])] += 1;
		except KeyError:
			recordRowCounts[str(row[1])] = 1;
	if errorRows:
		print(errorRows);

	# Loop the source counts to verify the individual has data in the supplied records.
	isValid = True;
	for row in sourceRowCounts['rows']:
		if int(row[1]) > 0:
			# Validation rules apply only to current members.
			if row[0] not in currentMembers:
				continue;
			# If a member in the source table is also a current member, removing their data is unallowable.
			elif row[0] not in recordRowCounts:
				print('Upload would remove all data for member with ID', row[0], '(currently', row[1], 'rows)');
				isValid = False;
			# The uploaded data should not have 0 rows, or more rows than the source, for each member.
			elif recordRowCounts[row[0]] > int(row[1]) or recordRowCounts[row[0]] < 1:
				print('Upload has improper row count for member with ID', row[0]);
				isValid = False;
	
	# Perform the uniqueness checks on the records (all should be "interesting").
	# Index the column headers to find the indices of interest.
	UID_INDEX = 1;
	LASTSEEN_INDEX = 2;
	RANK_INDEX = 9;
	seen = {};
	valids = 0;
	for row in records:
		try:
			if not IsInterestingRecord(row, UID_INDEX, LASTSEEN_INDEX, RANK_INDEX, seen):
				isValid = False;
				break;
		except Exception as e:
			print(e);
			return False;

	print('Validation of new records', 'completed.' if isValid else 'failed.');
	return isValid;



def KeepInterestingRecords(tableId = ''):
	'''
	Removes duplicated crown records, keeping each member's records which
	have a new LastSeen value, or a new Rank value.
	@params: tableId	- Required	:	The FusionTable ID which should have extraneous records removed.
	@return:	None
	'''
	startTime = time.perf_counter();
	if not tableId or type(tableId) != str or len(tableId) != 41:
		raise ValueError('Invalid table id');
	uids = [row[1] for row in GetUserBatch(0, 100000)];
	if not uids:
		print('No members returned');
		return;
	totalRows = GetTotalRowCount(tableId);
	rowids = IdentifyDiffSeenAndRankRecords(uids, tableId);
	if not rowids:
		print('No rowids received');
		return;
	if len(rowids) == totalRows:
		print("All records are interesting");
		return;
	
	keptValues = None if not CanUseLocalCopy(tableId, 'staging.csv') else ReadLocalCopy('staging.csv');
	isValid = (keptValues is not None and len(keptValues) == len(rowids) and ValidateRetrievedRecords(keptValues, tableId));
	if not isValid:
		# Local data cannot be used, so get the row data associated with rowids.
		keptValues = RetrieveWholeRecords(rowids, tableId);
		# Validate this downloaded data.
		isValid = ValidateRetrievedRecords(keptValues, tableId);

	if isValid:
		# Back up the table before we do anything crazy.
		BackupTable(tableId);
		# Do something crazy.
		ReplaceTable(tableId, keptValues);
	print('KeepInterestingRecords: Completed in', time.perf_counter() - startTime, 'total sec.');



def BackupTable(tableId = ''):
	'''
	Creates a copy of the existing MHCC CrownRecord Database and logs the new table id.
	Does not delete the previous backup (and thus can result in a space quota exception).
	@params: tableId	- Required	:	The ID for a FusionTable which should be copied. (str)
	@return:	dict				:	The minimal metadata for the copied FusionTable (id, name, description).
	'''
	if not tableId:
		raise ValueError("Missing input table ID.");
	backup = FusionTables.table().copy(tableId = tableId, copyPresentation = True, fields = 'tableId,name,description').execute();
	now = datetime.datetime.utcnow();
	newName = 'MHCC_CrownHistory_AsOf_' + '-'.join(x.__str__() for x in [now.year, now.month, now.day, now.hour, now.minute]);
	backup['name'] = newName;
	backup['description'] = 'Automatically generated backup of tableId=' + tableId;
	kwargs = {
		'tableId': backup['tableId'],
		'body': backup,
		'fields': 'tableId,name,description'};
	FusionTables.table().patch(**kwargs).execute();
	with open('tables.txt', 'a', newline = '') as f:
		csv.writer(f, quoting = csv.QUOTE_ALL).writerows([[newName, backup['tableId']]]);
	print('Backup of table', tableId, 'completed, new table logged to disk.');
	return backup;



def MakeMediaFile(values, path, isResumable = None, delimiter = ','):
	'''
	Returns a MediaFile with UTF-8 encoding, for use with FusionTable API calls
	that expect a media_body parameter.
	Also creates a hard disk backup (to facilitate the MediaFile creation).
	'''
	MakeLocalCopy(values, path, 'w', delimiter);
	return MediaFileUpload(path, mimetype = 'application/octet-stream', resumable = isResumable);



def MakeLocalCopy(values, path, fileMode, delimiter = ','):
	'''
	Writes the given values to disk in the given location.
	Example path: 'staging.csv' -> write file 'staging.csv' in the script's directory.
	'''
	if (not values) or (not path):
		raise ValueError('Needed both values to save and a path to save.');
	if not fileMode:
		fileMode = 'w';
	if fileMode == 'r':
		raise ValueError('File mode must be write-capable.');
	with open(path, fileMode, newline = '', encoding = 'utf-8') as f:
		csv.writer(f, strict = True, delimiter = delimiter, quoting = csv.QUOTE_NONNUMERIC).writerows(values);



def CanUseLocalCopy(tableId = '', fileName = ''):
	'''
	Returns True if the local data can be used in lieu of downloading records from the FusionTable.
	Returns False if the local data is missing or out-of-date.
	Checks the local directory for the presence of the input file, and if it is present, determines its modification
    time. This modification time is then compared to the modification time of the given FusionTable. If the FusionTable
    was modified more recently than the local data, the records must be reacquired.
	'''
	if type(tableId) is not str or type(fileName) is not str:
		raise TypeError('Expected string filename and string table ID.');
	elif len(tableId) != 41:
		raise ValueError('Received invalid table ID');
	elif len(fileName) == 0:
		raise ValueError('Received invalid filename.');
	
	# Ensure the file exists.
	try:
		with open(fileName, 'r'): pass;
	except FileNotFoundError as e:
		return False;
	except Exception as e:
		print(e);
		return False;

	# Obtain the last modified time for the FusionTable.
	info = GetModifiedInfo(tableId);
	# Obtain the last modified time for the local file.
	localModTime = datetime.datetime.fromtimestamp(os.path.getmtime(fileName), datetime.timezone.utc);
	print('FusionTable last modified:\t', info['modifiedDatetime'], '\nlocal data last modified:\t', localModTime);
	if localModTime > info['modifiedDatetime']:
		print('Local saved data modified more recently than remote FusionTable. Attempting to use saved data.');
		return True;
	print('Remote FusionTable modified more recently than local saved data. Record analysis and download is required.');
	return False;


	
def ReadLocalCopy(fileName = '', delimiter = ','):
	'''
	Attempt to load a CSV from the local directory containing the most recently downloaded data.
	If the file exists and the remote FusionTable has not been modified since the data was acquired,
	upload the data instead of re-performing record inspection and download (since the same result would
	be obtained).
	'''
	if type(fileName) is not str:
		raise TypeError('Expected string filename');
	diskValues = [];
	try:
		with open(fileName, 'r', newline = '', encoding = 'utf-8') as f:
			dataReader = csv.reader(f, strict = True, delimiter = delimiter, quoting = csv.QUOTE_NONNUMERIC);
			diskValues = [row for row in dataReader];
		print("Imported data from disk:", len(diskValues), "rows.");
		print("Row length:", len(diskValues[0]));
	except Exception as e:
		print(e);
		return None;
	return diskValues;



def GetSizeEstimate(values = [], numSamples = 5):
	'''
	Estimates the upload size of a 2D array without needing to make the actual
	upload file. Averages @numSamples different rows to improve result statistics.
	'''
	rowCount = len(values);
	if numSamples >= .1 * rowCount:
		numSamples = round(.03 * rowCount);
	sampled = random.sample(range(rowCount), numSamples);
	uploadSize = 0.;
	for row in sampled:
		uploadSize += len((','.join(col.__str__() for col in values[row])).encode('utf-8'));
	uploadSize *= float(rowCount) / (1024 * 1024 * numSamples);
	return uploadSize;



def ReplaceTable(tableId = '', newValues = []):
	if type(newValues) is not list:
		raise TypeError('Expected value array as list of lists.');
	elif not newValues:
		raise ValueError('Received empty value array.');
	elif type(newValues[0]) is not list:
		raise TypeError('Expected value array as list of lists.');
	if type(tableId) is not str:
		raise TypeError('Expected string table id.');
	elif len(tableId) != 41:
		raise ValueError('Table id is not of sufficient length.');
	# Estimate the upload size by averaging the size of several random rows.
	print('Replacing table with id =', tableId);
	estSize = GetSizeEstimate(newValues, 10);
	print('Approx. new upload size =', estSize, ' MB.');
	
	start = time.perf_counter();
	newValues.sort();
	print('Replacement completed in' if ReplaceRows(tableId, newValues) else 'Replacement failed after',
	   round(time.perf_counter() - start, 1), 'sec.');



def PickTable():
	'''
	Request user input to determine the FusionTable to operate on.
	'''
	choice = None;
	while choice is None:
		typed = input("Enter the table name from above, or a table id: ");
		if len(typed) == 41:
			choice = typed;
		elif typed in tableList.keys():
			choice = tableList[typed];
		else:
			print("Unable to use your input.");
	return choice;



if (__name__ == "__main__"):
	Initialize();
	Authorize();
	VerifyKnownTables();
	# Ask for the table to operate on
	id = PickTable();
	# Perform maintenance.
	KeepInterestingRecords(id);
