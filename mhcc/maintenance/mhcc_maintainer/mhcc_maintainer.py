#! /usr/bin/python3

import time
import datetime
import csv
import random
import json

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

scope = ['https://www.googleapis.com/auth/fusiontables',
		 'https://www.googleapis.com/auth/drive'];
FusionTables = None;

# Script which performs maintenance functions for the MHCC FusionTable and
# initiates a resumable upload to handle the large dataset
def Initialize():
	'''
	Read in the FusionTable ID and the api keys for oAuth
	'''
	global localKeys;
	with open('auth.txt') as f:
		for line in f:
			(key, val) = line.split('=');
			localKeys[key.strip()] = val.strip();
	with open('tables.txt') as f:
		for line in f:
			(key, val) = line.split('=');
			tableList[key.strip()] = val.strip();
	with open('backupTable.txt') as f:
		for line in f:
			if ',' in line:
				(key, val) = line.split(',');
				tableList[key.strip()] = val.strip();

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
	print('Authorized.');
	


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
	'''
	if not queryResult:
		return false;
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
		tableID		- Required	: the FusionTable to update (String)
		newRows		- Required	: the values to overwrite the FusionTable with (list of lists)
	'''
	if not tableId or not newRows:
		return;

	# Create a resumable MediaFileUpload containing the "interesting" data to retain.
	upload = MakeMediaFile(newRows, 'staging.csv', True);
	# Try the upload twice (which requires creating a new request).
	if upload and upload.resumable():
		if not StepUpload(FusionTables.table().replaceRows(tableId = tableId, media_body = upload, encoding = 'auto-detect')):
			return StepUpload(FusionTables.table().replaceRows(tableId = tableId, media_body = upload, encoding = 'auto-detect'));
	elif upload:
		if not Upload(FusionTables.table().replaceRows(tableId = tableId, media_body = upload, encoding = 'auto-detect')):
			return Upload(FusionTables.table().replaceRows(tableId = tableId, media_body = upload, encoding = 'auto-detect'));
	return True;



def Upload(request):
	'''
	Upload a non-resumable media file.
	@params:
		request		- Required	: an HttpRequest with an upload Media that might not support next_chunk().
	'''
	if not request:
		return False;

	try:
		response = request.execute(num_retries = 2);
		return True;
	except HttpError as e:
		print('Upload failed:', e);
		return False;



def StepUpload(request):
	'''
	Print the percentage complete for a given upload while it is executing.
	@params:
		request		- Required	: an HttpRequest that supports next_chunk() (i.e., is resumable).
	@return Bool
		False		: the upload failed.
		True		: the upload succeeded.
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
				time.sleep(1000 * (2 ^ fails));
				++fails;
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
    if iteration == total:
        print()



def GetUserBatch(start, limit = 10000):
	'''
	Get a set of members and their internal IDs.
	@params:
		start		- Required	: the first index to return a user for (unsigned).
		limit		- Optional	: the number of data pairs to be returned (unsigned).
	'''
	sql = 'SELECT Member, UID FROM ' + tableList['users'] + ' ORDER BY Member ASC';
	print('Fetching at most', limit, 'members, starting with', start);
	startTime = time.perf_counter();
	resp = GetQueryResult(sql, .1, start, limit);
	try:
		print('Fetched', len(resp['rows']), 'members in', round(time.perf_counter() - startTime, 1), ' sec.');
		return resp['rows'];
	except:
		print('Received no data from user fetch query.')
		return [];



def GetTotalRowCount(tableID):
	'''
	Queries the size of a table, in rows.
	@params:	tableID	- Required	: the FusionTable to determine a row count for.
	@return:	long
	'''
	countSQL = 'select COUNT(ROWID) from ' + tableID;
	print('Fetching total row count for table id', tableID);
	start = time.perf_counter();
	allRowIds = GetQueryResult(countSQL);
	try:
		print('Found', int(allRowIds['rows'][0][0]), 'rows in', round(time.perf_counter() - start, 1), 'sec.)');
		return int(allRowIds['rows'][0][0]);
	except:
		print('Received no data from row count query.');
		return int(0);


def RetrieveWholeRecords(rowids, tableID):
	'''
	Returns a list of lists (i.e. 2D array) corresponding to the full records
	associated with the requested rowids in the specified table.
	@params:
		rowids		- Required	: the rows in the table to be fully obtained (unsigned)
		tableID		- Required	: the FusionTable to obtain records from (String)
	@return:	list of records from the indicated FusionTable.
	'''
	if type(rowids) is not list:
		raise TypeError('Expected list of rowids.');
	elif not rowids:
		raise ValueError('Received empty list of rowids to retrieve.');
	if type(tableID) is not str:
		raise TypeError('Expected string table ID.');
	elif len(tableID) != 41:
		raise ValueError('Received invalid table ID.');
	records=[];
	numNeeded = len(rowids);
	rowids.reverse();
	baseSQL = 'SELECT * FROM ' + tableID + ' WHERE ROWID IN (';
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
	print('Retrieved', numNeeded, 'records in', time.perf_counter() - startTime,'sec.');
	return records;



def IdentifyDiffSeenAndRankRecords(uids, tableID):
	'''
	Returns a list of rowids for which the LastSeen values are unique, or the
	rank is unique (for a given LastSeen), for the given members.
	Uses SQLGet since only GET is done.
	'''
	rowids = [];
	if not tableID:
		tableID = tableList['crowns'];
	if not tableID:
		return rowids;

	uids.reverse();
	lastSeen = {};
	kept = set();
	savings = 0;
	memberCount = len(uids);
	baseSQL = 'SELECT ROWID, Member, UID, LastSeen, Rank FROM ' + tableID + ' WHERE UID IN (';
	print('Sifting through', memberCount, 'member\'s stored data.');
	printProgressBar(memberCount - len(uids), memberCount, "Members sifted: ", "", 1, 50);
	while uids:
		tailSQL = '';
		sqlUIDs = [];
		batch = time.monotonic();
		while uids and (len(baseSQL + tailSQL) <= 8000):
			sqlUIDs.append(uids.pop());
			tailSQL = ','.join(sqlUIDs) + ') ORDER BY LastSeen ASC';
		resp = GetQueryResult(''.join([baseSQL, tailSQL]), 0.2);
		try:
			totalRecords = len(resp['rows']);
		except:
			return [];
		lastSeen.clear();
		kept.clear();
		# The rows increase in LastSeen values, so a single member's rows are
		# scattered throughout it. However, this member will always be only in
		# this response object.
		for row in resp['rows']:
			rowid = row[0].__str__();
			uid = row[2].__str__();
			ls = row[3].__str__();
			r = row[4].__str__();
			# Add a new member
			if uid not in lastSeen:
				lastSeen[uid] = dict(ls=set(r));
				kept.add(rowid);
			# Add a new LastSeen to the dict.
			elif ls not in lastSeen[uid]:
				lastSeen[uid][ls]=set(r);
				kept.add(rowid);
			# Add a new Rank to the set.
			elif r not in lastSeen[uid][ls]:
				lastSeen[uid][ls].add(r);
				kept.add(rowid);
		# Store these new kept rows.
		rowids.extend(kept);
		savings += totalRecords - len(kept);
		elapsed = time.monotonic() - batch;
		if elapsed < .75:
			time.sleep(.75 - elapsed);
		printProgressBar(memberCount - len(uids), memberCount, "Members sifted: ", "", 1, 50);
		
	print('Found', savings, 'values to trim from', savings + len(rowids), 'records');
	return rowids;



def KeepInterestingRecords(tableID):
	'''
	Removes duplicated crown records, keeping each member's records which
	have a new LastSeen value, or a new Rank value.
	'''
	startTime = time.perf_counter();
	if not tableID:
		tableID = tableList['crowns'];
	if not tableID or len(tableID) != 41:
		print('Invalid table id');
		return;
	memberList = GetUserBatch(0, 100000);
	if not memberList:
		print('No members returned');
		return;
	totalRows = GetTotalRowCount(tableID);
	uidList = list(row[1] for row in memberList);
	rowids = IdentifyDiffSeenAndRankRecords(uidList, tableID);
	if not rowids:
		print('No rowids received');
		return;
	if len(rowids) == totalRows:
		print("All records are interesting");
		return;
	# Get the row data associated with the kept rowids
	keptValues = RetrieveWholeRecords(rowids, tableID);
	# Back up the table before we do anything crazy.
	BackupTable(tableID);
	# Do something crazy.
	ReplaceTable(tableID, keptValues);
	print('KeepInterestingRecords: complete');
	print(time.perf_counter() - startTime, ' total sec required.');



def BackupTable(tableID):
	'''
	Creates a copy of the existing MHCC CrownRecord Database and logs the new table id.
	'''
	if not tableID:
		tableID = tableList['crowns'];
	backup = FusionTables.table().copy(tableId=tableID).execute();
	now = datetime.datetime.utcnow();
	newName = 'MHCC_CrownHistory_AsOf_' + '-'.join(x.__str__() for x in [now.year, now.month, now.day, now.hour, now.minute]);
	backup['name'] = newName;
	FusionTables.table().update(tableId=backup['tableId'], body=backup).execute();
	with open('backupTable.txt','a') as f:
		csv.writer(f, quoting=csv.QUOTE_ALL).writerows([[newName, backup['tableId']]]);
	print('Backup completed to new table \'' + newName + '\' with id =', backup['tableId']);
	return newName;



def MakeMediaFile(values, path, isResumable = None):
	'''
	Returns a MediaFile with UTF-8 encoding, for use with FusionTable API calls
	that expect a media_body parameter.
	Also creates a hard disk backup (to facilitate the MediaFile creation).
	'''
	MakeLocalCopy(values, path, 'w');
	return MediaFileUpload(path, mimetype='application/octet-stream', resumable = isResumable);



def MakeLocalCopy(values, path, fileMode):
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
		csv.writer(f, strict=True).writerows(values);



def GetSizeEstimate(values, numSamples):
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



def ReplaceTable(tableID, newValues):
	if type(newValues) is not list:
		raise TypeError('Expected value array as list of lists.');
	elif not newValues:
		raise ValueError('Received empty value array.');
	elif type(newValues[0]) is not list:
		raise TypeError('Expected value array as list of lists.');
	if type(tableID) is not str:
		raise TypeError('Expected string table id.');
	elif len(tableID) != 41:
		raise ValueError('Table id is not of sufficient length.');
	# Estimate the upload size by averaging the size of several random rows.
	print('Replacing table with id =', tableID);
	estSize = GetSizeEstimate(newValues, 5);
	print('Approx. new upload size =', estSize, ' MB.');
	
	start = time.perf_counter();
	newValues.sort();
	print('Replacement completed in' if ReplaceRows(tableID, newValues) else 'Replacement failed after',
	   round(time.perf_counter() - start, 1), 'sec.');



if (__name__ == "__main__"):
	Initialize();
	Authorize();
	KeepInterestingRecords(tableList['crowns']);
	
