#! /usr/bin/python

import time
import datetime
import csv
import random

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from oauth2client import tools
from oauth2client.file import Storage
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import OAuth2WebServerFlow

localKeys = {};

scope = ['https://www.googleapis.com/auth/fusiontables',
		 'https://www.googleapis.com/auth/drive'];
FusionTables = None;

# Script which performs maintenance functions for the MHCC FusionTable and
# initiates a resumable upload to handle the large dataset
def Initialize():
	'''Read in the FusionTable ID and the api keys for oAuth''';
	global localKeys;
	with open('fusion.txt') as f:
		for line in f:
			(key, val) = line.split('=');
			localKeys[key.strip()] = val.strip();
	print('Initialized.');



def Authorize():
	'''Authenticate the requested Google API scopes for a single user.'''
	flow = OAuth2WebServerFlow(localKeys['client_id'], localKeys['client_secret'], scope);
	storage = Storage('credentials.dat');
	credentials = storage.get();
	if(credentials is None or credentials.invalid):
		credentials = tools.run_flow(flow, storage, tools.argparser.parse_args());
	else:
		print('Valid Credentials');
	global FusionTables;
	FusionTables = build('fusiontables', 'v2', credentials=credentials);#, cache='.cache');
	print('Authorized.');
	


def GetUserBatch(start, limit):
	'''Return up to @limit members, beginning with the index number @start
	Uses SQLGet since only GET is done.''';
	sql = 'SELECT Member, UID FROM ' + localKeys['userTable'] + ' ORDER BY Member ASC OFFSET ' + start.__str__() + ' LIMIT ' + limit.__str__();
	print('Fetching at most', limit, 'members, starting with', start);
	start = time.perf_counter();
	resp = FusionTables.query().sqlGet(sql=sql).execute();
	print('Fetched', len(resp['rows']), 'members in', round(time.perf_counter() - start, 1), ' sec.');
	return resp['rows'];



def GetTotalRowCount(tblID):
	'''Queries the size of a table, in rows.
	Uses SQLGet since only GET is done.''';
	countSQL = 'select COUNT(ROWID) from ' + tblID;
	print('Fetching total row count for table id', tblID);
	start = time.perf_counter();
	allRowIds = FusionTables.query().sqlGet(sql=countSQL).execute();
	print('Fetched total row count in', round(time.perf_counter() - start, 1),'sec.');
	return allRowIds['rows'][0];



def RetrieveWholeRecords(rowids, tblID):
	'''Returns a list of lists (i.e. 2D array) corresponding to the full records
	associated with the requested rowids in the specified table.
	Uses SQLGet since only GET is done.''';
	if(type(rowids) is not list):
		raise TypeError('Expected list of rowids.');
	elif(not rowids):
		raise ValueError('Received empty list of rowids to retrieve.');
	if(type(tblID) is not str):
		raise TypeError('Expected string table ID.');
	elif(len(tblID) != 41):
		raise ValueError('Received invalid table ID.');
	records=[];
	numNeeded = len(rowids);
	rowids.reverse();
	baseSQL = 'SELECT * FROM ' + tblID + ' WHERE ROWID IN (';
	print('Retrieving', numNeeded, 'records.');
	startTime = time.perf_counter();
	while(rowids):
		tailSQL = '';
		sqlROWIDs = [];
		batchTime = time.monotonic();
		while(rowids and (len(baseSQL + tailSQL) <= 8000)):
			sqlROWIDs.append(rowids.pop());
			tailSQL = ','.join(sqlROWIDs) + ')';
		# Fetch the batch of records.
		resp = FusionTables.query().sqlGet(sql=''.join([baseSQL, tailSQL])).execute();
		records.extend(resp['rows']);
		elapsed = time.monotonic() - batchTime;
		# Rate Limit
		if(elapsed < .75):
			time.sleep(.75 - elapsed);
	if(len(records) != numNeeded):
		raise LookupError('Obtained different number of records than specified');
	print('Retrieved', numNeeded, 'records in', time.perf_counter() - startTime,'sec.');
	return records;



def IdentifyDiffSeenAndRankRecords(uids):
	'''Returns a list of rowids for which the LastSeen values are unique, or the
	rank is unique (for a given LastSeen), for the given members.
	Uses SQLGet since only GET is done.''';
	rowids = [];
	uids.reverse();
	lastSeen = {};
	kept = set();
	savings = 0;
	baseSQL = 'SELECT ROWID, Member, UID, LastSeen, Rank FROM ' + localKeys['dataTable'] + ' WHERE UID IN (';
	print('Sifting through', len(uids), 'member\'s stored data.');
	while(uids):
		tailSQL = '';
		sqlUIDs = [];
		batch = time.monotonic();
		while((uids) and (len(baseSQL + tailSQL) <= 8000)):
			sqlUIDs.append(uids.pop());
			tailSQL = ','.join(sqlUIDs) + ') ORDER BY LastSeen ASC';
		resp = FusionTables.query().sqlGet(sql=''.join([baseSQL, tailSQL])).execute();
		lastSeen.clear();
		kept.clear();
		totalRecords = len(resp['rows']);
		# The rows increase in LastSeen values, so a single member's rows are
		# scattered throughout it. However, this member will always be only in
		# this response object.
		for row in resp['rows']:
			rowid = row[0].__str__();
			uid = row[2].__str__();
			ls = row[3].__str__();
			r = row[4].__str__();
			# Add a new member
			if(uid not in lastSeen):
				lastSeen[uid] = dict(ls=set(r));
				kept.add(rowid);
			# Add a new LastSeen to the dict.
			elif(ls not in lastSeen[uid]):
				lastSeen[uid][ls]=set(r);
				kept.add(rowid);
			# Add a new Rank to the set.
			elif(r not in lastSeen[uid][ls]):
				lastSeen[uid][ls].add(r);
				kept.add(rowid);
		# Store these new kept rows.
		rowids.extend(kept);
		savings += totalRecords - len(kept);
		elapsed = time.monotonic() - batch;
		if(elapsed < .75):
			time.sleep(.75 - elapsed);

	print('Found', savings, 'values to trim from', savings + len(rowids), 'records');
	return rowids;



def KeepInterestingRecords():
	'''Removes duplicated crown records, keeping each member's records which
	have a new LastSeen value, or a new Rank value.''';
	startTime = time.perf_counter();
	memberList = GetUserBatch(0, 100000);
	if(not memberList):
		print('No members returned');
		return;
	totalRows = GetTotalRowCount(localKeys['dataTable']);
	uidList = list(row[1] for row in memberList);
	rowids = IdentifyDiffSeenAndRankRecords(uidList);
	if(not rowids):
		print('No rowids received');
		return;
	if(len(rowids) == totalRows):
		print("All records are interesting");
		return;
	# Get the row data associated with the kept rowids
	keptValues = RetrieveWholeRecords(rowids, localKeys['dataTable']);
	# Back up the table before we do anything crazy.
	BackupTable();
	# Do something crazy.
	ReplaceTable(localKeys['dataTable'], keptValues);
	print('KeepInterestingRecords: complete');
	print(time.perf_counter() - startTime, ' total sec required.');



def BackupTable():
	'''Creates a copy of the existing MHCC CrownRecord Database and logs the new table id.''';
	backup = FusionTables.table().copy(tableId=localKeys['dataTable']).execute();
	now = datetime.datetime.utcnow();
	newName = 'MHCC_CrownHistory_AsOf_' + '-'.join(x.__str__() for x in [now.year, now.month, now.day, now.hour, now.minute]);
	backup['name'] = newName;
	FusionTables.table().update(tableId=backup['tableId'], body=backup).execute();
	with open('backupTable.txt','a') as f:
		csv.writer(f, quoting=csv.QUOTE_ALL).writerows([[newName, backup['tableId']]]);
	print('Backup completed to new tableId =', backup['tableId']);
	return newName;



def MakeMediaFile(values):
	'''Returns a mediafile with UTF-8 encoding, for use with FusionTable API calls
	that expect a media_body parameter.
	Also creates a hard disk backup.''';
	with open('staging.csv', 'w', newline='', encoding='utf-8') as f:
		csv.writer(f, strict=True).writerows(values);
	return MediaFileUpload('staging.csv', mimetype='application/octet-stream');
	


def GetSizeEstimate(values, numSamples):
	'''Estimates the upload size of a 2D array without needing to make the actual
	upload file. Averages @numSamples different rows to improve result statistics.''';
	rowCount = len(values);
	if(numSamples >= .1 * rowCount):
		numSamples = round(.03 * rowCount);
	sampled = random.sample(range(rowCount), numSamples);
	uploadSize = 0.;
	for row in sampled:
		uploadSize += len((','.join(col.__str__() for col in values[row])).encode('utf-8'));
	uploadSize *= float(rowCount) / (1024 * 1024 * numSamples);
	return uploadSize;



def ReplaceTable(tblID, newValues):
	if(type(newValues) is not list):
		raise TypeError('Expected value array as list of lists.');
	elif(not newValues):
		raise ValueError('Received empty value array.');
	elif(type(newValues[0]) is not list):
		raise TypeError('Expected value array as list of lists.');
	if(type(tblID) is not str):
		raise TypeError('Expected string table id.');
	elif(len(tblID) != 41):
		raise ValueError('Table id is not of sufficient length.');
	# Estimate the upload size by averaging the size of several random rows.
	print('Replacing table with id =', tblID);
	estSize = GetSizeEstimate(newValues, 5);
	print('Approx. new upload size =', estSize, ' MB.');

	start = time.perf_counter();
	newValues.sort();
	mediaFile = MakeMediaFile(newValues);

	response = FusionTables.table().replaceRows(tableId=tblID, media_body=mediaFile, encoding='auto-detect').execute();
	print('Replacement completed in', round(time.perf_counter() - start, 1), 'sec.');



if (__name__ == "__main__"):
	Initialize();
	Authorize();
	KeepInterestingRecords();
	