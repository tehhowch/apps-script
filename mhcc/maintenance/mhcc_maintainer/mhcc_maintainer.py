#! /usr/bin/python

import time
import datetime
import os
import sys
import csv

from apiclient.discovery import build
from oauth2client import tools
from oauth2client.file import Storage
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import OAuth2WebServerFlow

localKeys = {};

scope = ['https://www.googleapis.com/auth/fusiontables',
		 'https://www.googleapis.com/auth/drive'];

# Script which performs maintenance functions for the MHCC FusionTable and
# initiates a resumable upload to handle the large dataset
def Initialize():
	'''Read in the FusionTable ID and the api keys for oAuth''';
	global localKeys;
	with open('fusion.txt') as f:
		for line in f:
			(key, val) = line.split('=');
			localKeys[key.strip()] = val.strip();



def GetUserBatch(start, limit):
	'''Return up to @limit members, beginning with the index number @start''';
	sql = 'SELECT Member, UID, FROM ' + localKeys['userTable'] + ' ORDER BY Member ASC OFFSET ' + start.__str__() + ' LIMIT ' + limit.__str__();
	miniTable = FusionTables.Query.sql(sql);
	return miniTable.rows;



def GetTotalRowCount(tblID):
	'''Queries the size of a table, in rows.''';
	sql = 'select ROWID from ' + tblID;
	allRowIds = FusionTables.Query.sql(sql);
	return len(allRowIds.rows);


def IdentifyDiffSeenAndRankRecords(uids):
	'''Returns a list of rowids for which the LastSeen values are unique, or the rank is unique, for the given members.''';
	rowids = [];
	uids.reverse();
	lastSeen = {};
	ranks = {};
	kept = set();
	savings = 0;
	baseSQL = 'SELECT ROWID, Member, UID, LastSeen, Rank FROM ' + localKeys['dataTable'] + ' WHERE UID IN (';
	while(len(uids) > 0):
		tailSQL = '';
		sqlUIDs = [];
		while((len(uids) > 0) and (len(baseSQL + tailSQL) <= 8000)):
			sqlUIDs.append(uids.pop());
			tailSQL = ','.join(sqlUIDs) + ') ORDER BY LastSeen ASC';
		resp = FusionTables.Query.sql(baseSQL + tailSQL);
		lastSeen.clear();
		ranks.clear();
		kept.clear();
		totalRecords = len(resp.rows);
		# The rows increase in LastSeen values, so a single member's rows are scattered throughout it.
		for row in resp.rows:
			rowid = row[0].__str__();
			uid = row[2].__str__();
			ls = row[3].__str__();
			r = row[4].__str__();
			# Add a new member or LastSeen.
			if(uid not in lastSeen):
				lastSeen[uid] = set(ls);
				kept.add(rowid);
			elif(ls not in lastSeen[uid]):
				lastSeen[uid].add(ls);
				kept.add(rowid);
			# Add a new member or Rank
			if(uid not in ranks):
				ranks[uid] = set(r);
				kept.add(rowid);
			elif(r not in ranks[uid]):
				ranks[uid].add(r);
				kept.add(rowid);
		# Store these new kept rows.
		rowids.extend(kept);
		savings += totalRecords - len(kept);
	print('Eliminating', savings, 'values out of ', savings + len(rowids), 'records');
	return rowids;



def KeepInterestingRecords():
	'''Removes duplicated crown records, keeping those records which have a new LastSeen value, or a new Rank value.''';
	startTime = time.time();
	memberList = GetUserBatch(0, 100000);
	if(memberList == null):
		print('No members returned');
		return;
	totalRows = GetTotalRowCount(localKeys['dataTable']);
	uidList = list([x[1] in memberList]);
	print("uid list:", uidList);
	rowids = IdentifyDiffSeenAndRankRecords(uidList);
	if(rowids == null):
		print('No rowids received');
		return;
	if(len(rowids) == totalRows):
		print("All records are interesting");
		return;
	BackupTable();
	ReplaceTable();
	print('KeepInterestingRecords: complete');
	print((time.time() - startTime).__str__() + ' sec required.');



def BackupTable():
	'''Creates a copy of the existing MHCC CrownRecord Database and logs the new table id.''';
	backup = FusionTables.Table.copy(localKeys['dataTable']);
	now = datetime.datetime.utcnow();
	newName = 'MHCC_CrownHistory_AsOf_' + '-'.join(x.__str__() for x in [now.year, now.month, now.day, now.hour, now.minute]);
	backup.name = newName;
	FusionTables.Table.update(backup, backup.tableId);
	with open('backupTable.txt','a') as f:
		f.write(newName + backup.tableId);
	


if (__name__ == "__main__"):
	# Run test suite program to make sure functionality is as expected.
	Initialize();
	flow = OAuth2WebServerFlow(localKeys['client_id'], localKeys['client_secret'], scope);
	storage = Storage('credentials.dat');
	credentials = storage.get();
	if(credentials is None or credentials.invalid):
		credentials = tools.run_flow(flow, storage, tools.argparser.parse_args());
	else:
		print('Valid Credentials');
	KeepInterestingRecords();
