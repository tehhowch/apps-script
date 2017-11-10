#!  /usr/bin/python

import time

ftKey = None;
tableID = None;

# Script which performs maintenance functions for the MHCC FusionTable and
# initiates a resumable upload to handle the large dataset
def Initialize():
	'''Read in the FusionTable ID and the api keys for oAuth''';
	global ftKey;
	ftKey = ;
	global tableID;
	tableID = ;


def KeepInterestingRecords():
	'''Removes duplicated crown records, keeping those records which have a new LastSeen value, or a new Rank value.''';
	startTime = time.time();
	memberList = GetUserBatch(0, 1e6);
	if(memberList == null):
		print('No members returned');
		return;
	totalRows = GetTotalRowCount(tableID);
	uidList = list([x[1] in memberList]);
	print("uid list:", uidList);
	rowids = IdentifyDiffSeenAndRankRecords(uidList);
	if(rowids == null):
		print('No rowids received');
		return;
	if(len(rowids) == totalRows):
		print("All records are interesting");
		return;





if (__name__ == "__main__"):
	# Run test suite program to make sure functionality is as expected.
	print(KeepInterestingRecords.__doc__);
	Initialize();
	KeepInterestingRecords();
