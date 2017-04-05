/**
 * function doRecordsMaintenance
 *      Run on set interval to ensure database size does not exceed 250mb. Works in 
 *         partnership with keepInterestingRecords to maintain a reasonable database
 *         size. Function keepInterestingRecords will prune away any duplicated 
 *         UID-LastSeen-Rank values, but so long as the combinations are new, they 
 *         would be kept. This ignores the Rank component and focuses on removing
 *         only the oldest LastSeen values, such that the member has a total number
 *         of records that is less than the maximum. 
 *      Obtains list of UIDs and LastSeen values that can be kept while ensuring the
 *         number of records per user does not exceed the specified maxRecords value.
 *      Then obtains list of ROWIDs corresponding to those UID and LastSeen pairs 
 *         that should be kept.
 *      Then performs a Table.replaceRows() to remove all other records that were not
 *         selected for retention. This avoids costly and time-consuming, per-record
 *         delete by rowid
 */
function doRecordsMaintenance(){
  var maxRecords = 400, startTime = new Date().getTime();
  var keptUIDLastSeen = getKeptUIDLastSeen_(maxRecords);
  var arrayDescription = {'0':{'val':"UID"},'1':{'val':"LastSeen"}};
  var keptRowids = getKeptRowids_(ftid,arrayDescription,keptUIDLastSeen);
  var totalNumRecords = getTotalRowCount_(ftid);
  if ( keptRowids.length < totalNumRecords ) {
    Logger.log('Removing '+(totalNumRecords-keptRowids.length)+' records...');
    var records = retrieveWholeRecords_(keptRowids);
    Logger.log(records.length+' '+keptRowids.length);
//    var replaceProgress = doReplace_(records);
//    if ( replaceProgress.saved ) {
//      return true;
//    } else {
//      throw new Error(replaceProgress.errmsg);
//      return false;
//    }
  } else {
    Logger.log("All records were kept");
  }
}
/**
 * function queryStringMaker_ Based on the inputs, construct the relevant queryStrings with fewer than 
 *                            8100 characters each.
 * @param {String} tblID      The FusionTable identifier for the desired table
 * @param {Array} selectArr   A 1D array of the FusionTable columns that should be selected
 * @param {Array} groupArr    A 1D array of the FusionTable columns that should be grouped on ([] for no group)
 * @param {String} orderStr   A valid column name + " ASC" or " DESC" as appropriate, or '' for no ordering
 * @param {String} limitStr   The number to limit by, or '' for no limiting
 * @param {Object} arrHeader  A nested object with as many primary keys as there are columns in the value
 *                            array. The primary keys should evaluate to the the respective column index
 *                            in the value array. Each object referenced by the primary key should have a
 *                            'val' property indicating the name of the respective FusionTable column
 *                            For example: arrHeader = {"0":{"val":"UID"},"1":{"val":"LastSeen"}}
 * @param {Array} valArray    A 2D array in which each row describes one pairing of the individual values
 *                            and each column contains the values of a given FusionTable column.
 *                            For example: valArray = [ [uid1, LastSeen1], [uid1, LastSeen2], [uid2, LastSeen3] ]
 * @return {Array}            A 1D array of the ROWID in the specified FusionTable in which the provided 
 *                            values were found
 */
function queryStringMaker_(tblID, selectArr, groupArr, orderStr, limitStr, arrHeader, valArray){
  // Verify input types
  if ( typeof tblID != 'string' ) { throw new TypeError('Table identifier should be type string');}
  if ( selectArr.constructor != Array ) { throw new TypeError('SELECT clause values are not given in expected array');}
  if ( groupArr.constructor != Array ) { throw new TypeError('Group By values are not given in expected array');}
  if ( typeof orderStr != 'string' ) { throw new TypeError('Ordering string should be type string');}
  if ( (typeof limitStr != 'string') && (typeof limitStr != 'number') ) { throw new TypeError('Limit string should be type string or integer');}
  if ( typeof arrHeader != 'object' ) { throw new TypeError('Array description should be type object');}
  if ( valArray.constructor != Array ) { throw new TypeError('"WHERE IN (..)" values are not given in expected array');}
  // Verify input contents are valid
  if ( tblID.length != 41 ) { throw new Error('Table ID is not valid FusionTables identifier');}
  var sqlBase = ['SELECT',selectArr.join(", "),'FROM',tblID,'WHERE'].join(" ");
  var groupStr = groupArr.join(", "); // yields "" if groupArr is []
  if ( groupStr.length > 0 ) {
    groupStr = ["GROUP BY",groupStr].join(" ");
  }
  orderStr = String(orderStr).trim();
  if ( orderStr.length > 0 ){
    if ( orderStr.toUpperCase().indexOf("BY") == -1 ) {
      orderStr = ["ORDER BY",orderStr].join(" ");
    }
    if ( orderStr.toUpperCase().indexOf("SC") == -1 ) {
      orderStr = [orderStr.trim(),"ASC"].join(" ");
    }
  }
  if ( (String(limitStr).toUpperCase().indexOf("LIMIT") == -1) && (String(limitStr).trim().length > 0) ){
    limitStr = ["LIMIT",String(limitStr).trim()].join(" ");
  }
  var maxSQLlength = 8100;
  var numDim = Object.keys(arrHeader).length, headerLength = 0, columns = [];
  for (var key in arrHeader){
    if ( arrHeader[key].val === '' ) {
      throw new TypeError('Invalid valArray description');
    } else {
      arrHeader[key].len = arrHeader[key].val.length;
      headerLength += arrHeader[key].len*1;
      columns.push([]);
    }
  }
  var remLength = maxSQLlength - sqlBase.length - groupStr.length - orderStr.length - limitStr.length - headerLength - String(' AND ').length*(numDim-1) - String(' IN ()').length*numDim;
  // remLength is now the maximum length of characters we can add to our query and have it remain processable
  // We will assume that each item is 15 characters long, and include the separator comma as well ( = 16)
  var maxPairsPerQuery = Math.floor(remLength/(numDim*16))
  // Transpose valArray so we can easily splice each column's values for SQL WHERE IN ( ... ) insertion
  var valPrime = [];
  if ( numDim > 1 ) {
    valPrime = arrayTranspose_(valArray);
  } else {
    // Already have splice-ready array
    valPrime = valArray
  }
  var queryStrings = [];
  while ( valPrime[0].length > 0 ) {
    // Construct queryString
    var vals = {}
    for (var key in arrHeader) {
      vals[key]=valPrime[key*1].splice(0,maxPairsPerQuery);
      columns[key*1]=arrHeader[key].val+' IN ('+vals[key].join(',')+')';
    }
    var sql = [sqlBase,columns.join(' AND '),groupStr,orderStr,limitStr].join(" ").replace(/\s\s/gi," ").replace(/\s\s/gi," ").trim();
    // Store queryString
    queryStrings.push(sql);
  }
  return queryStrings;
}
/**
 * function getKeptRowids_   Based on the input value array and object array descriptor, query the 
 *                           indicated table and select the rowids for records having those input 
 *                           values. Will use as many queries as is needed while preventing any one 
 *                           querystring from exceeding the allowed length of 8100 characters.
 * @param {String} tblID     The FusionTable identifier for the desired table
 * @param {Object} arrHeader A nested object with as many primary keys as there are columns in the value
 *                           array. The primary keys should evaluate to the the respective column index
 *                           in the value array. Each object referenced by the primary key should have a
 *                           'val' property indicating the name of the respective FusionTable column
 *                           For example: arrHeader = {"0":{"val":"UID"},"1":{"val":"LastSeen"}}
 * @param {Array} valArray   A 2D array in which each row describes one pairing of the individual values
 *                           and each column contains the values of a given FusionTable column.
 *                           For example: valArray = [ [uid1, LastSeen1], [uid1, LastSeen2], [uid2, LastSeen3] ]
 * @return {Array}           A 1D array of the ROWID in the specified FusionTable in which the provided 
 *                           values were found
 */
function getKeptRowids_(tblID, arrHeader, valArray){
  if ( typeof tblID != 'string' ) { throw new TypeError('Table identifier should be type string');}
  if ( typeof arrHeader != 'object' ) { throw new TypeError('Array description should be type object');}
  if ( valArray.constructor != Array ) { throw new TypeError('Values are not given in expected array');}
  if ( tblID.length != 41 ) { throw new Error('Table ID is not valid FusionTables identifier');}
  var sqlQueries = queryStringMaker_(tblID, ['ROWID'], [], '', '', arrHeader, valArray)||[];

  var rowidArr = []
  if ( sqlQueries.length > 0 ){
    for (var queryNum=0;queryNum<sqlQueries.length;queryNum++){
      var batchStartTime = new Date().getTime();
      try {
        var sql = sqlQueries[queryNum];
        var batchResult = FusionTables.Query.sqlGet(sql);
        rowidArr = [].concat(rowidArr,batchResult.rows);
      }
      catch(e){Logger.log(e);throw new Error('Error while retrieving records by ROWID');}
      var elapsedMillis = new Date().getTime() - batchStartTime;
      if ( elapsedMillis < 750 ) {
        Utilities.sleep(750-elapsedMillis);
      }
    }
  }
  if ( rowidArr.length > 0 ) {
    // Convert from [ [rowid], [rowid], [rowid] ] to [rowid, rowid, rowid]
    var mapTime = new Date().getTime();
    rowidArr = rowidArr.map(function(value, index){return value[0]});
    Logger.log(new Date().getTime() - mapTime);
    return rowidArr;
  }
  return [];
}
/**
 * function getKeptUIDLastSEen_         This function figures out which LastSeen-Rank
 *                                       pairings are to be removed.
 * @param {Integer} maxDiffSeenRecords  The number of maximum different LastSeen crown snapshots a user can have
 * @return {Array[][]}                  An array of [UID,LastSeen,n] where n is the number of times to delete the Member-LastSeen pair
 */
function getKeptUIDLastSeen_(maxDiffSeenRecords){
  var sql1 = "SELECT UID, LastSeen, COUNT() FROM "+ftid+" GROUP BY UID, LastSeen ORDER BY LastSeen DESC";
  var memberRecordCount = FusionTables.Query.sql(sql1);
  if ( typeof memberRecordCount.rows == 'undefined' ) { throw new Error('Could not query for UID-LastSeen-COUNT()'); };
  var recordMap = {}, memberRecordCountList = memberRecordCount.rows, totalNotKept = 0;
  
  // lastSeen DESC sort means the oldest records are at the end of this array
  for (var row=0;row<memberRecordCountList.length;row++){
    var uid = memberRecordCountList[row][0];
    if ( typeof recordMap[uid] == 'undefined' ) {
      recordMap[uid] = {'lastSeen':{},'totalCount':0,'numNotKept':0};
    }
    recordMap[uid].totalCount+=memberRecordCountList[row][2]*1;
    if ( recordMap[uid].totalCount <= maxDiffSeenRecords ) {
      recordMap[uid].lastSeen[memberRecordCountList[row][1]]=memberRecordCountList[row][2]*1;
    } else {
      recordMap[uid].numNotKept += memberRecordCountList[row][2]*1;
      totalNotKept += memberRecordCountList[row][2]*1;
    }
  }
  for (var uid in recordMap){
    if (recordMap[uid].totalCount-recordMap[uid].numNotKept > maxDiffSeenRecords) {
      // Too many total records, even after accounting for the ones that are going to be deleted
      var keys = Object.keys(recordMap[uid].lastSeen);
      while ( (recordMap[uid].totalCount-recordMap[uid].numNotKept > maxDiffSeenRecords) && (keys.length > 0) ) {
        var discarded = keys.pop();
        recordMap[uid].numNotKept+=recordMap[uid].lastSeen[discarded]*1;
        totalNotKept+=recordMap[uid].lastSeen[discarded]*1;
        delete recordMap[uid].lastSeen[discarded];
      }
      Logger.log(uid+": keeping "+keys.length+" lastSeen values");
    }
  }
  Logger.log('Discarding a total of '+totalNotKept+' records');
  // recordMap now contains only uid and lastseen values which should be kept! 
  var uidLS2Keep = [];
  for (var uid in recordMap) {
    for (var ls in recordMap[uid].lastSeen) {
      uidLS2Keep.push([uid,ls]);
    }
  }
  return uidLS2Keep;
}
/**
 * function keepInterestingRecords 
 */
function keepInterestingRecords_(){
  var startTime = new Date().getTime();
  var members = getUserBatch_(0, 100000);
  var rowids = identifyDiffSeenAndRankRecords_(members.map(function(value,index){return value[1]}));
  var totalRows = getTotalRowCount_(ftid);
  var progress = {"saved":false,"errmsg":"","uploadSize":0};
  if ( rowids.length === totalRows ) {
    progress.errmsg = 'All records are interesting';
    progress.saved = true;
  } else if (rowids.length < totalRows ) {
    if ( rowids.length == 0 ) {
      progress.errmsg = 'No rowids returned';
    } else { 
      if (doBackupTable_() === false) {
        progress.errmsg = "Couldn't back up existing table data";
      } else {
        var records = retrieveWholeRecords_(rowids,ftid);
        if ( records.length === 0 ) {
          progress.errmsg = 'No records returned from rowids';
        } else {
          progress = doReplace_(ftid, records)
          if ( progress.saved === false) {
            throw new Error(progress.errmsg);
          }
        }
      }
    }
  } else {
    throw new Error('More interesting records than records... How???');
  }
  Logger.log('keepInterestingRecords: '+((new Date().getTime() - startTime)/1000) +' sec');
  return progress;
}
/**
 * function identifyDiffSeenAndRankRecords_   For the given members, returns the ROWIDs of all records 
 *                                            that have different LastSeen or Ranks. All other records
 *                                            do not have "interesting" data that is not already on 
 *                                            these records.
 * @param {String[]} memUIDs                    The members to query for
 * @return {Array}                            The ROWIDs of these members interesting records
 */
function identifyDiffSeenAndRankRecords_(memUIDs){
  var rowidArray = [];
  memUIDs.reverse();
  // Need to loop over memUIDs in case too many were given
  while ( memUIDs.length > 0 ) {
    var sql = '', sqlUIDs = []
    while (( sql.length <= 8000 ) && ( memUIDs.length > 0 )) {
      sqlUIDs.push(memUIDs.pop());
      sql = "SELECT ROWID, Member, UID, LastSeen, Rank FROM "+ftid+" WHERE UID IN ("+sqlUIDs.join(",")+") ORDER BY LastSeen ASC";
    }
    var resp = FusionTables.Query.sqlGet(sql);
    if ( typeof resp.rows == 'undefined' ) {
      throw new Error('Unable to reach FusionTables');
    } else {
      var lsMap = {}; var rankMap = {};
      var keptArray = [];
      var nRecords = resp.rows.length;
      for (var row=0;row<nRecords;row++){
        var data = resp.rows[row];
        var memLSs = lsMap[data[2]]||[];
        var memRanks = rankMap[data[2]]||[];
        if (( memLSs.indexOf(data[3]) === -1 ) ||        // New LastSeen
            ( memRanks.indexOf(data[4]) === -1 )) {      // OR new Rank
          // Store the ROWID, and add the LastSeen and Rank to the uid objects
          keptArray.push(data[0]);
          if ( memLSs.length === 0 ) {
            lsMap[data[2]]=[data[3]];
          } else {
            lsMap[data[2]].push(data[3]);
          }
          if ( memRanks.length === 0 ) {
            rankMap[data[2]]=[data[4]];
          } else {
            rankMap[data[2]].push(data[4]);
          }
        } else {
          // Already stored a ROWID that has either this LastSeen date or this Rank for this member
        }
      }
      rowidArray = [].concat(rowidArray,keptArray);
    }
  }
  if ( rowidArray.length > 0 ) {
    return rowidArray;
  } else {
    return [];
  }
}
/**
 * function doBookending    This maintenance method will drastically reduce the database size if a significant
 *                          portion of the members are infrequently seen while a much smaller group that is 
 *                          evenly distributed throughout the ranking is considerably more active (as is expected).
 *                          Only records which demarcate crown changes are kept. Within a crown change date, two 
 *                          records are kept per rank attained, as the rank can only worsen until the player gains 
 *                          a new crown change date.
 */
function doBookending(){
  var members = getUserBatch_(0, 100000), startTime = new Date().getTime();
  var rowids = identifyBookendRowids_(members.map(function(value,index){return value[1]}));
  var totalRowCount = getTotalRowCount_(ftid);
  Logger.log('Current non-bookend count: '+(totalRowCount-rowids.length)+' out of '+totalRowCount+' rows');
  var progress = {"saved":false,"errmsg":"","uploadSize":0};
  if ( rowids.length === totalRowCount ) {
    progress.errmsg = 'All records are bookends';
    progress.saved = true;
  } else if (rowids.length < totalRowCount ) {
    if ( rowids.length == 0 ) {
      progress.errmsg = 'No rowids returned';
    } else { 
      if (doBackupTable_() === false) {
        progress.errmsg = "Couldn't back up existing table data";
      } else {
        var records = retrieveWholeRecords_(rowids,ftid);
        if ( records.length === 0 ) {
          progress.errmsg = 'No records returned from rowids';
        } else {
          progress = doReplace_(ftid, records)
          if ( progress.saved === false) {
            throw new Error(progress.errmsg);
          }
        }
      }
    }
  } else {
    throw new Error('More bookending records than records... How???');
  }
  Logger.log('doBookending: '+((new Date().getTime() - startTime)/1000) +' sec');
  return progress;
}
/**
 * function identifyBookendRowids_   For the given members, returns the ROWID array containing each
 *                                   members records that describe 
 *                                     1) every instance of a rank change
 *                                     2) every instance of a crown change
 *                                   If a user is newly seen but didn't get any new crowns, we no 
 *                                   longer need any other records for that user having the same crown
 *                                   change date except for the first one. E.g, keep the first and last
 *                                   instance of each crown change and rank date 
 * @param {String[]} memUIDs         The members to query for
 * @return {Array}                   The ROWIDs of these members' bookending records
 */
function identifyBookendRowids_(memUIDs){
  if ( memUIDs.constructor !== Array ) { throw new TypeError('Expected input to be type Array') };
  if ( typeof memUIDs[0] !== 'string' ) { throw new TypeError('Expected input array to contain strings') };
  
  var startTime = new Date().getTime(), resultArr = []
  while ( memUIDs.length > 0 ) {
    var sql = '', sqlUIDs = [], batchTime = new Date().getTime();
    while (( sql.length <= 8050 ) && ( memUIDs.length > 0 )) {
      sqlUIDs.push(memUIDs.pop());
      sql = "SELECT UID, LastCrown, ROWID, LastSeen, Rank FROM "+ftid+" WHERE UID IN ("+sqlUIDs.join(",")+") ORDER BY LastCrown ASC";
    }
    var resp = FusionTables.Query.sqlGet(sql);
    if ( typeof resp.rows == 'undefined' ) {
      throw new Error('Unable to reach FusionTables');
    } else {
      resultArr = [].concat(resultArr,resp.rows);
      var elapsedMillis = new Date().getTime()-batchTime;
      if ( elapsedMillis < 600 ) Utilities.sleep(601-elapsedMillis);
    }
  }
  // Categorize the large resultArr
  var lcMap = {};
  for (var row=0;row<resultArr.length;row++){
    var data = resultArr[row], usedRow = '';
    // Have we seen this member yet?
    if ( typeof lcMap[data[0]] === 'undefined' ) lcMap[data[0]] = {'lc':{},'numOrig':1,'numDrop':0};
    // Have we seen this member's crown change date before?
    if ( typeof lcMap[data[0]].lc[data[1]] === 'undefined' ) {
      // No, so create a new object array describing when it was first and last seen, and associated ranks
      lcMap[data[0]].lc[data[1]] = {"minLS":{"val":data[3],"rowid":data[2]},
                                    "maxLS":{"val":data[3],"rowid":data[2]},
                                    'minRank':{'val':data[4],'rowid':data[2]},
                                    'maxRank':{'val':data[4],'rowid':data[2]}
                                   }
    } else {
      // Yes, compare vs existing data
      lcMap[data[0]].numOrig++;
      if ( data[3] < lcMap[data[0]].lc[data[1]].minLS.val ) {
        // This row has the same crown change date, but occurred before the stored min value. Replace the stored LastSeen
        lcMap[data[0]].lc[data[1]].minLS = {"val":data[3],"rowid":data[2]};
      } else if ( data[3] > lcMap[data[0]].lc[data[1]].maxLS.val ) {
        // This row has the same crown change date, but occurred after the stored max value. Replace the stored LastSeen
        lcMap[data[0]].lc[data[1]].maxLS = {"val":data[3],"rowid":data[2]};
      } else {
        usedRow = false;
      }
      if ( data[4] < lcMap[data[0]].lc[data[1]].minRank.val ) {
        // Same crown change date, but lower rank than the stored min rank. Updated stored rank.
        lcMap[data[0]].lc[data[1]].minRank = {'val':data[4],'rowid':data[2]};
      } else if ( data[4] > lcMap[data[0]].lc[data[1]].maxRank.val ) {
        // Same crown change date, but higher rank than the stored max rank. Updated stored rank.
        lcMap[data[0]].lc[data[1]].maxRank = {'val':data[4],'rowid':data[2]};
      } else {
        if ( usedRow === '' ) usedRow = false;
      }
      if (usedRow === false) lcMap[data[0]].numDrop++;
    }
  }
  // Push the needed rowids
  var keptRowids = []
  for (var mem in lcMap) {
    for (var lc in lcMap[mem].lc) {
      if ( keptRowids.indexOf(lcMap[mem].lc[lc].minLS.rowid) === -1 ) keptRowids.push(lcMap[mem].lc[lc].minLS.rowid);
      if ( keptRowids.indexOf(lcMap[mem].lc[lc].maxLS.rowid) === -1 ) keptRowids.push(lcMap[mem].lc[lc].maxLS.rowid);
      if ( keptRowids.indexOf(lcMap[mem].lc[lc].minRank.rowid) === -1 ) keptRowids.push(lcMap[mem].lc[lc].minRank.rowid);
      if ( keptRowids.indexOf(lcMap[mem].lc[lc].maxRank.rowid) === -1 ) keptRowids.push(lcMap[mem].lc[lc].maxRank.rowid);
    }
  }
  Logger.log((new Date().getTime()-startTime)/1000 + ' sec to find bookend ROWIDs');
  return keptRowids.sort();   
}
/** 
 * function keepOnlyUniqueRecords          Removes all non-unique records in the crown database. 
 *                                         If the database is large, this could take longer than
 *                                         the script execution time limit. 
 */
function keepOnlyUniqueRecords(){
  var sqlUnique = 'select UID, LastSeen, RankTime, MINIMUM(LastTouched) from '+ftid+' group by UID, LastSeen, RankTime';
  var totalRowCount = getTotalRowCount_(ftid);
  var uniqueRowList = FusionTables.Query.sqlGet(sqlUnique), uniqueRowCount = totalRowCount;
  if ( typeof uniqueRowList.rows == 'undefined' ) {
    throw new Error('No response from FusionTables for sql='+sqlUnique);
  } else {
    uniqueRowCount = uniqueRowList.rows.length;
    if ( (totalRowCount-uniqueRowCount) > 0 ) {
      if (doBackupTable_() == false) {
        throw new Error("Couldn't back up existing table data");
      } else {
        var batchSize = 190, records = [], batchResult = [], nReturned = 0, nRows = 0, rowidArray = [];
        var totalQueries = 1+Math.ceil(uniqueRowCount/batchSize);
        var st = new Date().getTime();
        while ( uniqueRowList.rows.length > 0) {
          var lsArray = [], uidArray = [], rtArray = [], ltArray = [], batchStartTime = new Date().getTime();
          // Construct UID and LastSeen and RankTime arrays to be able to query the ROWID values
          var sql = '';
          while ( (sql.length <= 8010) && (uniqueRowList.rows.length > 0) ) {
            var row = uniqueRowList.rows.pop();
            uidArray.push(row[0]); lsArray.push(row[1]); rtArray.push(row[2]); ltArray.push(row[3]);
            sql = "SELECT ROWID FROM "+ftid+" WHERE LastSeen IN ("+lsArray.join(",")+") AND UID IN ("+uidArray.join(",")+") AND RankTime IN ("+rtArray.join(",")+") AND LastTouched IN ("+ltArray.join(",")+")";
          }
          // Query for the corresponding ROWIDs
          try {
            var rowIDresult = FusionTables.Query.sqlGet(sql);
            nRows += rowIDresult.rows.length*1;
            rowidArray = [].concat(rowidArray, rowIDresult.rows);
            // Avoid exceeding API rate limits (200 / 100 sec and 5 / sec)
            var elapsedMillis = new Date().getTime() - batchStartTime;
            if ( totalQueries > 190 && elapsedMillis < 600) {
              Utilities.sleep(601-elapsedMillis);
            } else if ( elapsedMillis < 200 ) {
              Utilities.sleep(201-elapsedMillis);
            }
          }
          catch(e){
            Logger.log(e);
            throw new Error('Gathering ROWIDs failed');
          }
        }
        Logger.log('Get ROWIDs: '+(new Date().getTime() - st)+' millis');
        st = new Date().getTime()
        // Duplicated records have same LastTouched value
        // Build an {mem:[lt]} object and check against it (since members aren't returned alphabetically)
        var ltMap = {};
        while ( rowidArray.length > 0 ) {
          sql = '';
          var sqlRowIDs = [], batchStartTime = new Date().getTime();
          // Construct ROWID query sql from the list of unique ROWIDs
          while ( (sql.length <= 8050) && (rowidArray.length > 0) ){
            var rowid = rowidArray.pop();
            sqlRowIDs.push(rowid[0])
            sql = "SELECT * FROM "+ftid+" WHERE ROWID IN ("+sqlRowIDs.join(",")+")";
          }
          try {
            batchResult = FusionTables.Query.sqlGet(sql);
            nReturned += batchResult.rows.length*1;
            var kept = [];
            for (var row=0;row<batchResult.rows.length;row++) {
              var memsLTs = ltMap[batchResult.rows[row][1]]||[];
              if ( memsLTs.indexOf(batchResult.rows[row][4]) == -1 ) {
                // Did not find this LastTouched in this member's array of already-added LastTouched values
//                if ( batchResult.rows[row][11] == 1480307602000 ) batchResult.rows[row][11] = batchResult.rows[row][2];
                kept.push(batchResult.rows[row])
                if ( memsLTs.length == 0 ) {
                  ltMap[batchResult.rows[row][1]]=[batchResult.rows[row][4]];
                } else {
                  ltMap[batchResult.rows[row][1]].push(batchResult.rows[row][4]);
                }
              }
            }
            records = [].concat(records, kept);
            // Avoid exceeding API rate limits (30 / min and 5 / sec)
            var elapsedMillis = new Date().getTime() - batchStartTime;
            if ( totalQueries > 190 && elapsedMillis < 600) {
              Utilities.sleep(601-elapsedMillis);
            } else if ( elapsedMillis < 200 ) {
              Utilities.sleep(201-elapsedMillis);
            }
          }
          catch(e){
            Logger.log(e);
            throw new Error('Batchsize likely too large. SQL length was ='+sql.length);
          }
        }
        Logger.log('Get Row Data: '+(new Date().getTime() - st)+' millis');
        st = new Date().getTime();
        if ( records.length === uniqueRowCount ) {
          doReplace_(ftid, records)
        }
        Logger.log('Upload data: '+(new Date().getTime() - st)+' millis');
      }
    } else {
      Logger.log('Cannot trim out any records - only have uniques left!');
    }
  }
}
