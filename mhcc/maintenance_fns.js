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
function queryStringMaker_(tblID, selectArr, groupArr, orderStr, limitStr, arrHeader, valArray)
{
  // Verify input types
  if (typeof tblID != 'string' || tblID.length != 41)
    throw new TypeError('Table identifier should be type string with length 41');
  if (selectArr.constructor != Array)
    throw new TypeError('SELECT clause values are not given in expected array');
  if (groupArr.constructor != Array)
    throw new TypeError('Group By values are not given in expected array');
  if (typeof orderStr != 'string')
    throw new TypeError('Ordering string should be type string');
  if ((typeof limitStr != 'string') && (typeof limitStr != 'number'))
    throw new TypeError('Limit string should be type string or integer');
  if (typeof arrHeader != 'object')
    throw new TypeError('Array description should be type object');
  if (valArray.constructor != Array)
    throw new TypeError('"WHERE IN (..)" values are not given in expected array');

  var sqlBase = ['SELECT', selectArr.join(", "), 'FROM', tblID, 'WHERE'].join(" ");
  var groupStr = groupArr.join(", ");
  if (groupStr.length > 0)
    groupStr = ["GROUP BY", groupStr].join(" ");

  orderStr = String(orderStr).trim();
  if (orderStr.length > 0)
  {
    if (orderStr.toUpperCase().indexOf("BY") == -1)
      orderStr = ["ORDER BY", orderStr].join(" ");
    
    if (orderStr.toUpperCase().indexOf("SC") == -1)
      orderStr = [orderStr.trim(), "ASC"].join(" ");
  }
  if ((String(limitStr).toUpperCase().indexOf("LIMIT") == -1) && (String(limitStr).trim().length > 0))
    limitStr = ["LIMIT", String(limitStr).trim()].join(" ");

  var maxSQLlength = 8100;
  var numDim = Object.keys(arrHeader).length, headerLength = 0, columns = [];
  for (var key in arrHeader)
  {
    if (arrHeader[key].val === '')
      throw new TypeError('Invalid valArray description');
    else
    {
      arrHeader[key].len = arrHeader[key].val.length;
      headerLength += arrHeader[key].len * 1;
      columns.push([]);
    }
  }
  var remLength = maxSQLlength - sqlBase.length - groupStr.length - orderStr.length - limitStr.length - headerLength;
  remLength -= String(' AND ').length * (numDim - 1)
  remLength -= String(' IN ()').length * numDim;
  // remLength is now the maximum length of characters we can add to our query and still process it.
  // Assume that each item is 15 characters long, then include the separator as well ( = 16).
  var maxPairsPerQuery = Math.floor(remLength / (numDim * 16))
  // Transpose valArray so we can easily splice each column's values for SQL WHERE IN ( ... ) insertion
  var valPrime = [];
  if (numDim > 1)
    valPrime = arrayTranspose_(valArray);
  // Already have splice-ready array
  else
    valPrime = valArray

  var queryStrings = [];
  while (valPrime[0].length > 0)
  {
    // Construct queryString
    var vals = {}
    for (var key in arrHeader)
    {
      vals[key] = valPrime[key * 1].splice(0, maxPairsPerQuery);
      columns[key * 1] = arrHeader[key].val + ' IN (' + vals[key].join(',') + ')';
    }
    var sql = [sqlBase, columns.join(' AND '), groupStr, orderStr, limitStr].join(" ").replace(/\s\s/gi, " ").replace(/\s\s/gi, " ").trim();
    // Store queryString
    queryStrings.push(sql);
  }
  return queryStrings;
}
/**
 * function getKeptRowids_    Based on the input value array and object array descriptor, query the
 *                            indicated table and select the rowids for records having those input
 *                            values. Will use as many queries as is needed while preventing any one
 *                            queryString from exceeding the allowed length of 8100 characters.
 * @param {String} tblID      The FusionTable identifier for the desired table.
 * @param {Object} arrHeader  A nested object with one primary keys per column in the value array.
 *                            The primary keys must evaluate to the respective column index in the
 *                            value array. Each object referenced by the primary key should have a
 *                            'val' property that is the name of the respective FusionTable column.
 *                            E.g.: arrHeader = {"0":{"val":"UID"}, "1":{"val":"LastSeen"}}
 * @param {Array[]} valArray  A 2D array in which each row describes one pairing of the individual
 *                            values and each column contains the values of the FusionTable column.
 *                            E.g.: valArray = [ [uid1, LastSeen1], [uid1, LastSeen2], [uid2, LastSeen3] ]
 * @return {Array}            A 1D array of the ROWID in the specified FusionTable in which the provided 
 *                            values were found
 */
function getKeptRowids_(tblID, arrHeader, valArray)
{
  if (typeof tblID != 'string' || tblID.length != 41)
    throw new Error('Table ID is not valid FusionTables identifier');
  else if (typeof arrHeader != 'object')
    throw new TypeError('Array description should be type object');
  else if (valArray.constructor != Array)
    throw new TypeError('Values are not given in expected array');

  var sqlQueries = queryStringMaker_(tblID, ['ROWID'], [], '', '', arrHeader, valArray) || [];
  
  var rowidArr = []
  if (sqlQueries.length > 0)
    for (var queryNum = 0; queryNum < sqlQueries.length; ++queryNum)
    {
      var batchStartTime = new Date().getTime();
      try
      {
        var sql = sqlQueries[queryNum];
        var batchResult = FusionTables.Query.sqlGet(sql);
        rowidArr = [].concat(rowidArr, batchResult.rows);
      }
      catch (e)
      {
        console.error(e);
        throw new Error('Error while retrieving records by ROWID');
      }
      var elapsedMillis = new Date().getTime() - batchStartTime;
      if (elapsedMillis < 500)
        Utilities.sleep(502 - elapsedMillis);
    }

  // Convert from [ [rowid], [rowid], [rowid] ] to [rowid, rowid, rowid]
  if (rowidArr.length > 0)
  {
    rowidArr = rowidArr.map(
      function (value, index) { return value[0] }
    );
    return rowidArr;
  }
  return [];
}
/**
 * function keepInterestingRecords 
 */
function keepInterestingRecords_()
{
  var startTime = new Date().getTime();
  var members = getUserBatch_(0, 100000);
  var rowids = identifyDiffSeenAndRankRecords_(members.map(function(value,index){return value[1]}));
  var totalRows = getTotalRowCount_(ftid);
  if (rowids.length < 1)
    throw new Error('No rowids received from DiffSeenAndRank');
  else if (rowids.length < totalRows)
  {
    if (rowids.length > 0)
    {
      if (doBackupTable_())
      {
        var records = retrieveWholeRecords_(rowids,ftid);
        if (records.length > 0)
          doReplace_(ftid, records);
        else
          throw new Error('Did not retrieve any rows from given rowids');
      }
    }
  }
  Logger.log('keepInterestingRecords: ' + ((new Date().getTime() - startTime) / 1000) + ' sec');
}
/**
 * function identifyDiffSeenAndRankRecords_   Returns the ROWIDs of all members' records having
 *                                            different LastSeen or Ranks. The other records do not
 *                                            have data that is not already on these records.
 * @param {String[]} memUIDs                  The members to query for.
 * @return {Array}                            The ROWIDs of these members interesting records.
 */
function identifyDiffSeenAndRankRecords_(memUIDs)
{
  var rowidArray = [];
  memUIDs.reverse();
  // Need to loop over memUIDs in case too many were given for a single query.
  while (memUIDs.length > 0)
  {
    var sql = '', sqlUIDs = [];
    while ((sql.length <= 8000) && (memUIDs.length > 0))
    {
      sqlUIDs.push(memUIDs.pop());
      sql = "SELECT ROWID, Member, UID, LastSeen, Rank FROM " + ftid + " WHERE UID IN (" + sqlUIDs.join(",") + ") ORDER BY LastSeen ASC";
    }
    var resp = FusionTables.Query.sqlGet(sql);
    if (typeof resp.rows == 'undefined')
      throw new Error('Unable to reach FusionTables');
    else
    {
      var lsMap = {}, keptArray = [], nRecords = resp.rows.length;
      for (var row = 0; row < nRecords; ++row)
      {
        var data = resp.rows[row];
        var rowid = data[0];
        var uid = data[2];
        var ls = data[3];
        var r = data[4];
        // Is this member new?
        if (Object.keys(lsMap).indexOf(uid) < 0)
        {
          lsMap[uid] = {};
          lsMap[uid][ls] = [r];
          keptArray.push(rowid);
        }
        // The member has been seen. Has this LastSeen value been seen?
        else if (Object.keys(lsMap[uid]).indexOf(ls) < 0)
        {
          lsMap[uid][ls] = [r];
          keptArray.push(rowid);
        }
        // This LastSeen has been seen. Has this rank been seen?
        else if (lsMap[uid][ls].indexOf(r) < 0)
        {
          lsMap[uid][ls].push(r);
          keptArray.push(rowid);
        }
        else
        {
          // Already stored a ROWID that has either this LastSeen date or this Rank for this member
        }
      }
      rowidArray = [].concat(rowidArray, keptArray);
    }
  }
  if (rowidArray.length > 0)
    return rowidArray;
  else
    return [];
}
/**
 * function doReplace_      Replaces the contents of the specified FusionTable with the input array
 *                          after sorting on its first element (i.e. alphabetical by Member name).
 *                          If the new records are too large (~10 MB), this call will fail.
 * @param {string} tblID    The table whose contents will be replaced.
 * @param {Array[]} records The new contents of the specified table.
 */
function doReplace_(tblID, records)
{
  if (typeof tblID !== 'string')
    throw new TypeError('Argument tblID was not type String');
  else if (tblID.length !== 41)
    throw new Error('Argument tbldID not a FusionTables id');
  if (records.constructor !== Array)
    throw new TypeError('Argument records was not type Array');
  else if (!records.length)
    throw new Error('Argument records must not be length 0');

  records.sort();
  // Sample a few rows to estimate the size of the upload.
  var uploadSize = getDbSizeData_(records.length, records).totalSize;
  console.info("New data is " + uploadSize + " MB (rounded up)");
  if (uploadSize >= 250)
    throw new Error("Upload size (" + uploadSize + " MB) is too large");

  var cUpload = Utilities.newBlob(array2CSV_(records), 'application/octet-stream');
  try { FusionTables.Table.replaceRows(tblID, cUpload); }
  // Try again if FusionTables didn't respond to the request.
  catch (e)
  {
    if (e.message.toLowerCase() === "empty response")
      FusionTables.Table.replaceRows(tblID, cUpload);
    else
      throw e;
  }
}
/**
 * function retrieveWholeRecords_    Queries for the specified ROWIDs, at most once per 0.5 sec.
 * @param {string[]} rowidArray      A 1D array of string rowids to retrieve (can be very large).
 * @param {string} tblID             The FusionTable which holds the desired records.
 * @returns {Array[]}                A 2D array of the specified records, or [].
 */
function retrieveWholeRecords_(rowidArray, tblID)
{
  if (!rowidArray.length)
    return [];
  else if (typeof rowidArray[0] !== 'string')
    throw new TypeError('Expected ROWIDs of type String but received type ' + typeof rowidArray[0]);

  if (typeof tblID !== 'string')
    throw new TypeError('Expected table id of type String but received type ' + typeof tblID);

  var nReturned = 0, nRowIds = rowidArray.length, records = [];
  do
  {
    var sql = '';
    var sqlRowIDs = [], batchStartTime = new Date();
    // Construct ROWID query sql from the list of unique ROWIDs.
    do
    {
      sqlRowIDs.push(rowidArray.pop());
      sql = "SELECT * FROM " + tblID + " WHERE ROWID IN (" + sqlRowIDs.join(",") + ")";
    } while (sql.length <= 8000 && rowidArray.length > 0);

    try
    {
      var batchResult = FusionTables.Query.sqlGet(sql);
      nReturned += batchResult.rows.length * 1;
      Array.prototype.push.apply(records, batchResult.rows);
    }
    catch (e)
    {
      e.message = "Error while retrieving records by ROWID: " + e.message;
      console.error({ "message": e.message, "response": batchResult, "numGathered": records.length });
      throw e;
    }
    var elapsedMillis = new Date() - batchStartTime;
    if (elapsedMillis < 500)
      Utilities.sleep(502 - elapsedMillis);
  } while (rowidArray.length > 0);

  if (nReturned !== nRowIds)
    throw new Error("Got different number of rows (" + nReturned + ") than desired (" + nRowIds + ")");
  return records;
}
/**
 * Called if UpdateDatabase has no stored information about a member, to obtain their most recent record.
 * The record is used for crown change calculation.
 * @param {string} memUID         The UID of the member who needs a record.
 * @returns {Array[]}             The most recent update for the specified member, or [].
 */
function getMostRecentCrownRecord_(memUID)
{
  if (!memUID.length || memUID.split(",").length > 1)
    throw new Error("Invalid UID input '" + memUID + "'.");

  const recentSql = "SELECT * FROM " + ftid + " WHERE UID = " + memUID + " ORDER BY LastTouched DESC LIMIT 1";
  const resp = FusionTables.Query.sqlGet(recentSql);
  if (!resp || !resp.rows || !resp.rows.length)
    return [];
  else
    return resp.rows[0];
}
/**
 * function doBookending    This maintenance method will drastically reduce the database size if a
 *                          significant portion of the members are infrequently seen, while a much
 *                          smaller group is considerably more active (as is expected). Only records
 *                          which demarcate crown changes are kept (as opposed to just being seen).
 *                          For the same crown change date, two records are kept per rank seen, as
 *                          the rank can only worsen until the player gains a new crown change date.
 */
function doBookending()
{
  var members = getUserBatch_(0, 100000), startTime = new Date().getTime();
  var rowids = identifyBookendRowids_(
    members.map(
      function (value, index) { return value[1] }
    )
  );
  var totalRowCount = getTotalRowCount_(ftid);
  Logger.log('Current non-bookend count: ' + (totalRowCount - rowids.length) + ' out of ' + totalRowCount + ' rows');
  if (rowids.length === totalRowCount)
    Logger.log('All records are bookends');
  else if (rowids.length > totalRowCount)
    throw new Error('More bookending records than records... How???');
  else if (rowids.length == 0)
    throw new Error('No rowids returned for bookending.');
  else if (doBackupTable_() === false)
    throw new Error("Couldn't back up existing table data");
  else
  {
    var records = retrieveWholeRecords_(rowids, ftid);
    if (records.length > 0)
      doReplace_(ftid, records);
    else
      throw new Error('No records returned from given bookend rowids');
  }
  Logger.log('doBookending: ' + ((new Date().getTime() - startTime) / 1000) + ' sec');
}
/**
 * function identifyBookendRowids_  For the given members, returns the ROWID array containing each
 *                                  members' records that describe
 *                                       1) every instance of a rank change
 *                                       2) every instance of a crown change
 *                                  If a user is newly seen, but doesn't get any new crowns, the
 *                                  record is ignored (keeping the earlier record having that same
 *                                  crown change date). 
 * @param {String[]} memUIDs        The members whose data will be kept.
 * @return {Array}                  The ROWIDs of their "bookending" records
 */
function identifyBookendRowids_(memUIDs)
{
  if (memUIDs.constructor !== Array)
    throw new TypeError('Expected input to be type Array');
  else if (memUIDs.length == 0 || typeof memUIDs[0] !== 'string')
    throw new TypeError('Expected input array to contain strings');
  
  var startTime = new Date().getTime(), resultArr = [];
  while (memUIDs.length > 0)
  {
    var baseSQL = 'SELECT UID, LastCrown, ROWID, LastSeen, Rank FROM ' + ftid + ' WHERE UID IN (';
    var tailSQL = '', sqlUIDs = [], batchTime = new Date().getTime(), lengthLimit = 8050 - baseSQL.length;
    while ((tailSQL.length <= lengthLimit) && (memUIDs.length > 0))
    {
      sqlUIDs.push(memUIDs.pop());
      tailSQL = sqlUIDs.join(",") + ") ORDER BY LastCrown ASC";
    }
    var resp = FusionTables.Query.sqlGet(baseSQL + tailSQL);
    if (typeof resp.rows == 'undefined')
      throw new Error('Unable to reach FusionTables');
    else
    {
      resultArr = [].concat(resultArr, resp.rows);
      var elapsedMillis = new Date().getTime() - batchTime;
      if (elapsedMillis < 500)
        Utilities.sleep(502 - elapsedMillis);
    }
  }
  // Categorize the database records.
  var lcMap = {};
  var row = 0, data, cur_uid, cur_lastCrown, cur_row_id, cur_lastSeen, cur_Rank;
  for (; row < resultArr.length; ++row)
  {
    data = resultArr[row];
    cur_uid = data[0];
    cur_lastCrown = data[1];
    cur_row_id = data[2];
    cur_lastSeen = data[3];
    cur_Rank = data[4];
    // Have we seen this member yet?
    if (typeof lcMap[cur_uid] === 'undefined')
      lcMap[cur_uid] = { 'lc': {} };
    // Have we seen this member's crown change date before?
    if (typeof lcMap[cur_uid].lc[cur_lastCrown] === 'undefined')
    {
      // Create a new object describing when it was first and last seen, and associated ranks.
      lcMap[cur_uid].lc[cur_lastCrown] = {
        "minLS": { "val": cur_lastSeen, "rowid": cur_row_id },
        "maxLS": { "val": cur_lastSeen, "rowid": cur_row_id },
        'minRank': { 'val': cur_Rank, 'rowid': cur_row_id },
        'maxRank': { 'val': cur_Rank, 'rowid': cur_row_id }
      };
    }
    else
    {
      // Yes, compare vs existing data
      if (cur_lastSeen < lcMap[cur_uid].lc[cur_lastCrown].minLS.val)
      {
        // This row has the same crown change date, but occurred before the stored min value. Replace the stored LastSeen
        lcMap[cur_uid].lc[cur_lastCrown].minLS = { "val": cur_lastSeen, "rowid": cur_row_id };
      }
      else if (cur_lastSeen > lcMap[cur_uid].lc[cur_lastCrown].maxLS.val)
      {
        // This row has the same crown change date, but occurred after the stored max value. Replace the stored LastSeen
        lcMap[cur_uid].lc[cur_lastCrown].maxLS = { "val": cur_lastSeen, "rowid": cur_row_id };
      }

      if (cur_Rank < lcMap[cur_uid].lc[cur_lastCrown].minRank.val)
      {
        // Same crown change date, but lower rank than the stored min rank. Updated stored rank.
        lcMap[cur_uid].lc[cur_lastCrown].minRank = { 'val': cur_Rank, 'rowid': cur_row_id };
      }
      else if (cur_Rank > lcMap[cur_uid].lc[cur_lastCrown].maxRank.val)
      {
        // Same crown change date, but higher rank than the stored max rank. Updated stored rank.
        lcMap[cur_uid].lc[cur_lastCrown].maxRank = { 'val': cur_Rank, 'rowid': cur_row_id };
      }
    }
  }
  // Push the needed rowids
  var keptRowids = [];
  for (var mem in lcMap)
    for (var changeDate in lcMap[mem].lc)
    {
      var lc = lcMap[mem].lc[changeDate];
      // Rowids are unique to each user, but each user's entry may have the same minimum and maximum
      // LastSeen or Rank for each LastCrown. Only the lc object needs to be checked for duplicates.
      // Restricting this check prevents nasty indexOf() use on a very large (>100,000) array.
      var toAdd = [lc.minLS.rowid];
      if (toAdd.indexOf(lc.maxLS.rowid) === -1)
        toAdd.push(lc.maxLS.rowid);
      if (toAdd.indexOf(lc.minRank.rowid) === -1)
        toAdd.push(lc.minRank.rowid);
      if (toAdd.indexOf(lc.maxRank.rowid) === -1)
        toAdd.push(lc.maxRank.rowid);

      // Add the user's unique rows for this LastCrown to the rowid list for fetching.
      while (toAdd.length != 0)
        keptRowids.push(toAdd.pop());
    }
  Logger.log((new Date().getTime() - startTime) / 1000 + ' sec to find bookend ROWIDs');
  return keptRowids.sort();   
}
/** 
 * function keepOnlyUniqueRecords          Removes all non-unique records in the crown database. 
 *                                         If the database is large, this could take longer than
 *                                         the script execution time limit. 
 */
function keepOnlyUniqueRecords()
{
  var sqlUnique = 'select UID, LastSeen, RankTime, MINIMUM(LastTouched) from ' + ftid + ' group by UID, LastSeen, RankTime';
  var totalRowCount = getTotalRowCount_(ftid);
  var uniqueRowList = FusionTables.Query.sqlGet(sqlUnique), uniqueRowCount = totalRowCount;
  if (typeof uniqueRowList.rows == 'undefined')
    throw new Error('No response from FusionTables for sql='+sqlUnique);
  else
  {
    uniqueRowCount = uniqueRowList.rows.length;
    if ((totalRowCount - uniqueRowCount) > 0)
    {
      if (!doBackupTable_())
        throw new Error("Couldn't back up existing table data");
      else
      {
        var batchSize = 190, records = [], batchResult = [], nReturned = 0, nRows = 0, rowidArray = [];
        var totalQueries = 1 + Math.ceil(uniqueRowCount / batchSize);
        var st = new Date().getTime();
        while (uniqueRowList.rows.length > 0)
        {
          var lsArray = [], uidArray = [], rtArray = [], ltArray = [], batchStartTime = new Date().getTime();
          // Construct UID and LastSeen and RankTime arrays to be able to query the ROWID values
          var sql = '';
          while ((sql.length <= 8010) && (uniqueRowList.rows.length > 0))
          {
            var row = uniqueRowList.rows.pop();
            uidArray.push(row[0]); lsArray.push(row[1]); rtArray.push(row[2]); ltArray.push(row[3]);
            sql = "SELECT ROWID FROM " + ftid + " WHERE LastSeen IN (" + lsArray.join(",") + ") AND UID IN (";
            sql += uidArray.join(",") + ") AND RankTime IN (" + rtArray.join(",") + ") AND LastTouched IN (" + ltArray.join(",") + ")";
          }
          // Query for the corresponding ROWIDs
          try
          {
            var rowIDresult = FusionTables.Query.sqlGet(sql);
            nRows += rowIDresult.rows.length * 1;
            rowidArray = [].concat(rowidArray, rowIDresult.rows);
            // Avoid exceeding API rate limits (200 / 100 sec and 5 / sec)
            var elapsedMillis = new Date().getTime() - batchStartTime;
            if (totalQueries > 190 && elapsedMillis < 600)
              Utilities.sleep(601-elapsedMillis);
            else if (elapsedMillis < 200)
              Utilities.sleep(201-elapsedMillis);
          }
          catch (e)
          {
            console.error(e);
            throw new Error('Gathering ROWIDs failed');
          }
        }
        Logger.log('Get ROWIDs: ' + (new Date().getTime() - st) + ' millis');

        st = new Date().getTime()
        // Duplicated records have same LastTouched value. Build an {mem:[lt]} object and check
        // against it (since members aren't returned alphabetically).
        var ltMap = {};
        while (rowidArray.length > 0)
        {
          var sql = '', sqlRowIDs = [], batchStartTime = new Date().getTime();
          // Construct ROWID query sql from the list of unique ROWIDs.
          while ((sql.length <= 8050) && (rowidArray.length > 0))
          {
            var rowid = rowidArray.pop();
            sqlRowIDs.push(rowid[0])
            sql = "SELECT * FROM " + ftid + " WHERE ROWID IN (" + sqlRowIDs.join(",") + ")";
          }
          try
          {
            batchResult = FusionTables.Query.sqlGet(sql);
            nReturned += batchResult.rows.length * 1;
            var kept = [];
            for (var row = 0; row < batchResult.rows.length; ++row)
            {
              var memsLTs = ltMap[batchResult.rows[row][1]] || [];
              if (memsLTs.indexOf(batchResult.rows[row][4]) == -1)
              {
                // Did not find this LastTouched in this member's array of already-added LastTouched values
                kept.push(batchResult.rows[row])
                if (memsLTs.length == 0)
                  ltMap[batchResult.rows[row][1]]=[batchResult.rows[row][4]];
                else
                  ltMap[batchResult.rows[row][1]].push(batchResult.rows[row][4]);
              }
            }
            records = [].concat(records, kept);
            // Avoid exceeding API rate limits (30 / min and 5 / sec)
            var elapsedMillis = new Date().getTime() - batchStartTime;
            if (totalQueries > 190 && elapsedMillis < 600)
              Utilities.sleep(601-elapsedMillis);
            else if (elapsedMillis < 200)
              Utilities.sleep(201-elapsedMillis);
          }
          catch (e)
          {
            console.error(e);
            throw new Error('Batchsize likely too large. SQL length was =' + sql.length);
          }
        }
        Logger.log('Get Row Data: ' + (new Date().getTime() - st) + ' millis');
        st = new Date().getTime();
        if (records.length === uniqueRowCount)
          doReplace_(ftid, records)
        Logger.log('Upload data: ' + (new Date().getTime() - st) + ' millis');
      }
    }
    else
      console.log('Cannot trim out any records - only have uniques left!');
  }
}
/**
 * function arrayTranspose Transposes the array if it is a 2D array. Throws an error if it is not.
 * @param {Array[]} oldArr   The array to be transposed.
 * @return {Array[]}         The transposed array.
 */
function arrayTranspose_(oldArr)
{
  if (oldArr.constructor != Array)
    throw new TypeError('Array to transpose is not an array');
  else if (oldArr[0].constructor != Array)
    throw new TypeError('Array is 1D - not transposable');
  else if (oldArr[0][0].constructor === Array)
    throw new TypeError('Array has too many dimensions');

  var newArr = [];
  for (var nr = 0; nr < oldArr[0].length; ++nr)
  {
    newArr[nr] = [];
    for (var nc = 0; nc < oldArr.length; ++nc)
      newArr[nr][nc] = oldArr[nc][nr];
  }
  return newArr;
}
/** Function that takes the 12-column Crowns table and exports rank data, operating on sets of users at a time.
 * 
 */
function populateRankFusionTable_()
{
  /**
   * Obtain the UIDs of those members that have data in the Rank DB FusionTable.
   * @return {String[]}
   */
  function _getRankTableUserIDs_()
  {
    var sql = "SELECT UID FROM " + rankTableId + " GROUP BY UID ORDER BY UID ASC";
    var resp = FusionTables.Query.sqlGet(sql, { quotaUser: "maintenance" });
    if (!resp || !resp.rows || !resp.columns)
      return [];
    return (resp.rows.map(function (row) { return String(row[0]); }));
  }
  /**
   * Mutate the input records by determining the appropriate LastSeen and MHCC Crown values for each
   * member's unique RankTime values. If a RankTime is duplicated, only the first instance is kept.
   * 
   * @param {Array[]} records    2D array destined to be uploaded to the Rank DB FusionTable.
   * @param {{uid: {rankTimes: {String: Number}}}} dataMap Object which stores yet-unprocessed RankTimes, and the source row.
   * @param {Array[]} reference  2D array downloaded from the Crown DB FusionTable, which birthed the records array.
   * @return {void}
   */
  function _fillRankRecords_(records, dataMap, reference)
  {
    for (var r = 0; r < records.length; /* mutating */)
    {
      var record = records[r];
      var rt = record[3], UID = String(record[1]);
      // The RankTime will exist, unless we have used it already.
      if (dataMap[UID].rankTimes[rt])
      {
        var desiredIndex = dataMap[UID].rankTimes[rt].row - 1;
        if (desiredIndex >= 0 && String(reference[desiredIndex][1]) === UID)
        {
          var basisRow = reference[desiredIndex];
          // We can use this row's data.
          record[2] = basisRow[2];
          record[5] = basisRow[4];
          // Log the time shift (difference in LastTouched values) we've induced.
          timeShifts.push(reference[desiredIndex + 1][3] - basisRow[3]);
        }
        else { /* Keep blank values for LastSeen and MHCC Crowns */ }

        // We have used this RankTime value.
        delete dataMap[UID].rankTimes[rt];
        ++r;
      }
      // This is a duplicated value of RankTime for this member.
      else
        duplicates.push({ "uid": UID, "value": records.splice(r, 1) });
    }
  }

  const start = new Date();

  // Don't let Apps Script run this function concurrently.
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30000))
    return;

  // Query both tables, to determine who is yet to be operated on.
  const members = getUserBatch_(0, 10000),
    done = _getRankTableUserIDs_(),
    sizeData = getDbSizeData_();

  // Filter the member list by those who have yet to be processed.
  const toDo = members.filter(function (row) { return done.indexOf(String(row[1])) === -1; }),
    rowKey = getCrownTableRowCounts_(),
    logs = [];

  // We only need the Member Name, UID, LastSeen, LastTouched, MHCC Crowns, Rank, and RankTime.
  const SELECT = ("SELECT Member, UID, LastSeen, LastTouched, MHCC, Rank, RankTime FROM " + ftid),
    ORDER = ") ORDER BY UID ASC, RankTime ASC, LastTouched ASC";

  do {
    var uids = getNextUserSet_(toDo, rowKey, 60000, 8000 - SELECT.length - ORDER.length - 15);
    if (!uids || !uids.length) break;

    console.time("Batch execution: " + uids.length + " members.");
    var sql = SELECT + " WHERE UID IN (" + uids.join(",") + ORDER;
    logs.push(sql);
    // Download every record for these members from the MHCC Crowns DB, sorted ASC by RankTime & then LastTouched.
    // The LastTouched value should be greater than the RankTime value for every record.
    var crownRecords = FusionTables.Query.sqlGet(sql, { quotaUser: "maintenance" }).rows;
    // Map between a given RankTime and the row it came from so that the previous row can be queried for
    // LastSeen & MHCC Crown data, and we can remove duplicated RankTime values.
    var rankMapping = {};
    crownRecords.forEach(function (record, index) {
      var UID = String(record[1]);
      if (!rankMapping[UID]) rankMapping[UID] = { "rankTimes": {} };

      // If this is a new RankTime, store it (& the row).
      if (!rankMapping[UID].rankTimes[record[6]]) rankMapping[UID].rankTimes[record[6]] = { "row": index };
    });

    // Create the new Rank DB records from the bulk crown records, leaving LastSeen and MHCC Crown fields empty.
    var rankRecords = crownRecords.map(function (record) {
      //      Member   | UID      | LS         | RankTime | Rank     | MHCC Crowns.
      return [record[0], record[1], /* blank */, record[6], record[5], /* blank */];
    });

    // Use the mapped information to fill in the rows as required.
    var timeShifts = [], duplicates = [];
    _fillRankRecords_(rankRecords, rankMapping, crownRecords);

    // Upload the new rows to the Rank DB FusionTable.
    if (rankRecords.length)
      ftBatchWrite_(rankRecords, rankTableId, false);

    console.timeEnd("Batch execution: " + uids.length + " members.");
    console.log({ "numDuplicates": duplicates.length, "rowsAdded": rankRecords.length, "rowsDownloaded": crownRecords.length });
    console.log({ "time shifts": timeShifts });
  } while (toDo.length && (new Date() - start < 250 * 1000));

  // Log a status report.
  console.info({ "message": logs.length + " queries completed", "logs": logs, "timeUsed": (new Date() - start) / 1000, "# Remaining": toDo.length });

  // Allow running this again.
  lock.releaseLock();
}
/**
 * Function which drops the given columns from the target table. Returns true only if all requested drops were complete.
 * 
 * @param {String} tableId
 * @param {{matchOn:String, columns:any[]}} columnsToDrop An object which indicates what property of the columns should be matched, and the corresponding identifier to match.
 *                                                     For example, matchOn could be 'name', and columns would contain the names of columns to drop.
 * @return {Boolean}
 */
function columnDropper_(tableId, columnsToDrop)
{
  if (!tableId) tableId = ftid;
  if (!columnsToDrop) columnsToDrop = { matchOn: "name", columns: ["Rank", "RankTime"] };

  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30 * 1000) || !columnsToDrop || !columnsToDrop.columns.length)
    return;

  // Make a backup of this table, but do *not* delete the previous one.
  // If no backup is created (for example, FT storage quota is full), quit.
  const backupID = doBackupTable_(tableId, false);
  if (!backupID) return;

  // Ensure a valid ID was given by attempting to read it.
  const options = { quotaUser: "maintenance", "fields": "tableId,name,columns" };
  try { var target = FusionTables.Table.get(tableId, options); }
  catch (e) { console.error(e); return; }

  // Check that the columns exist prior to removing them.
  columnsToDrop.columns.forEach(function (col) {
    if (target.columns.every(function (tc) { return tc[columnsToDrop.matchOn] !== col; }))
      return;
    try { FusionTables.Column.remove(target.tableId, col); }
    catch (e) { console.error(e); }
  });
  lock.releaseLock();

  // Report on the operational success by reading the new resource schema.
  try { var alteredTable = FusionTables.Table.get(target.tableId, options); }
  catch (e) { console.warn(e); }
  const allCompleted = (target.columns.length - columnsToDrop.columns.length === alteredTable.columns.length);
  console.log({ "message": "Column drop " + (allCompleted ? " successful." : " unsuccessful."), "original": target, "altered": alteredTable });
  return allCompleted;
}
/**
   * Query the Crown FusionTable to determine how many rows each member has, and return a UID-indexed object.
   * 
   * @return {{uid:Number}}
   */
function getCrownTableRowCounts_()
{
  var sql = "SELECT UID, Count(LastTouched) FROM " + ftid + " GROUP BY UID";
  var resp = FusionTables.Query.sqlGet(sql, { quotaUser: "maintenance" });
  var output = {};
  resp.rows.forEach(function (row) { output[String(row[0])] = row[1] * 1; });
  return output;
}
/**
 * Return an array of the next set of UIDs to query for migratable records. Makes sure that both the query string length
 * and number of rows queried will be below the supplied arguments.
 * Mutates the input "remaining" array.
 * 
 * @param {String[][]} remaining MUTABLE 2D array of [Name, UID] of members that have yet to be migrated.
 * @param {{uid:Number}} rowKey  Lookup object which stores the number of rows a given UID adds to the overall query.
 * @param {Number} maxRows       The maximum number of rows that should return from a query (i.e. to not generate a "Response > 10 MB" 503 error).
 * @param {Number} maxUIDStrLength The maximum joined length of the queried UIDs (total query string length must be < ~8050 characters).
 * @return {String[]}
 */
function getNextUserSet_(remaining, rowKey, maxRows, maxUIDStrLength)
{
  if (!remaining || !remaining.length)
    return [];
  if (!maxUIDStrLength) maxUIDStrLength = 7900;
  const unknown = remaining.filter(function (memUID) { return (!rowKey[memUID[1]] && rowKey[memUID[1]] !== 0); });
  if (unknown.length)
  {
    console.warn({ "no-row members": unknown });
    throw new Error("Some members not present in the row key");
  }

  // Require the "remaining" set is sorted descending by number of rows.
  remaining.sort(function (a, b) { return rowKey[b[1]] - rowKey[a[1]]; });

  const nextSet = [];
  var nextRows = 0, queryLength = 0;
  // Fill from the front (the largest number of rows) first.
  for (var m = 0; m < remaining.length; /* mutating */)
  {
    var memberRows = rowKey[remaining[m][1]];
    if (memberRows && (nextRows + memberRows < maxRows) && (queryLength + remaining[m][1].length < maxUIDStrLength))
    {
      var uid = String(remaining.splice(m, 1)[0][1]);
      nextSet.push(uid);
      nextRows += memberRows;
      queryLength = nextSet.join(",").length;
    }
    else if (memberRows === 0)
      remaining.splice(m, 1);
    else
      ++m;
    // Stop searching if we can't add another member.
    if (maxUIDStrLength - queryLength < 16)
      break;
  }
  return nextSet;
}
/**
 * HT MostMice's "lst" parameter is a tz-naive string using the local HT time (PST / PDT)
 * Script timezone is America/New_York, and thus Date.parse(lst) returned the incorrect values
 * by 2-4 hours
 * Apps Script has no support for arguments in Date#toLocaleString, so a more complex
 * resolution method must be used.
 * FusionTables does not support multi-record update, so instead we delete and then import fixed rows via ftBatchWrite_().
*/
function correctTimestamps_()
{
  const offsetTable = [
    [new Date("2016/03/13 01:59:59 EST").getTime(), new Date("2016/03/13 01:59:59 PST").getTime(), 4 * 3600 * 1000], // 2016 spring
    [new Date("2016/11/06 01:59:59 EDT").getTime(), new Date("2016/11/06 01:59:59 PDT").getTime(), 2 * 3600 * 1000], // 2016 fall
    [new Date("2017/03/12 01:59:59 EST").getTime(), new Date("2017/03/12 01:59:59 PST").getTime(), 4 * 3600 * 1000], // 2017 spring
    [new Date("2017/11/05 01:59:59 EDT").getTime(), new Date("2017/11/05 01:59:59 PDT").getTime(), 2 * 3600 * 1000], // 2017 fall
    [new Date("2018/03/11 01:59:59 EST").getTime(), new Date("2018/03/11 01:59:59 PST").getTime(), 4 * 3600 * 1000], // 2018 spring
    [new Date("2018/11/04 01:59:59 EDT").getTime(), new Date("2018/11/04 01:59:59 PDT").getTime(), 2 * 3600 * 1000] // 2018 fall
  ];
  /**
   * Determine the offset needed to correct the record's LastSeen and LastCrown timestamps
   * by checking if the record was generated within the DST "activation windows".
   * Returns the number of milliseconds that need to be added to the LastSeen and LastCrown timestamps.
   * 
   * @param {Number} lastTouched
   * @return {Number}
   */
  function _getTzOffset_(lastTouched)
  {
    const offset = 3 * 3600 * 1000;
    for (var y = 0; y < offsetTable.length; ++y)
    {
      if (lastTouched < offsetTable[y][0])
        break;
      else if (lastTouched < offsetTable[y][1])
        return offsetTable[y][2];
    }
    return offset;
  }
  /**
   * Deletes user rows prior to the upload of their new, fixed rows.
   * Returns the number of rows that were modified.
   * 
   * @param {String[]} uidsToDelete The users for whom records are being fixed.
   * @param {String} tableId The table to operate on (Rank DB or Crown DB id)
   * @return {Number} 
   */
  function _deleteUserRows_(uidsToDelete, tableId)
  {
    var sql = "DELETE FROM " + tableId + " WHERE UID IN (" + uidsToDelete.join(",") + ")";
    var resp = FusionTables.Query.sql(sql, { quotaUser: "maintenance" });
    return resp.rows[0][0] * 1;
  }
  function _getLogSheet()
  {
    var sheet = SpreadsheetApp.getActive().getSheetByName("tzshifted");
    return sheet ? sheet : SpreadsheetApp.getActive().insertSheet("tzshifted");
  }
  /**
   * Acquire all rows from the given table for the given users
   * 
   * @param {String[]} usersToGet
   * @param {String} tableId
   * @return {Array[]}
   */
  function _getRows_(usersToGet, tableId)
  {
    var sqlGet = "SELECT * FROM " + tableId + " WHERE UID IN (" + usersToGet.join(",") + ") ORDER BY UID ASC";
    var resp = FusionTables.Query.sqlGet(sqlGet, { quotaUser: "maintenance" });
    return resp.rows;
  }

  const start = new Date();
  // tz info was added to HT querying by checking the localedatestring for "dt" and then adding "-700" or "-800"
  // as appropriate on 2018-05-27 12:39:00 CDT
  const beforeAutofix = new Date("2018-05-27T12:39-0500").getTime();

  const members = getUserBatch_(0, 10000),
    rowKey = getCrownTableRowCounts_(),
    logSheet = _getLogSheet(),
    done = logSheet.getDataRange().getValues().map(function (r) { return String(r[0]); }),
    toDo = members.filter(function (member) { return done.indexOf(String(member[1])) === -1; });

  const prefix = "SELECT * FROM ",
    where = " WHERE UID IN (",
    suffix = ") ORDER BY UID ASC",
    allowedUIDLength = 8050 - (prefix + suffix + where + ftid).length,
    createdOnIndex = { "crown": 4, "rank": 3 },
    seenIndex = { "crown": 2, "rank": 2 },
    ccIndex = 3;

  do {
    // For each set of users
    var uids = getNextUserSet_(toDo, rowKey, 40000, allowedUIDLength);
    if (!uids || !uids.length) break;

    //   1) Query records (some which need fixing and some which do not).
    //   2) Compute the needed offset for records in which LastTouched < beforeAutofix
    //   3) Apply the needed offset to LastSeen & LastCrown for these records
    var fixedCrownRows = _getRows_(uids, ftid).map(function (record) {
      var lastTouched = record[createdOnIndex.crown];
      if (lastTouched < beforeAutofix)
      {
        var offset = _getTzOffset_(lastTouched);
        record[seenIndex.crown] += offset;
        record[ccIndex] += offset;
      }
      return record;
    });
    var fixedRankRows = _getRows_(uids, rankTableId).map(function (record) {
      if (record[seenIndex.rank] !== "NaN")
      {
        var lastTouched = record[createdOnIndex.rank];
        if (lastTouched < beforeAutofix)
          record[seenIndex.rank] += _getTzOffset_(lastTouched);
      }
      // Rather than upload "NaN", keep an empty value where an empty value was.
      for (var c = 0; c < record.length; ++c)
        if (record[c] === "NaN")
          delete record[c];
      return record;
    });

    //   4) Delete their existing rows
    var removed = [
      _deleteUserRows_(uids, ftid),
      _deleteUserRows_(uids, rankTableId)
    ];
    console.log({
      "message": ("Modified rank and crown databases for " + uids.length + " members, " + toDo.length + " remaining."),
      "crownChange": { "table": ftid, "removed": removed[0], "added": fixedCrownRows.length },
      "rankChange": { "table": rankTableId, "removed": removed[1], "added": fixedRankRows.length }
    });
    //   5) Upload the fixed rows
    ftBatchWrite_(fixedCrownRows, ftid);
    ftBatchWrite_(fixedRankRows, rankTableId, false);

    //   6) Add the UIDs of these members to the spreadsheet.
    logSheet.getRange(logSheet.getLastRow() + 1, 1, uids.length, 1).setValues(uids.map(function (u) { return [u]; }));
  } while (toDo.length && (new Date() - start < 300 * 1000));
  if (toDo.length)
    ScriptApp.newTrigger("correctTimestamps").timeBased().at(new Date(new Date().getTime() + 60 * 1000)).create();
  else console.log("All LastSeen and LastCrown records have been corrected.");
}