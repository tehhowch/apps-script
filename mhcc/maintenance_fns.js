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
