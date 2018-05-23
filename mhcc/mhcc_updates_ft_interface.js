/**
* Google FusionTables to serve as the backend
* Structural changes: since updating/deleting rows is a 1-by-1 operation (yes, really >_> ), we
* will have multiple entries for each person. To prevent excessive table growth, we will store only
* 30 snapshots per user, removing any duplicates which occur when the script updates more often than
* the person's data is refreshed.
* Bonus: this means we can serve new data such as most crowns in the last 30 days.
*/
// Table of the user names and their associated UID.
var utbl = '1O4lBLsuvfEzmtySkJ-n-GjOSD_Ab2GsVgNIX8uZ-';
// Table of the user's crown data. Unique on LastTouched.'
var ftid = '1hGdCWlLjBbbd-WW1y32I2e-fFqudDgE3ev-skAff';

/**
 * function getUserBatch_       Return up to @limit members, beginning with the index @start.
 * @param  {Integer} start      First retrieved index (relative to an alphabetical sort on members).
 * @param  {Integer} limit      The maximum number of pairs to return.
 * @return {String[][]}         [Name, UID]
 */
function getUserBatch_(start, limit)
{
  var sql = "SELECT Member, UID FROM " + utbl + " ORDER BY Member ASC OFFSET " + start + " LIMIT " + limit;
  var miniTable = FusionTables.Query.sqlGet(sql);
  return miniTable.rows;
}

/**
 * function getDbIndexMap_  Assemble a dictionary for the db based on the user's UID (which is to be
                            unique within any given Scoreboard update). For the MHCC SheetDb db, the
                            UID is array index [1].
 * @param  {Array[]} db     A 2D array of the most recent Scoreboard update.
 * @return {Object}         A simple dictionary with the key-value pair of {UID: dbIndex}.
 */
function getDbIndexMap_(db, numMembers, isRepeat)
{
  var output = {};
  for (var i = 0; i < db.length; ++i)
    output[String(db[i][1])] = i;
  // If there are fewer unique rows on SheetDb than numMembers, some rows may be missing (i.e. a new
  // member was added since the last scoreboard updates. If there are exactly as many total rows on
  // SheetDb as numMembers, though, this generally means that a UID appeared twice. This is not a
  // strict requirement, as combinations of member removal and member addition can reproduce it.
  if ((Object.keys(output).length < numMembers * 1) && (db.length === numMembers * 1))
  {
    console.warn({message: "UID failed to be unique when indexing SheetDb", dbIndex: output, firstTry: !isRepeat});
    if(!isRepeat)
      return getDbIndexMap_(refreshDbIndexMap_(), numMembers, true);
    else
      throw new Error('Unique ID failed to be unique. Rewriting SheetDb index before next call...');
  }
  return output;
}

/**
 * function refreshDbIndexMap_  Rewrite SheetDb with fresh data from the FusionTable. Called if there
 *                              is a uniqueness error.
 * @return {Array[]}            Returns the new db.
 */
function refreshDbIndexMap_()
{
  var ss = SpreadsheetApp.getActive();
  saveMyDb_(ss, getLatestRows_());
  return getMyDb_(ss, 1);
}

/**
 * function getLatestRows_    Returns the record associated with the most recently touched snapshot
 *                            for each current member. Called by UpdateScoreboard. Returns a single
 *                            record per member so long as every record has a different LastTouched
 *                            value. First finds each UID's maximum LastTouched value, then gets the
 *                            associated row (after verifying the member is current).  A query with
 *                            "UID IN ( ... ) AND LastTouched IN ( ... )" would greatly *relax* the
 *                            unique LastTouched requirement, but would not eliminate it.
 * @return {Array[]}          N-by-crownDBnumColumns Array for UpdateScoreboard to parse.
 */
function getLatestRows_()
{
  // Query for the most recent records of all persons with data.
  var ltSQL = "SELECT UID, MAXIMUM(LastTouched) FROM " + ftid + " GROUP BY UID";
  var mostRecentRecordTimes = FusionTables.Query.sqlGet(ltSQL);
  if (!mostRecentRecordTimes || !mostRecentRecordTimes.rows || !mostRecentRecordTimes.rows.length)
  {
    console.error({message: "Invalid response from FusionTable API.", data: mostRecentRecordTimes});
    return [];
  }
  
  // Query for those members that are still current.
  var members = getUserBatch_(0, 100000);
  if (!members || !members.length || !members[0].length)
  {
    console.error({message: "No members returned from call to getUserBatch", data: members});
    return [];
  }

  // Construct an associative map object for checking the uid from records against current members.
  var valids = {};
  members.forEach(function (pair) { valids[pair[1]] = pair[0]});
  
  // The maximum SQL request length for the API is ~8100 characters (via POST), e.g. ~571 17-digit time values.
  var totalQueries = 1 + Math.ceil(members.length / 571);
  var snapshots = [], batchResult = [];
  var baseSQL = "SELECT * FROM " + ftid + " WHERE LastTouched IN (";
  var tail = ") ORDER BY Member ASC";
  
  do {
    // Assemble a query.
    var lastTouched = [], batchStart = new Date();
    do {
      var member = mostRecentRecordTimes.rows.pop();
      if (valids[member[0]])
        lastTouched.push(member[1]);
    } while (mostRecentRecordTimes.rows.length > 0 && (baseSQL + lastTouched.join(",") + tail).length < 8050);
    
    // Execute the query.
    try
    {
      batchResult = FusionTables.Query.sqlGet(baseSQL + lastTouched.join(",") + tail);
    }
    catch (e)
    {
      console.error({message: "Error while fetching whole rows", sql: baseSQL + lastTouched.join(",") + tail, error: e});
      return [];
    }
    snapshots = [].concat(snapshots, batchResult.rows);
    
    // Sleep to avoid exceeding ratelimits (30 / min, 5 / sec).
    var elapsed = new Date() - batchStart;
    if (totalQueries > 29)
      Utilities.sleep(2001 - elapsed);
    else if (elapsed < 200)
      Utilities.sleep(201 - elapsed);
  } while (mostRecentRecordTimes.rows.length > 0);
  
  // Log any current members who weren't included in the fetched data.
  if (snapshots.length !== members.length)
  {
    for(var rc = 0, len = snapshots.length; rc < len; ++rc)
      delete valids[snapshots[rc][1]];
    console.debug({message: "Some members lack scoreboard records", data: valids});
  }
  return snapshots;
}

/**
 * function getUserHistory_   Queries the crown data snapshots for the given user and returns crown
 *                            counts and rank data as a function of HT MostMice's LastSeen property.
 * @param  {String} UID       The UIDs of specific members for which the crown data snapshots are
 *                            returned. If multiple members are queried, use comma separators.
 * @param  {Boolean} blGroup  Optional parameter controlling GROUP BY or "return all" behavior.
 * @return {Object}           An object containing "user" (member's name), "headers", and "dataset".
 */
function getUserHistory_(UID, blGroup)
{
  var sql = 'SELECT Member, LastSeen, Bronze, Silver, Gold, MHCC, Rank, ';
  if (UID == '')
    throw new Error('No UID provided');
  if (blGroup == true)
    sql += "MINIMUM(RankTime) FROM " + ftid + " WHERE UID IN (" + UID.toString() + ") GROUP BY Member, LastSeen, Bronze, Silver, Gold, MHCC, Rank ORDER BY LastSeen ASC";
  else
    sql += "RankTime FROM " + ftid + " WHERE UID IN (" + UID.toString() + ") ORDER BY LastSeen ASC";

  var resp = FusionTables.Query.sqlGet(sql, {quotaUser: String(UID)});
  if (typeof resp.rows == 'undefined')
    throw new Error('No data for UID=' + UID);

  if (resp.rows.length > 0)
    return {"user": resp.rows[resp.rows.length - 1][0], "headers": resp.columns, "dataset": resp.rows};
  else
    return "";
}

/**
 * function ftBatchWrite_     Convert the data array into a CSV blob and upload to FusionTables.
 * @param  {Array[]} hdata    The 2D array of data that will be written to the database.
 * @return {Integer}          Returns the number of rows that were added to the database.
 */
function ftBatchWrite_(hdata)
{
  // hdata[][] [Member][UID][Seen][Crown][Touch][br][si][go][si+go][squirrel][RankTime]
  var crownCsv = array2CSV_(hdata);
  try
  {
    var cUpload = Utilities.newBlob(crownCsv, 'application/octet-stream');
  }
  catch (e)
  {
    e.message = "Unable to convert array into CSV format: " + e.message;
    console.error({message: e.message, input: hdata, csv: crownCsv, blob: cUpload});
    throw e;
  }

  try
  {
    var numAdded = FusionTables.Table.importRows(ftid, cUpload);
    return numAdded.numRowsReceived * 1;
  }
  catch (e)
  {
    e.message = "Unable to upload rows: " + e.message;
    console.error(e);
    var didPrint = false, badRows = 0, example = [];
    for (var row = 0, len = hdata.length; row < len; ++row)
      if (hdata[row].length !== crownDBnumColumns)
      {
        ++badRows;
        if(!didPrint)
        {
          example = hdata[row];
          didPrint = true;
        }
      }
    console.warn({message: badRows + ' rows with incorrect column count out of ' + hdata.length, example: example});
    throw e;
  }
}

/**
 * function getByteCount_   Computes the size in bytes of the passed string
 * @param  {String} str     The string to analyze
 * @return {Long}           The bytesize of the string, in bytes
 */
function getByteCount_(str)
{
  return Utilities.base64EncodeWebSafe(str).split(/%(?:u[0-9A-F]{2})?[0-9A-F]{2}|./).length - 1;
}
/**
 * function val4CSV_        Inspect all elements of the array and ensure the values are strings.
 * @param  {Object} value   The element of the array to be escaped for encapsulation into a string.
 * @return {String}         A string representation of the passed element, with special character
 *                          escaping and double-quoting where needed.
 */
function val4CSV_(value)
{
  var str = (typeof(value) === 'string') ? value : value.toString();
  if (str.indexOf(',') != -1 || str.indexOf("\n") != -1 || str.indexOf('"') != -1)
    return '"'+str.replace(/"/g,'""')+'"';
  else
    return str;
}
/**
 * function row4CSV_        Pass every element of the given row to a function to ensure the elements
 *                          are escaped strings and then join them into a CSV string with commas.
 * @param  {Array} row      A 1-D array of elements which may be strings or other types.
 * @return {String}         A string of the joined array elements.
 */
function row4CSV_(row)
{
  return row.map(val4CSV_).join(",");
}
/**
 * function array2CSV_      Pass every row of the input array to a function that converts them into
 *                          escaped strings, join them with CRLF, and return the CSV string.
 * @param  {Array[]} myArr  A 2D array to be converted into a string representing a CSV.
 * @return {String}         A string representing the rows of the input array, joined by CRLF.
 */
function array2CSV_(myArr)
{
  return myArr.map(row4CSV_).join("\r\n");
}
/**
 * function getNewLastRanValue_     Determines the new lastRan parameter value based on the existing
 *                                  lastRan parameter, the original 'lastRan' user, and the slice of
 *                                  the memberlist near the old lastRan, ± number of changed rows.
 * @param {String} origUID          The UID of the most recently updated member.
 * @param {Long} origLastRan        The original value of lastRan prior to any memberlist updates.
 * @param {Array[]} diffMembers     The member names that were added or deleted: [Member, UID]
 * @return {Long}                   The value of lastRan that ensures the next update will not
 *                                  skip/redo any preexisting member.
 */
function getNewLastRanValue_(origUID, origLastRan, diffMembers)
{
  if (origLastRan === 0)
    return 0;

  var newLastRan = -10000, differential = diffMembers.length;
  var newUserBatch = getUserBatch_(origLastRan - differential, 2 * differential + 1);
  // Check if lastRan is beyond the scope of the member list. If it is, do nothing.
  if (!newUserBatch.length)
    return origLastRan;
  
  // If the original UID is found in the shifted UIDs, then the offset is simple.
  var newUIDs = newUserBatch.map(function (value) { return value[1] } );
  var diffIndex = newUIDs.indexOf(origUID);
  if (diffIndex > -1)
    newLastRan = origLastRan + (diffIndex - differential);
  else
  {
    // LastRan was pointing at one of the deleted members.
    console.log("Exactly removed the lastRan member. Not changing lastRan, even if it causes issues (which it shouldn't).");
    if (diffMembers.length == 1)
    {
      // Only removed 1 member, and that was the very next member in line for updating. Therefore, no change is needed.
      newLastRan = origLastRan
    }
    else
    {
      // Tough case here: we lost the exact point of reference needed for determining if deletions were before or after
      // where we've updated. Shortcut: no-op.
      newLastRan = origLastRan
    }
  }
  
  return ((newLastRan < 0) ? 0 : newLastRan);
}
/**
 * function getDbSize           Determines the size of the database by extrapolating from the size
 *                              of a random row, and reports the result via spreadsheet "toast".
 *                              Maximum number of selected rows for this db is ~53900
 *                              53900 rows at 0.5r kb per row is about 7.8 MB of data
 */
function getDbSize()
{
  var sizeData = getDbSizeData_();
  var sizeStr = 'The crown database has ' + sizeData['nRows'] + ' entries, each consuming ~';
  sizeStr += sizeData['kbSize'] + ' kB of space, based on ';
  sizeStr += sizeData['samples'] + ' sampled rows.<br>The total database size is ~';
  sizeStr += sizeData['totalSize'] + ' mB.<br>The maximum size allowed is 250 MB.';
  SpreadsheetApp.getUi().showModalDialog(HtmlService.createHtmlOutput(sizeStr), "Database Size");
}
function getDbSizeData_()
{
  var nRows = getTotalRowCount_(ftid);
  var toGet = [], samples = 10;
  for (var n = 0; n < samples; ++n)
    toGet.push(Math.floor(nRows * Math.random()));

  var rowSizes = [];
  toGet.forEach(function (row) {
    var resp = FusionTables.Query.sqlGet("SELECT * FROM " + ftid + " OFFSET " + row + " LIMIT 1");
    if(!resp.rows || !resp.rows.length || !resp.rows[0].length)
    {
      rowSizes.push(0);
      --samples;
    }
    else
      rowSizes.push(getByteCount_(resp.rows[0].toString()));
  });
  if(!samples) return;
  
  var kbSize = rowSizes.reduce(function (kb, bytes) { return (kb + Math.ceil(bytes * 1000 / 1024) / 1000); }, 0) / samples;
  var totalSize = Math.ceil(kbSize * nRows * 1000 / 1024) / 1000;
  kbSize = Math.round(kbSize * 1000) / 1000;
  return {kbSize: kbSize, totalSize: totalSize, nRows: nRows, samples: samples};
}
/**
 * function doBackupTable_      Ensure that a copy of the database exists prior to performing some
 *                              major update, such as attempting to remove all non-unique rows.
 */
function doBackupTable_(){
  // TODO: save 30 days worth of tables (or at least more than 1).
  var userBackup = 'MHCC_MostRecentCrownBackupID', scriptBackup = 'backupTableID';
  var oldUsersBackupID = PropertiesService.getUserProperties().getProperty(userBackup) || '';
  var oldGlobalBackupID = PropertiesService.getScriptProperties().getProperty(scriptBackup) || '';
  try
  {
    var newBackupTable = FusionTables.Table.copy(ftid);
    var now = new Date();
    var backupName = 'MHCC_CrownHistory_AsOf_' + [now.getUTCFullYear(), 1-0 + now.getUTCMonth(), now.getUTCDate(),
                                                  now.getUTCHours(), now.getUTCMinutes() ].join('-');
    newBackupTable.name = backupName;
    FusionTables.Table.update(newBackupTable, newBackupTable.tableId);
    // Store the most recent backup.
    PropertiesService.getScriptProperties().setProperty(scriptBackup, newBackupTable.tableId);
    PropertiesService.getUserProperties().setProperty(userBackup, newBackupTable.tableId);
    // Delete this user's old backup, if it exists.
    if ( oldUsersBackupID.length > 0 )
      FusionTables.Table.remove(oldUsersBackupID);
    
    return true;
  }
  catch (e)
  {
    console.error(e);
    return false;
  }
  return false;
}
/**
 * function getMostRecentRecord_  Called if UpdateDatabase has no stored information about a member,
 *                                returning their most recent crown database record.
 * @param {String} memUID         The UID of the member who needs a record.
 * @return {Array}                The most recent update for the specified member, or [].
 */
function getMostRecentRecord_(memUID)
{
  if (String(memUID).length === 0)
    throw new Error('No input UID given');
  if (String(memUID).indexOf(",") > -1)
    throw new Error('Too many input UIDs');
  var recentSql = "SELECT * FROM " + ftid + " WHERE UID = " + memUID + " ORDER BY LastTouched DESC LIMIT 1";
  var resp = FusionTables.Query.sqlGet(recentSql);

  if (typeof resp.rows == 'undefined' || resp.rows.length == 0)
    return [];
  else
    return resp.rows[0];
}
/**
 * function retrieveWholeRecords_    Queries for the specified ROWIDs, at most once per 0.5 sec.
 * @param {String[]} rowidArray      A 1D array of String rowids to retrieve (can be very large).
 * @param {String} tblID             The FusionTable which holds the desired records.
 * @return {Array}                   A 2D array of the specified records, or [].
 */
function retrieveWholeRecords_(rowidArray, tblID)
{
  if (rowidArray.length === 0)
    return [];
  else if (typeof rowidArray[0] !== 'string')
    throw new TypeError('Expected ROWIDs of type String but received type ' + typeof rowidArray[0]);

  if (typeof tblID !== 'string')
    throw new TypeError('Expected table id of type String but received type ' + typeof tblID);

  var nReturned = 0, nRowIds = rowidArray.length, records = [];
  do {
    var sql = '';
    var sqlRowIDs = [], batchStartTime = new Date();
    // Construct ROWID query sql from the list of unique ROWIDs.
    do {
      sqlRowIDs.push(rowidArray.pop())
      sql = "SELECT * FROM " + tblID + " WHERE ROWID IN (" + sqlRowIDs.join(",") + ")";
    } while ((sql.length <= 8050) && (rowidArray.length > 0))
    
    try
    {
      var batchResult = FusionTables.Query.sqlGet(sql);
      nReturned += batchResult.rows.length * 1;
      records = [].concat(records, batchResult.rows);
    }
    catch (e)
    {
      e.message = "Error while retrieving records by ROWID: " + e.message;
      console.error({message: e.message, response: batchResult, numGathered: records.length});
      throw e;
    }
    var elapsedMillis = new Date() - batchStartTime;
    if (elapsedMillis < 500)
      Utilities.sleep(502 - elapsedMillis);
  } while (rowidArray.length > 0)
  
  if (nReturned === nRowIds)
    return records;
  else
    throw new Error("Got different number of rows (" + nReturned + ") than desired (" + nRowIds + ")");
}
/**
 * function getTotalRowCount_  Gets the total number of rows in the supplied FusionTable.
 * @param {String} tblID       The table id
 * @return {Long}              The number of rows in the table
 */
function getTotalRowCount_(tblID)
{
  var sqlTotal = 'select COUNT(ROWID) from ' + tblID;
  try
  {
    var response = FusionTables.Query.sqlGet(sqlTotal);
    return response.rows[0][0];
  }
  catch (e)
  {
    console.error({message: e.message, sql: sqlTotal, data: response});
    throw e;
  }
}
/**
 * function doReplace_      Replaces the contents of the specified FusionTable with the input array
 *                          after sorting on its first element (i.e. alphabetical by Member name).
 *                          If the new records are too large (~10 MB), this call will fail.
 * @param {String} tblID    The table whose contents will be replaced.
 * @param {Array[]} records The new contents of the specified table.
 */
function doReplace_(tblID, records)
{
  if (typeof tblID != 'string')
    throw new TypeError('Argument tblID was not type String');
  else if (tblID.length != 41)
    throw new Error('Argument tbldID not a FusionTables id');
  if (records.constructor != Array)
    throw new TypeError('Argument records was not type Array');
  else if (records.length == 0)
    throw new Error('Argument records must not be length 0');

  records.sort();
  // Sample a few rows to estimate the size of the upload.
  var uploadSize = 0;
  var n = 0;
  for ( ; n < 5; ++n)
  {
    var row = Math.floor(Math.random() * records.length);
    uploadSize += getByteCount_(records[row].toString()) / (1024 * 1024);
  }
  uploadSize = Math.ceil((uploadSize / n) * records.length * 100) / 100;
  console.info("New data is " + uploadSize + " MB (rounded up)");
  if ( uploadSize >= 250 )
    throw new Error("Upload size (" + uploadSize + " MB) is too large");
    
  var cUpload = Utilities.newBlob(array2CSV_(records), 'application/octet-stream');
  try
  {
    FusionTables.Table.replaceRows(tblID, cUpload);
  }
  // Try again if FusionTables didn't respond to the request.
  catch (e)
  { 
    if (e.message.toLowerCase() == "empty response")
      FusionTables.Table.replaceRows(tblID, cUpload);
    else
      throw e; 
  }
}
