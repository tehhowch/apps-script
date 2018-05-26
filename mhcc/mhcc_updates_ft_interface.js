/**
* Google FusionTables to serve as the backend
* Structural changes: since updating rows is a 1-by-1 operation (yes, really >_> ), we
* will have multiple entries for each person. To prevent excessive table growth, we will store only
* 30 snapshots per user, removing any duplicates which occur when the script updates more often than
* the person's data is refreshed.
* Bonus: this means we can serve new data such as most crowns in the last 30 days.
*/
// Table of the user names and their associated UID.
var utbl = '1O4lBLsuvfEzmtySkJ-n-GjOSD_Ab2GsVgNIX8uZ-';
// Table of the user's crown data. Unique on LastTouched.'
var ftid = '1hGdCWlLjBbbd-WW1y32I2e-fFqudDgE3ev-skAff';
// Table of the user's rank data.
var rankTableId = '1Mt7E_3qWRkpGGUZ-pbxkdFYCdhNbyj1oWnEx5haL';

/**
 * function getUserBatch_       Return up to @limit members, beginning with the index @start.
 * @param  {Integer} start      First retrieved index (relative to an alphabetical sort on members).
 * @param  {Integer} limit      The maximum number of pairs to return.
 * @return {String[][]}         [Name, UID]
 */
function getUserBatch_(start, limit)
{
  var sql = "SELECT Member, UID FROM " + utbl + " ORDER BY Member ASC OFFSET " + start + " LIMIT " + limit;
  return FusionTables.Query.sqlGet(sql).rows;
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
  members.forEach(function (pair) { valids[pair[1]] = pair[0]; });
  
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
    } while (mostRecentRecordTimes.rows.length > 0 && (baseSQL + lastTouched.join(",") + tail).length < 8000);
    
    // Execute the query.
    try { batchResult = FusionTables.Query.sqlGet(baseSQL + lastTouched.join(",") + tail); }
    catch (e)
    {
      console.error({message: "Error while fetching whole rows", sql: baseSQL + lastTouched.join(",") + tail, error: e});
      return [];
    }
    Array.prototype.push.apply(snapshots, batchResult.rows);
    
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
 * function ftBatchWrite_     Convert the data array into a CSV blob and upload to FusionTables.
 * @param  {Array[]} newData  The 2D array of data that will be written to the database.
 * @param  {String}  tableId  The table to which the batch data should be written.
 * @param  {Boolean} strict   If the number of columns must match the table schema (default true).
 * @return {Integer}          Returns the number of rows that were added to the database.
 */
function ftBatchWrite_(newData, tableId, strict)
{
  const options = { isStrict: (strict !== false) };
  if (!tableId)
    tableId = ftid;

  var dataAsCSV = array2CSV_(newData);
  try { var dataAsBlob = Utilities.newBlob(dataAsCSV, 'application/octet-stream'); }
  catch (e)
  {
    e.message = "Unable to convert array into CSV format: " + e.message;
    console.error({message: e.message, input: newData, csv: dataAsCSV, blob: dataAsBlob});
    throw e;
  }

  try { return FusionTables.Table.importRows(tableId, dataAsBlob, options).numRowsReceived * 1; }
  catch (e)
  {
    e.message = "Unable to upload rows: " + e.message;
    if (tableId == ftid && options.isStrict === true)
    {
      var badRows = newData.filter(function (record) { return record.length !== crownDBnumColumns; });
      if (badRows.length)
        console.warn({ message: badRows.length + ' rows with incorrect column count out of ' + newData.length, badRows: badRows });
    }
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
  if (str.indexOf(',') !== -1 || str.indexOf("\n") !== -1 || str.indexOf('"') !== -1)
    return ('"' + str.replace(/"/g,'""') + '"');
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
  var newUIDs = newUserBatch.map(function (value) { return value[1]; });
  var diffIndex = newUIDs.indexOf(origUID);
  if (diffIndex > -1)
    newLastRan = origLastRan + (diffIndex - differential);
  else
  {
    // LastRan was pointing at one of the deleted members.
    console.log("Exactly removed the lastRan member. Not changing lastRan, even if it causes issues (which it shouldn't).");
    if (diffMembers.length === 1)
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
 * @return {void}
 */
function getDbSize()
{
  var sizeData = getDbSizeData_(getTotalRowCount_(ftid));
  var sizeStr = 'The crown database has ' + sizeData['nRows'] + ' entries, each consuming ~';
  sizeStr += sizeData['kbSize'] + ' kB of space, based on ';
  sizeStr += sizeData['samples'] + ' sampled rows.<br>The total database size is ~';
  sizeStr += sizeData['totalSize'] + ' mB.<br>The maximum size allowed is 250 MB.';
  SpreadsheetApp.getUi().showModalDialog(HtmlService.createHtmlOutput(sizeStr), "Database Size");
}
/**
 * Return an object detailing the average size of a row (in KB), the total size (in MB), and the number of rows.
 * 
 * @return {{kbSize:Number, totalSize:Number, nRows:Number, samples: Number}}
 */
function getDbSizeData_(nRows, db)
{
  var toGet = [], rowSizes = [], samples = 10;
  for (var n = 0; n < samples; ++n)
    toGet.push(Math.floor(nRows * Math.random()));

  // If not given a database, query the crown FusionTable.
  if (!db || !db.length)
  {
    var base = "SELECT * FROM " + ftid + " OFFSET ";
    toGet.forEach(function (row)
    {
      var resp = FusionTables.Query.sqlGet(base + row + " LIMIT 1");
      if (!resp.rows || !resp.rows.length || !resp.rows[0].length)
      {
        rowSizes.push(0);
        if(--samples === 0) return;
      }
      else
        rowSizes.push(getByteCount_(resp.rows[0].toString()));
    });
  }
  else
    toGet.forEach(function (row) { rowSizes.push(getByteCount_(db[row].toString())); });

  var kbSize = rowSizes.reduce(function (kb, bytes) { return (kb + Math.ceil(bytes * 1000 / 1024) / 1000); }, 0) / samples;
  return {
    kbSize: Math.round(kbSize * 1000) / 1000,
    totalSize: Math.ceil(kbSize * nRows * 1000 / 1024) / 1000,
    nRows: nRows, samples: samples
  };
}
/**
 * function doBackupTable_      Ensure that a copy of the database exists prior to performing some
 *                              major update, such as attempting to remove all non-unique rows.
 *                              Returns the table id of the copy, if one was made.
 * @param {String} tableId      The FusionTable to operate on (default: MHCC Crowns).
 * @param {Boolean} deleteEarliest  If true (default), the previous backup will be deleted after a successful backup.
 * @return {String}
 */
function doBackupTable_(tableId, deleteEarliest)
{
  /**
   * Access or initialize a backup object for the given user. The backup objects store the tableIds of copies for
   * a given input table, keyed to the time at which they were created.
   * 
   * @param {any} store  A PropertiesService object (UserProperties or ScriptProperties)
   * @param {String} key The key which is used to access the object in the store.
   * @return {{tableId: {String: String}}}
   */
  function _getBackupObject_(store, key)
  {
    var value = store.getProperty(key);
    if (value)
      return JSON.parse(value);
    var newObject = {};
    newObject[tableId] = {};
    return newObject;
  }

  if (!tableId) tableId = ftid;
  if (deleteEarliest !== false) deleteEarliest = true;

  const uStore = PropertiesService.getUserProperties(),
    store = PropertiesService.getScriptProperties(),
    userKey = "MHCC_MostCrownBackupIDs",
    scriptKey = "backupTableIDs";

  // Get the user and script backup objects, which will have at least the input tableId as a property.
  var userBackup = _getBackupObject_(uStore, userKey);
  var scriptBackup = _getBackupObject_(store, scriptKey);

  const copyOptions = { "copyPresentation": true, "fields": "tableId,name,description" };
  // We store the time a backup was made (ms epoch) as the key, and the tableId as the value.
  const now = new Date();
  const newSuffix = "_AsOf_" + [now.getUTCFullYear(), 1 + now.getUTCMonth(), now.getUTCDate(),
  now.getUTCHours(), now.getUTCMinutes()].join("-");

  // Get the minimal resource of the copied table.
  try { var backup = FusionTables.Table.copy(tableId, copyOptions); }
  catch (e) { console.error(e); return; }

  // Rename it and set the new description.
  backup.name = backup.name.slice(backup.name.indexOf("Copy of ") + "Copy of ".length).split(" ").join("_") + newSuffix;
  backup.description = "Automatic backup of table with id= '" + tableId + "'.";
  try { backup = FusionTables.Table.patch(backup, backup.tableId); }
  catch (e) { console.warn(e); }

  // Remove the oldest backup, if desired (and possible).
  try
  {
    if (deleteEarliest && Object.keys(userBackup[tableId]).length > 1)
    {
      var earliest = Object.keys(userBackup[tableId]).reduce(function (dt, next) { return Math.min(dt * 1, next * 1); });
      const key = String(earliest);
      const idToDelete = userBackup[tableId][key];
      if (idToDelete != tableId)
      {
        FusionTables.Table.remove(idToDelete);
        delete userBackup[tableId][key];
        if (scriptBackup[tableId][key])
          delete scriptBackup[tableId][key];
      }
    }
  }
  catch (e) { console.warn(e); }

  // Store the data about the backup.
  const newBackupKey = String(now.getTime());
  userBackup[tableId][newBackupKey] = backup.tableId;
  scriptBackup[tableId][newBackupKey] = backup.tableId;
  uStore.setProperty(userKey, JSON.stringify(userBackup));
  store.setProperty(scriptKey, JSON.stringify(scriptBackup));
  return backup.tableId;
}

/**
 * function getMostRecentRecord_  Called if UpdateDatabase has no stored information about a member,
 *                                returning their most recent crown database record.
 * @param {String} memUID         The UID of the member who needs a record.
 * @return {Array[]}                The most recent update for the specified member, or [].
 */
function getMostRecentRecord_(memUID)
{
  if (String(memUID).length === 0)
    throw new Error('No input UID given');
  if (String(memUID).indexOf(",") > -1)
    throw new Error('Too many input UIDs');
  var recentSql = "SELECT * FROM " + ftid + " WHERE UID = " + memUID + " ORDER BY LastTouched DESC LIMIT 1";
  var resp = FusionTables.Query.sqlGet(recentSql);

  if (!resp || !resp.rows || !resp.rows.length)
    return [];
  else
    return resp.rows[0];
}
/**
 * function retrieveWholeRecords_    Queries for the specified ROWIDs, at most once per 0.5 sec.
 * @param {String[]} rowidArray      A 1D array of String rowids to retrieve (can be very large).
 * @param {String} tblID             The FusionTable which holds the desired records.
 * @return {Array[]}                   A 2D array of the specified records, or [].
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
  do {
    var sql = '';
    var sqlRowIDs = [], batchStartTime = new Date();
    // Construct ROWID query sql from the list of unique ROWIDs.
    do {
      sqlRowIDs.push(rowidArray.pop())
      sql = "SELECT * FROM " + tblID + " WHERE ROWID IN (" + sqlRowIDs.join(",") + ")";
    } while ((sql.length <= 8000) && (rowidArray.length > 0))
    
    try
    {
      var batchResult = FusionTables.Query.sqlGet(sql);
      nReturned += batchResult.rows.length * 1;
      Array.prototype.push.apply(records, batchResult.rows);
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
  
  if (nReturned !== nRowIds)
    throw new Error("Got different number of rows (" + nReturned + ") than desired (" + nRowIds + ")");
  return records;
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
    return response.rows[0][0] * 1;
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
  else if (!records.length)
    throw new Error('Argument records must not be length 0');

  records.sort();
  // Sample a few rows to estimate the size of the upload.
  var uploadSize = getDbSizeData_(records.length, records).totalSize;
  console.info("New data is " + uploadSize + " MB (rounded up)");
  if ( uploadSize >= 250 )
    throw new Error("Upload size (" + uploadSize + " MB) is too large");
    
  var cUpload = Utilities.newBlob(array2CSV_(records), 'application/octet-stream');
  try { FusionTables.Table.replaceRows(tblID, cUpload); }
  // Try again if FusionTables didn't respond to the request.
  catch (e)
  { 
    if (e.message.toLowerCase() == "empty response")
      FusionTables.Table.replaceRows(tblID, cUpload);
    else
      throw e; 
  }
}
