/**
* Google FusionTables to serve as the backend
* Structural changes: since updating rows is a 1-by-1 operation (yes, really >_> ), we
* will have multiple entries for each person. To prevent excessive table growth, we will store only
* 30 snapshots per user, removing any duplicates which occur when the script updates more often than
* the person's data is refreshed.
* Bonus: this means we can serve new data such as most crowns in the last 30 days.
*/

/**
 * Returns a 2D array of the members' names and identifiers, from `start` until `limit`, or EOF.
 * @param  {number} start First retrieved index (relative to an alphabetical sort on members).
 * @param  {number} limit The maximum number of pairs to return.
 * @return {Array <string>[]} [Name, UID]
 */
function getUserBatch_(start, limit)
{
  var sql = "SELECT Member, UID FROM " + utbl + " ORDER BY Member ASC OFFSET " + start + " LIMIT " + limit;
  return FusionTables.Query.sqlGet(sql).rows;
}

/**
 * Returns the record associated with the most recently touched snapshot for each current member.
 * Returns a single record per member, so long as every record has a different LastTouched value.
 * First finds each UID's maximum LastTouched value, then gets the associated row (after verifying
 * the member is current).
 * A query with "UID IN ( ... ) AND LastTouched IN ( ... )" would relax but not eliminate the
 * "unique LastTouched" requirement.
 * @return {Array[]}         N-by-crownDBnumColumns Array for UpdateScoreboard to parse.
 */
function getLatestRows_()
{
  // Query for the most recent records of all persons with data.
  const ltSQL = "SELECT UID, MAXIMUM(LastTouched) FROM " + ftid + " GROUP BY UID";
  var mostRecentRecordTimes = FusionTables.Query.sqlGet(ltSQL);
  if (!mostRecentRecordTimes || !mostRecentRecordTimes.rows || !mostRecentRecordTimes.rows.length)
  {
    console.error({ "message": "Invalid response from FusionTable API.", "data": mostRecentRecordTimes });
    return [];
  }

  // Query for those members that are still current.
  const members = getUserBatch_(0, 100000);
  if (!members || !members.length || !members[0].length)
  {
    console.error({ "message": "No members returned from call to getUserBatch", "data": members });
    return [];
  }

  // Construct an associative map object for checking the uid from records against current members.
  /** @type {Object <string, string>} */
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
      console.error({ "message": "Error while fetching whole rows", "sql": baseSQL + lastTouched.join(",") + tail, "error": e });
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
    for (var rc = 0, len = snapshots.length; rc < len; ++rc)
      delete valids[snapshots[rc][1]];
    console.log({ "message": "Some members lack scoreboard records", "data": valids });
  }
  return snapshots;
}

/**
 * function ftBatchWrite_     Convert the data array into a CSV blob and upload to FusionTables.
 * @param  {Array[]} newData  The 2D array of data that will be written to the database.
 * @param  {string}  tableId  The table to which the batch data should be written.
 * @param  {boolean} [strict=true] If the number of columns must match the table schema (default true).
 * @return {number}          Returns the number of rows that were added to the database.
 */
function ftBatchWrite_(newData, tableId, strict)
{
  if (!tableId) return;
  const options = { isStrict: strict !== false };

  var dataAsCSV = array2CSV_(newData);
  try { var dataAsBlob = Utilities.newBlob(dataAsCSV, "application/octet-stream"); }
  catch (e)
  {
    e.message = "Unable to convert array into CSV format: " + e.message;
    console.error({ "message": e.message, "input": newData, "csv": dataAsCSV, "blob": dataAsBlob });
    throw e;
  }

  try { return FusionTables.Table.importRows(tableId, dataAsBlob, options).numRowsReceived * 1; }
  catch (e)
  {
    e.message = "Unable to upload rows: " + e.message;
    if (tableId === ftid && options.isStrict === true)
    {
      var badRows = newData.filter(function (record) { return record.length !== crownDBnumColumns; });
      if (badRows.length)
        console.warn({ "message": badRows.length + ' rows with incorrect column count out of ' + newData.length, "badRows": badRows });
    }
    throw e;
  }
}

/**
 * function getByteCount_   Computes the size in bytes of the passed string
 * @param  {string} str     The string to analyze
 * @return {number}        The bytesize of the string, in bytes
 */
function getByteCount_(str)
{
  return Utilities.base64EncodeWebSafe(str).split(/%(?:u[0-9A-F]{2})?[0-9A-F]{2}|./).length - 1;
}

/**
 * Determines the size of the database by extrapolating from the size of a random row,
 * and reports the result via spreadsheet "toast".
 */
function getDbSize()
{
  const sizeData = getDbSizeData_(getTotalRowCount_(ftid));
  var sizeStr = 'The crown database has ' + sizeData.nRows + ' entries, each consuming ~';
  sizeStr += sizeData.kbSize + ' kB of space, based on ';
  sizeStr += sizeData.samples + ' sampled rows.<br>The total database size is ~';
  sizeStr += sizeData.totalSize + ' mB.<br>The maximum size allowed is 250 MB.';
  SpreadsheetApp.getUi().showModalDialog(HtmlService.createHtmlOutput(sizeStr), "Database Size");
}
/**
 * Return an object detailing the average size of a row (in KB), the total size (in MB), and the number of rows.
 * @param {number} nRows The total number of rows that exist in the database which is to be sampled from.
 * @param {Array[]} [db] The database to analyze. If omitted, will be the MHCC Crown DB.
 * @return {{kbSize: number, totalSize: number, nRows: number, samples: number}} Size information about the given database
 */
function getDbSizeData_(nRows, db)
{
  var toGet = [], rowSizes = [], samples = 10;
  for (var n = 0; n < samples; ++n)
    toGet.push(Math.floor(nRows * Math.random()));

  // If not given a database, query the crown FusionTable.
  if (!db || !db.length)
  {
    const base = "SELECT * FROM " + ftid + " OFFSET ";
    toGet.forEach(function (row) {
      var resp = FusionTables.Query.sqlGet(base + row + " LIMIT 1");
      if (!resp.rows || !resp.rows.length || !resp.rows[0].length)
      {
        rowSizes.push(0);
        if (--samples === 0) return;
      }
      else
        rowSizes.push(getByteCount_(resp.rows[0].toString()));
    });
  }
  else
    toGet.forEach(function (row) { rowSizes.push(getByteCount_(db[row].toString())); });

  var kbSize = rowSizes.reduce(function (kb, bytes) { return kb + Math.ceil(bytes * 1000 / 1024) / 1000; }, 0) / samples;
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
 * @param {string} [tableId]    The FusionTable to operate on (default: MHCC Crowns).
 * @param {boolean} [deleteEarliest=true]  If true, the previous backup will be deleted after a successful backup.
 * @return {string} The FusionTable ID for the newly-created FusionTable.
 */
function doBackupTable_(tableId, deleteEarliest)
{
  /**
   * Access or initialize a backup object for the given user. The backup objects store the tableIds of copies for
   * a given input table, keyed to the time at which they were created.
   *
   * @param {any} store  A PropertiesService object (UserProperties or ScriptProperties)
   * @param {string} key The key which is used to access the object in the store.
   * @return {Object <string, Object <string, string>>} An object with keys of FusionTable IDs, yielding time-id associative objects.
   */
  function _getBackupObject_(store, key)
  {
    var value = store.getProperty(key);
    // If the value was there, it may or may not be stringified JSON.
    if (value && value[0] === "{" && value[1] === "\"")
    {
      var existing = JSON.parse(value);
      // Ensure the backup object has the requested table as a property.
      if (!existing[tableId])
        existing[tableId] = {};
      return existing;
    }
    var newObject = {};
    newObject[tableId] = {};
    return newObject;
  }

  if (!tableId) tableId = ftid;
  deleteEarliest = deleteEarliest !== false;

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
      if (idToDelete !== tableId)
      {
        FusionTables.Table.remove(idToDelete);
        delete userBackup[tableId][key];
        if (scriptBackup[tableId][key])
          delete scriptBackup[tableId][key];
      }
    }
  }
  catch (e) { console.warn({ "message": e.message, "error": e, "tableId": tableId, "user": userBackup, "script": scriptBackup, "earliest": earliest, "idToDelete": idToDelete }); }

  // Store the data about the backup.
  const newBackupKey = String(now.getTime());
  userBackup[tableId][newBackupKey] = backup.tableId;
  scriptBackup[tableId][newBackupKey] = backup.tableId;
  uStore.setProperty(userKey, JSON.stringify(userBackup));
  store.setProperty(scriptKey, JSON.stringify(scriptBackup));
  return backup.tableId;
}

/**
 * function getTotalRowCount_  Gets the total number of rows in the supplied FusionTable.
 * @param {string} tblID       The table id
 * @return {number}            The number of rows in the table
 */
function getTotalRowCount_(tblID)
{
  const sqlTotal = 'select COUNT(ROWID) from ' + tblID;
  try
  {
    const response = FusionTables.Query.sqlGet(sqlTotal);
    return response.rows[0][0] * 1;
  }
  catch (e)
  {
    console.error({ "message": e.message, "sql": sqlTotal, "data": response });
    throw e;
  }
}
