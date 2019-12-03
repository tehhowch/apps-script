/**
 *  This spreadsheet uses Google Apps Script, Spreadsheets, and the "experimental" FusionTables API
 *  to maintain a record of all MHCC members that opted into crown tracking. Via UpdateDatabase()
 *  and HornTracker.com's "MostMice", the arduous task of tracking all members' crowns is be reduced
 *  to simply clicking a profile.
 *  Via a time-basis "Trigger", current crown data is fetched from HornTracker in sets of up to 127
 *  members (higher batch sizes overload the maximum URL length). This fetch is performed until all
 *  member data has been fetched, or a specified execution duration is met, in order to avoid the
 *  "Maximum Execution Time Exceeded" Google Apps Script error (at 300 seconds).
 *  The time-basis Trigger frequency should not be set too high, as unless member data is constantly
 *  updated, this results in sending essentially duplicated data to the FusionTable. A member rank's
 *  Rank is only updated at the end of an update cycle, when the Scoreboard is written.
 *
 *
 * Tracking Progress of Updates
 *
 *  SheetDb holds the same data as that used to construct the currently-visibile Scoreboard page,
 *  but is sorted alphabetically. To observe the current cycle status, view the 'lastRan' parameter
 *  via File -> Project Properties -> Project Properties.
 *
 *
 * Email Error Notifications
 *
 *  It is normal to receive email notifications of script errors. These originate largely due to
 *  the availability (or not) of HornTracker resources. However, some error messages are generated
 *  by this application to indicate if it is having issues or finding unexpected data. If the same
 *  non-HornTracker error repeatedly occurs and Scoreboard updates seem to have ceased, it is best
 *  to contact the person who set it up.
 *
 *
 * Forcing a Scoreboard Update
 *
 *  If you must update the scoreboard immediately, manually run the UpdateScoreboard function via
 *  the "Run" -> "UpdateScoreboard" menus located above. Alternately, the 'Administration' tab on
 *  the spreadsheet can call the function. Doing so will commit the current state of the member
 *  list to the scoreboard sheet.
 *
 *
 * Forcing a Restart (Getting the database to restart crown updates instead of continuing)
 *
 *  Click "File" -> "Project Properties" -> "Project Properties", and you should now see a table of
 *  fields and values. Click the current value for lastRan (e.g. 2364) and replace it with 0.
 *  Click "Save" or press "Enter" to commit your change.
 */
// @OnlyCurrentDoc
var crownDBnumColumns = 10;
// Change this number when additional rows containing special titles (like "Super Secret Squirrel") get added.
// This only needs to change when rows are added - if values of existing rows are changed, all is well. :)
var numCustomTitles = 16;
/**
 * function onOpen()      Sets up the admin's menu from the spreadsheet interface.
 * @param {Object <string, any>} e The "spreadsheet open" event object, provided by Google.
 */
function onOpen(e)
{
  e.source.addMenu('Administration', [
    { name: "Manage Members", functionName: "getSidebar" },
    { name: "Perform Crown Update", functionName: "UpdateDatabase" },
    { name: "Check Database Size", functionName: "getDbSize" }]);
}
/**
 * function getMyDb_          Returns the entire data contents of the worksheet SheetDb to the calling
 *                            code as a rectangular array. Does not supply header information.
 * @param  {GoogleAppsScript.Spreadsheet.Spreadsheet} wb      The workbook containing the worksheet named SheetDb
 * @param  {number|Array <{column: number, ascending: boolean}>} [sortObj]   An integer column number or an Object[][] of sort objects
 * @return {Array[]}          An M by N rectangular array which can be used with Range.setValues() methods
 */
function getMyDb_(wb, sortObj)
{
  const SS = wb.getSheetByName('SheetDb');
  try
  {
    var r = SS.getRange(2, 1, SS.getLastRow() - 1, SS.getLastColumn());
    if (sortObj)
      r.sort(sortObj);
    return r.getValues();
  }
  catch (e) { console.warn({ "message": e.message, "error": e, "dbRange": r, "sorter": sortObj }); return []; }
}
/**
 * function saveMyDb_         Uses the Range.setValues() method to write a rectangular array to the worksheet
 *
 * @param  {GoogleAppsScript.Spreadsheet.Spreadsheet} wb      The workbook containing the worksheet named SheetDb
 * @param  {Array[]} db         The rectangular array of data to write to SheetDb
 * @return {boolean}          Returns true if the data was written successfully, and false if a database lock was not acquired.
 */
function saveMyDb_(wb, db)
{
  function _getDbSheet_()
  {
    const sheet = wb.getSheetByName("SheetDb");
    return sheet ? sheet : wb.insertSheet("SheetDb");
  }
  if (!wb || !db || !db.length || !db[0].length)
    return false;
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(10000))
    return false;

  // Have a lock on the db, now save.
  const SS = _getDbSheet_(), oldDataRows = SS.getLastRow() - 1;
  // If the new db is smaller, the old db must be cleared first.
  if (db.length < oldDataRows || db[0].length < SS.getLastColumn())
  {
    SS.getRange(2, 1, oldDataRows, SS.getLastColumn()).clearContent();
    SpreadsheetApp.flush();
  }
  SS.getRange(2, 1, db.length, db[0].length).setValues(db).sort(1);
  SpreadsheetApp.flush();
  lock.releaseLock();
  return true;
}
/**
 * function UpdateDatabase  This is the main function which governs the process of updating crowns
 *                          and writing Scoreboard data. Batch sizes of 127 are the maximum, due to
 *                          the maximum length of a URL. The maximum execution time is 300 seconds,
 *                          after which Google's servers will kill the script.
 */
function UpdateDatabase()
{
  const batchSize = 127, startTime = new Date(), maxRuntime = 150;
  const wb = SpreadsheetApp.getActive(), store = PropertiesService.getScriptProperties();
  const props = store.getProperties();
  // Database records count (may be larger than the written db on SheetDb)
  var numMembers = bq_getTableRowCount_('Core', 'Members');
  // The last queried member number indicates where to begin new update queries.
  var lastRan = props.lastRan * 1 || 0;

  // Read in the MHCC tiers as an array.
  // (If the MHCC tiers are moved, this getRange target MUST be updated!)
  try { const aRankTitle = wb.getSheetByName('Members').getRange(3, 8, numCustomTitles, 3).getValues(); }
  catch (e) { throw new Error("'Members' sheet was renamed or deleted - cannot locate crown-title relationship data."); }

  // After each Scoreboard operation, a backup of the FusionTable databases should be attempted.
  if (lastRan >= numMembers)
  {
    UpdateScoreboard();
    store.setProperty("lastRan", 0);
    doBackupTable_(ftid);
    doBackupTable_(rankTableId);
    if (Math.random() < 0.4)
      doWebhookPost_(props["mhDiscord"]);
  }
  else
  {
    // Guard against concurrent execution of code which modifies LastRan (and thus, member data).
    const lock = LockService.getScriptLock();
    if (!lock.tryLock(30000))
      return;

    // allMembers is an array of [Name, UID]
    const allMembers = bq_getMemberBatch_(),
      newMemberData = {};
    var updateLastRan = false;

    // Query the datasources for new crown data, using sets of batchSize members.
    do {
      var batch = allMembers.slice(lastRan, lastRan + batchSize);
      var newCrownData = _getNewCrownData_(batch);
      if (newCrownData === null)
        throw new Error("Crown Data Acquisition failed due to invalid input member data");
      updateLastRan |= newCrownData.completed;

      // Collect the partial records for later record construction. Only members for whom
      // new crowns were seen, or without new records in the past week, will be updated.
      for (var id in newCrownData.partials)
        newMemberData[id] = newCrownData.partials[id];

      lastRan += batchSize;
    } while ((new Date() - startTime) < maxRuntime * 1000 && lastRan < numMembers);

    // Convert partial records into insertable MHCC records. This involves querying the
    // FusionTable database to determine each member's last crown counts.
    const newRecords = [];
    if (Object.keys(newMemberData).length)
    {
      const existing = _fetchExistingRecords_(newMemberData);
      if (!existing || !existing.records || !existing.indices)
        throw new Error("Failed to obtain stored records");
      _addRecords_(newMemberData, existing.records, existing.indices, newRecords);
      if (newRecords.length > 0)
        bq_addCrownSnapshots_(newRecords);
    }

    if (updateLastRan)
      store.setProperty('lastRan', lastRan.toString());

    console.log("Through %d members, %s sec. elapsed. Uploaded %d new crown records.", lastRan, (new Date() - startTime) / 1000, newRecords.length);
    lock.releaseLock();
  }

  /**
   * @typedef {{bronze: number, silver: number, gold: number, seen: number}} CrownSnapshot
   *
   * @typedef {Object} MemberData A member-specific object with information about this member's stored Crown DB data.
   * @property {string} displayName The name of this member
   * @property {number} lastSeen The most recent "LastSeen" value for this member's stored records.
   * @property {number} lastCrown The most recent "LastCrown" value for this member's stored records.
   * @property {number} lastTouched The most recent "LastTouched" value, indicating the last time this member's stored data was updated.
   */
  /**
   * @typedef {Object} MemberUpdate A combination of new data from datasources, and stored data from the Crown DB.
   * @property {MemberData} storedInfo A summary of stored information in the MHCC Crown DB for this member.
   * @property {CrownSnapshot[]} collected All crown snapshots read from the datasources for this member that could be added.
   * @property {CrownSnapshot[]} toAdd Crown snapshots read from the datasources that are unique enough to store.
   *
   * @typedef {Object} MemberUpload
   * @property {MemberData} storedInfo A summary of stored information in the MHCC Crown DB for this member.
   * @property {CrownSnapshot[]} newData Crown snapshots that should be formed into records and uploaded.
   */
  /**
   * Nested function which handles obtaining the previously known data for the given partial records.
   * Returns an object with the full record referenced for a given member in the partial records, indexed by UID.
   *
   * @param {Object <string, MemberUpload>} partials UID-indexed object with the timestamps needed to query for the previous stored record.
   * @return {{records: Object <string, (string|number)[]>, indices: Object <string, number>}} Object containing UID-indexed full records, and the indices needed to order values in a new record.
   */
  function _fetchExistingRecords_(partials)
  {
    if (!partials || !Object.keys(partials).length)
      return null;

    const resp = bq_getLatestRows_('Core', 'Crowns');
    const records = resp.rows;
    // TODO: use columns to set indices appropriately
    // Collect the full records into a UID-indexed Object (for rapid accessing).
    const headers = resp.columns.map(function (column) { return column.toLowerCase(); });
    const labels = ["uid", "lastseen", "lastcrown", "lasttouched", "bronze", "silver", "gold", "mhcc", "squirrel"];
    /** @type {Object <string, (string|number)[]} */
    const storedRecords = {};
    const indices = labels.reduce(function (map, colName) {
      map[colName] = headers.indexOf(colName);
      if (map[colName] === -1) throw new Error("Missing column name '" + colName + "'");
      return map;
    }, {});
    records.forEach(function (record) { storedRecords[record[indices.uid]] = record; });

    return { records: storedRecords, indices: indices };
  }
  /**
   * Nested function which constructs new records based on the previously known data and the latest input data.
   *
   * @param {Object <string, MemberUpload>} partials The latest crown snapshots for the members being updated.
   * @param {Object <string, (string|number)[]>} existingRecords  The most recent records for the members being updated.
   * @param {Object <string, number>} indices Mapping between column names and column position in the record.
   * @param {Array} fullRecords Mutable array which holds computed records.
   */
  function _addRecords_(partials, existingRecords, indices, fullRecords)
  {
    /**
     * Initialize a Crown Record based on the supplied data values
     * @param {string} name The member's display name
     * @param {string} id The member's MHCC identifier
     * @param {CrownSnapshot} source The source of crown counts and time values
     * @param {Object <string, number>} indices The positions of specific values within the row
     * @return {(string|number)[]} A sparse array with data extracted from the input source.
     */
    function _makeRawRecord_(name, id, source, indices)
    {
      var r = [name];
      r[indices.uid] = id;
      r[indices.lastseen] = source.seen;
      r[indices.bronze] = source.bronze;
      r[indices.silver] = source.silver;
      r[indices.gold] = source.gold;
      r[indices.mhcc] = (source.silver || 0) * 1 + (source.gold || 0) * 1;
      return r;
    }

    // Now that the desired full records are available, iterate through the
    // partial list and use the stored data to construct the new record.
    for (var uid in partials)
    {
      var dbr = existingRecords[uid] || _makeRawRecord_(partials[uid].storedInfo.displayName, uid, {}, indices);

      // Sort the new data in ascending order, to ensure crown change date consistency.
      if (partials[uid].newData.length > 1)
        partials[uid].newData.sort(function (a, b) { return a.seen - b.seen; });

      partials[uid].newData.forEach(function (newData, i, all) {
        var newRecord = _makeRawRecord_(dbr[0], uid, newData, indices);

        // Check for different crown counts.
        // TODO: Add "MHCCcrown" column to table, and update it only when a new MHCC crown is gained. Use it
        // for rank determination, rather than LastCrown, since a member should not be penalized for gaining
        // a bronze crown. Currently, if they are tied for MHCC Crown counts with a different member, the new
        // crown will be treated as though the other member got to that MHCC Crown total first, even if they
        // did not. Similarly, this is true for silver->gold.
        var last = (i > 0)
          ? { bronze: all[i - 1].bronze, silver: all[i - 1].silver, gold: all[i - 1].gold }
          : { bronze: dbr[indices.bronze] * 1, silver: dbr[indices.silver] * 1, gold: dbr[indices.gold] * 1 };
        // Update the crown change date to the time the member's new data was seen if any crowns have changed.
        if (newData.bronze !== last.bronze || newData.silver !== last.silver || newData.gold !== last.gold)
          newRecord[indices.lastcrown] = newData.seen;
        else
          newRecord[indices.lastcrown] = (i > 0)
            ? fullRecords[fullRecords.length - 1][indices.lastcrown]
            : dbr[indices.lastcrown];

        // Compute a new "Squirrel" rating (the ratings may have changed even if the member's crowns did not).
        for (var k = 0; k < aRankTitle.length; ++k)
          if (newRecord[indices.mhcc] >= aRankTitle[k][0])
          {
            // Crown count meets/exceeds required crowns for this level.
            newRecord[indices.squirrel] = aRankTitle[k][2];
            break;
          }

        // Append the computed record to the records that will be uploaded.
        fullRecords.push(newRecord);
      });
    }

    // Assign a unique lastTouched value to each new record.
    var now = (new Date()).getTime();
    for (var r = 0; r < fullRecords.length; ++r)
      fullRecords[r][indices.lasttouched] = ++now;
  }
  /**
   * Nested function which handles requesting crown info for a given set of members from the various data
   * sources available:
   *    1. HornTracker MostMice
   *    2. Jack's MH Crowns FusionTable
   *
   *  Each unique instance of data is imported, provided
   *    A) It has a different "Last Seen" than the member's existing "Last Seen" data.
   *      or
   *    B) It has been a week since the last time the member had a new record entered.
   *
   *  Returns null only if memberSet has no IDs, or all of the IDs have no associated record data in the MHCC Crowns DB FusionTable.
   *  otherwise, returns a uid-indexed Object with the database's maximum values for seen, touched, and crown data, and at least 1
   *  crown snapshot for which a full record needs to be constructed.
   *
   * @param {Array <string>[]} memberSet  The subset of the global member list for which data should be queried.
   * @return {{partials: Object <string, MemberUpload>, completed: boolean}} null, or an object with feedback about the query result and the minimal set of new crown data to be uploaded.
   */
  function _getNewCrownData_(memberSet)
  {

    /**
     * Check if the record has different stored crowns than all of the other ones being added.
     * @param {CrownSnapshot} record The crown snapshot being considered.
     * @param {CrownSnapshot[]} recordsToAdd Already-accepted crown snapshots.
     * @return {boolean} If the considered record has different crowns than the other records being added.
     */
    function _hasDifferentCrowns_(record, recordsToAdd)
    {
      for (var r = 0; r < recordsToAdd.length; ++r)
      {
        var existing = recordsToAdd[r];
        if (record.gold === existing.gold
          && record.silver === existing.silver
          && record.bronze === existing.bronze)
          return false;
      }
      return true;
    }


    // Assemble the IDs to query for, and create an Object in which to collect the data from each datasource.
    const urlIDs = [];
    /** @type {Object <string, MemberUpdate>} */
    const data = {};
    memberSet.forEach(function (member) {
      if (!member[1]) return;
      urlIDs.push(member[1].toString());
      data[member[1].toString()] = { collected: [], toAdd: [], storedInfo: { displayName: member[0] } };
    });
    if (!urlIDs.length)
    {
      console.warn({ "message": "No valid UIDs in member set", "memberSet": memberSet });
      return null;
    }

    // Obtain the lastSeen and lastTouched timestamps for the users in this batch.
    var uidString = urlIDs.join(",");
    const batchRows = bq_getLatestBatch_('Core', 'Crowns', urlIDs);
    // While unlikely, this entire batch of requested members may have no existing Crown DB records.
    if (batchRows)
      batchRows.forEach(function (memberRow) {
        var uid = memberRow[0];
        data[uid].storedInfo.lastSeen = memberRow[1] * 1;
        data[uid].storedInfo.lastCrown = memberRow[2] * 1;
        data[uid].storedInfo.lastTouched = memberRow[3] * 1;
      });

    // Collect the data from each datasource.
    var datasourceData = [
      QueryMostMice_(uidString),
      QueryJacksData_(uidString)
    ];

    // Compare the collected data with the existing data.
    const now = new Date();
    /** @type {Object <string, MemberUpload>} */
    const output = {};
    for (var uid in data)
    {
      // If this member's records as held by the datasources are different than the stored data, stage them.
      var last_stored = data[uid].storedInfo.lastSeen || 0;
      var elapsed_since_storage = (now - (data[uid].storedInfo.lastTouched || 0)) / (1000 * 86400);
      var insert_anyway = elapsed_since_storage > 7;
      for (var ds = 0; ds < datasourceData.length; ++ds)
      {
        var dsr = datasourceData[ds][uid];
        if (!dsr)
          continue;
        // Is this a new record, relative to what we have stored? Or has it been a
        // long time since we stored a record for this member (new or otherwise)?
        if (dsr.seen > last_stored || insert_anyway)
          data[uid].collected.push(dsr);
      }
      // If this member is currently absent from all datasources (i.e. the one with the data is
      // currently unavailable), skip to the next member.
      if (!data[uid].collected.length)
        continue;

      // If this is a "courtesy" insert, we only want the most recent record. The most recent
      // record is the one which the database has been working with since its insertion, and
      // inserting any other records would distort the collected records.
      // If not, then we want any records which are newer than the stored record.

      // Sort the crown snapshots from most to least recently acquired.
      data[uid].collected.sort(function (a, b) { return b.seen - a.seen; });
      if (insert_anyway)
        data[uid].toAdd.push(data[uid].collected[0]);
      else
      {
        data[uid].collected.forEach(function (record)
        {
          // We know that this record is newer than the stored record (otherwise
          // it would not be 'collected' unless the forced insert was happening).
          // Thus, if it has different crowns compared to what we have already
          // decided to add, include it for addition.
          if (_hasDifferentCrowns_(record, data[uid].toAdd))
            data[uid].toAdd.push(record);
        });
      }

      // No matter why we are inserting data, we want the record with the most
      // recent LastSeen value to come last. This ensures it will be the last
      // record inserted for the member (and receive the largest LastTouched).
      if (data[uid].toAdd.length)
      {
        data[uid].toAdd.sort(function (a, b) { return a.seen - b.seen; });
        output[uid] = {
          storedInfo: data[uid].storedInfo,
          newData: data[uid].toAdd
        };
      }
    }

    return { partials: output, completed: true };
  }
  /** Nested function which handles querying HornTracker's MostMice endpoint and constructing the expected data object.
   * @param {string} uids  A comma-joined string of member identifiers for whom to request catch totals.
   * @return {Object <string, CrownSnapshot>}  A UID-indexed object with the latest known crown counts for the input members.
   */
  function QueryMostMice_(uids)
  {
    /**
     * HornTracker returns the individual catch counts per mouse, so crown counts must be aggregated.
     * @param {Object <string, number>} mice The number of catches for each mouse, indexed by mouse name.
     * @return {{bronze: number, silver: number, gold: number}} The number of bronze, silver, and gold crowns, based on the input per-mouse catch data.
     */
    function _getCrowns_(mice)
    {
      var nB = 0, nS = 0, nG = 0;
      // Compute crowns by summing over all mice.
      for (var m in mice)
      {
        var count = mice[m];
        if (count >= 500)
          ++nG;
        else if (count >= 100)
          ++nS;
        else if (count >= 10)
          ++nB;
      }
      return { bronze: nB, silver: nS, gold: nG };
    }

    const htData = {};
    try { var resp = UrlFetchApp.fetch('http://horntracker.com/backend/mostmice.php?function=hunters&hunters=' + uids); }
    catch (e)
    {
      // If the query to HT failed in an unknown way, throw the new error.
      var msg = e.message.toLowerCase();
      const knownErrors = ["unexpected error: h", "timeout: h", "502 bad gateway"];
      knownErrors.forEach(function (fragment) {
        if (msg.indexOf(fragment) > -1)
          resp = null;
      });
      if (resp !== null)
      {
        e.message = "HT GET - errmsg: " + e.message;
        console.error({ "message": e.message, "uids": uids.split(","), "response": resp });
        throw e;
      }
    }
    // Validate the returned data prior to attempting to coerce it to JSON.
    if (!resp || resp.getResponseCode() !== 200)
    {
      console.log({ "message": "HornTracker query failed in known manner", "response": resp, "targets": uids.split(",") });
      return htData;
    }

    const text = resp.getContentText();
    if (!text.length || text.toLowerCase().indexOf('connect to mysql') !== -1)
    {
      console.log("HornTracker database unavailable");
      return htData;
    }

    try { var mostMiceData = JSON.parse(text).hunters; }
    catch (e)
    {
      e.message = "Unhandled JSON parsing error: " + e.message;
      console.error({ "message": e.message, "HT Text": text, "response": resp });
      throw e;
    }

    // HornTracker's lst is given in naive Pacific Time (which may be PST or PDT). Script TZ is EST/EDT (America/New_York)
    // NOTE: Apps Script does not use arguments for `toLocaleString`, i.e. the time zone cannot be specified.
    //   Without being able to specify the time zone, it is expected any HT data created or imported during US DST
    //   changeovers is off by up to 1 hour.
    const timeOffset = (new Date()).toLocaleString().toLowerCase().indexOf("dt") > -1 ? "-0700" : "-0800";
    // var utcMillisOffset = (new Date()).getTimezoneOffset() *60*1000;

    // Construct a UID-keyed Object from the received data, with the crown data as the value.
    // A member will only appear once in the received data.
    for (var htKey in mostMiceData)
    {
      // Traditional member IDs appear as 'ht_#############'.
      // TODO: document and handle the newer member ID format
      var uid = htKey.slice(3);
      // Initialize this user's data object with bronze, silver, and gold crown counts.
      htData[uid] = _getCrowns_(mostMiceData[htKey].mice);
      // Timestamp the MostMice record (using ms since epoch to eliminate timezone issues).
      htData[uid].seen = Date.parse(mostMiceData[htKey].lst.replace(/-/g, "/") + timeOffset);
    }
    return htData;
  }
  /** Nested function which handles querying @devjacksmith's "MH Latest Crowns" FusionTable and constructing the expected data object.
   * @param {string} uids  A comma-joined string of member identifiers for whom to request catch totals.
   * @return {Object <string, CrownSnapshot>} A UID-indexed object with the latest known crown counts for the input members.
   */
  function QueryJacksData_(uids)
  {
    // Jack provides a queryable FusionTable which may or may not have data for the users in question.
    // His fusiontable is unique on snuid - each person has only one record.
    // snuid | timestamp (seconds UTC) | bronze | silver | gold | platinum | diamond
    const sql = "SELECT * FROM " + alt_table + " WHERE snuid IN (" + uids + ")";
    const jkData = {};
    const resp = bq_readMHCTCrowns_(uids.split(','));
    const records = resp.rows;

    // Check that the SQL response has the data we want.
    const headers = resp.columns.map(function (col) { return col.toLowerCase(); });
    const indices = ["bronze", "silver", "gold", "platinum", "diamond", "timestamp"].reduce(function (acc, label) {
      acc[label] = headers.indexOf(label);
      return acc;
    }, {});
    if (!Object.keys(indices).every(function (val) { return indices[val] > -1; }))
    {
      console.error({
        "message": "Unable to find required column headers in MHCT's BQ Table.",
        "response": records, "headers": headers, "indices": indices
      });
      return jkData;
    }

    records.forEach(function (record) {
      var id = record[0].toString();
      jkData[id] = {
        bronze: record[indices.bronze] * 1,
        silver: record[indices.silver] * 1,
        gold: record[indices.gold] * 1,
        seen: record[indices.timestamp] * 1000
      };

      // Since not all FusionTable records have values for these columns, we need to coerce them to a valid number.
      var platCount = parseInt(record[indices.platinum], 10);
      var diamondCount = parseInt(record[indices.diamond], 10);
      // Add platinum and diamond crowns to the gold tally. TODO: record these separately.
      jkData[id].gold += (isNaN(platCount) ? 0 : platCount) + (isNaN(diamondCount) ? 0 : diamondCount);
    });
    return jkData;
  }
}
/**
 * function UpdateStale:          Writes to the secondary page that serves as a "Help Wanted" ad for
 *                                recruiting updates for oft-unvisited members.
 * @param {Object} wb             The MHCC SpreadsheetApp instance.
 * @param {number} lostTime       The number of milliseconds after which a member is considered
 *                                "in dire need of revisiting."
 */
function UpdateStale_(wb, lostTime)
{
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30000))
    return;

  // Retrieve the most recent crown snapshots in chronological order from oldest -> newest,
  // based on the last time the member's profile was seen.
  const db = getMyDb_(wb, 3);
  const lostSheet = wb.getSheetByName('"Lost" Hunters'); if (!lostSheet) return;
  // Remove pre-existing "Stale" reports.
  lostSheet.getRange(3, 3, Math.max(lostSheet.getLastRow() - 2, 1), 4).setValue('');

  const staleArray = [], startTime = new Date().getTime();
  for (var i = 0, len = db.length; i < len; ++i)
  {
    // Snapshots are ordered oldest -> newest, so quit once the first non-old record is found.
    if (startTime - db[i][2] <= lostTime)
      break;
    staleArray.push([
      db[i][0],
      Utilities.formatDate(new Date(db[i][2]), 'EST', 'yyyy-MM-dd'),
      "https://apps.facebook.com/mousehunt/profile.php?snuid=" + db[i][1],
      "https://www.mousehuntgame.com/profile.php?snuid=" + db[i][1]
    ]);
  }
  // Add new Stale hunters (if possible).
  if (staleArray.length > 0)
    lostSheet.getRange(3, 3, staleArray.length, 4).setValues(staleArray);
  lock.releaseLock();
}
/**
 * function UpdateScoreboard:     Write the most recent snapshots of each member's crowns to the
 *                                Scoreboard page, update the spreadsheet's snapshots of crown data
 *                                on SheetDb, and update the number of members.
 */
function UpdateScoreboard()
{
  /**
   * Compute the "Squirrel" rating from the given silver & gold crown counts,
   * and the current tier minimums. (The ratings may have changed even if the
   * member's crowns did not).
   * @param {(number|string)[]} record A member's new database record (modified in-place).
   * @param {[number, string, string][]} squirrelTiers an array of crown count minimums and the corresponding Squirrel tier
   * @param {number} mhccIndex The array index corresponding to the squirrel data.
   * @param {number} squirrelIndex The array index corresponding to the Squirrel data.
   */
  function _setCurrentSquirrel(record, squirrelTiers, mhccIndex, squirrelIndex)
  {
    for (var k = 0; k < squirrelTiers.length; ++k)
      if (record[mhccIndex] >= squirrelTiers[k][0]) {
        // Crown count meets/exceeds required crowns for this level.
        record[squirrelIndex] = squirrelTiers[k][2];
        return;
      }
  }
  /**
   * Function that handles the prediction of the next scoreboard update.
   * @param {string} targetSheet The sheet on which scoreboard timings are logged
   * @param {string} predictionCell The cell in which the next scoreboard update time is estimated
   * @param {string} logCell The cell in which the current scoreboard update's start time is logged
   * @param {Date} start The time at which the current update began running.
   */
  function _estimateNextScoreboard(targetSheet, predictionCell, logCell, start)
  {
    const s = wb.getSheetByName(targetSheet), r = s.getRange(logCell);
    s.getRange(predictionCell).setValue((start - r.getValue()) / (24 * 3600 * 1000));
    r.setValue(start);
  }

  const startTime = new Date(), wb = SpreadsheetApp.getActive();
  try { const aRankTitle = wb.getSheetByName('Members').getRange(3, 8, numCustomTitles, 3).getValues(); }
  catch (e) { throw new Error("'Members' sheet was renamed or deleted - cannot locate crown-title relationship data."); }

  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30000))
    return;

  // To build the scoreboard....
  // 1) Store the most recent snapshots of all members on SheetDb
  if (!saveMyDb_(wb, bq_getLatestRows_().rows))
    throw new Error('Unable to save snapshots retrieved from crown database');

  // 2) Sort it by MHCC crowns, then LastCrown, then LastSeen. This means the first to have a
  // particular crown total should rank above someone who (was seen) attaining it at a later time.
  // TODO: use the new MHCCCrownChange column rather than LastCrown, to avoid penalizing for upgrading crowns.
  const allHunters = getMyDb_(wb, [{ column: 9, ascending: false }, { column: 4, ascending: true }, { column: 3, ascending: true }]),
    len = allHunters.length, scoreboardArr = [],
    plotLink = 'https://script.google.com/macros/s/AKfycbxvvtBNQ66BBlB-md1jn_y-TlujQf1ytDkYG-7nEAG4SDaecMFF/exec?uid=';
  var rank = 1;
  if (!len)
    return;

  // 3) Build the array with this format:   Rank UpdateDate CrownChangeDate Squirrel MHCCCrowns Name Profile
  do {
    var ah_i = rank - 1;
    _setCurrentSquirrel(allHunters[ah_i], aRankTitle, 8, 9);
    scoreboardArr.push([
      rank,
      Utilities.formatDate(new Date(allHunters[ah_i][2]), 'EST', 'yyyy-MM-dd'),              // Last Seen
      Utilities.formatDate(new Date(allHunters[ah_i][3]), 'EST', 'yyyy-MM-dd'),              // Last Crown
      allHunters[ah_i][9],                                                                   // Squirrel
      '=HYPERLINK("' + plotLink + allHunters[ah_i][1] + '","' + allHunters[ah_i][8] + '")',  // # MHCC Crowns
      allHunters[ah_i][0],                                                                   // Name
      'https://apps.facebook.com/mousehunt/profile.php?snuid=' + allHunters[ah_i][1],
      'https://www.mousehuntgame.com/profile.php?snuid=' + allHunters[ah_i][1]
    ]);
    if (rank % 150 === 0)
      scoreboardArr.push(['Rank', 'Last Seen', 'Last Crown', 'Squirrel Rank', 'G+S Crowns', 'Hunter', 'Profile Link', 'MHG']);

    // Store the time this rank was generated.
    allHunters[ah_i][10] = startTime.getTime();
    // Store the counter as the hunters' rank, then increment the counter.
    allHunters[ah_i][11] = rank++;
  } while (rank <= len);

  // 4) Write it to the spreadsheet
  const sheet = wb.getSheetByName('Scoreboard');
  sheet.getRange(2, 1, sheet.getLastRow(), scoreboardArr[0].length).setValue('');
  sheet.getRange(2, 1, scoreboardArr.length, scoreboardArr[0].length).setValues(scoreboardArr);

  // 5) Upload it to the Rank History DB.
  const rankUpload = allHunters.map(function (record) {
    // Name, UID, LastSeen, RankTime, Rank, MHCC Crowns
    // [0],  [1], [2],      [10],     [11], [8]
    return [record[0], String(record[1]), record[2], record[10], record[11], record[8]];
  });
  if (rankUpload.length && rankUpload[0].length === 6)
    bq_addRankSnapshots_(rankUpload);

  // Provide estimate of the next new scoreboard posting and the time this one was posted.
  _estimateNextScoreboard('Members', 'I23', 'H23', startTime);

  // Overwrite the latest db version with the version that has the proper ranks, Squirrel, and ranktimes.
  saveMyDb_(wb, allHunters);

  // If a member hasn't been seen in the last 20 days, then request a high-priority update
  UpdateStale_(wb, 20 * 86400 * 1000);

  console.log("%s sec. for all scoreboard operations", (new Date() - startTime) / 1000);
  lock.releaseLock();
}

/**
 * Webhook to MH Discord
 *   Lets everyone know when the scoreboard has updated
 *   @param {string} url A webhook URL which accepts POST data.
 */
function doWebhookPost_(url)
{
  var hookObject = {
    "content": "The MHCC Scoreboard just updated!\n[Check it out](https://docs.google.com/spreadsheets/d/e/2PACX-1vQG5g3vp-q7LRYug-yZR3tSwQzAdN7qaYFzhlZYeA32vLtq1mJcq7qhH80planwei99JtLRFAhJuTZn/pubhtml) or come visit us on [Facebook](<https://www.facebook.com/groups/MousehuntCenturyClub/>)",
    "avatar_url": 'https://i.imgur.com/RNDe7XK.jpg'
  };
  var params = {
    "method": "post",
    "payload": hookObject
  };
  try { UrlFetchApp.fetch(url, params); }
  catch (e) { console.warn({ "message": "Webhook failed", "exception": e }); }
}
