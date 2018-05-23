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
var mhccSSkey = '1P8UDv4j2lPM0hAKw4EbBT_GtvlOgFYeARV16NzWA6pc';
var crownDBnumColumns = 12;
// Change this number when additional rows containing special titles (like "Super Secret Squirrel") get added.
// This only needs to change when rows are added - if values of existing rows are changed, all is well. :)
var numCustomTitles = 16;
/**
 * function onOpen()      Sets up the admin's menu from the spreadsheet interface.
 */
function onOpen()
{
  SpreadsheetApp.getActiveSpreadsheet().addMenu('Administration', [{name:"Manage Members", functionName:"getSidebar"},
                                                                   {name:"Perform Crown Update", functionName:"UpdateDatabase"},
                                                                   {name:"Check Database Size", functionName:"getDbSize"}]);
}
/**
 * function getMyDb_          Returns the entire data contents of the worksheet SheetDb to the calling
 *                            code as a rectangular array. Does not supply header information.
 * @param  {Workbook} wb      The workbook containing the worksheet named SheetDb
 * @param  {Object} sortObj   An integer column number or an Object[][] of sort objects
 * @return {Array[]}          An M by N rectangular array which can be used with Range.setValues() methods
 */
function getMyDb_(wb, sortObj)
{
  var SS = wb.getSheetByName('SheetDb');
  var db = SS.getRange(2, 1, SS.getLastRow() - 1, SS.getLastColumn()).sort(sortObj).getValues();
  return db;
}
/**
 * function saveMyDb_         Uses the Range.setValues() method to write a rectangular array to the worksheet
 *
 * @param  {Workbook} wb      The workbook containing the worksheet named SheetDb
 * @param  {Array} db         The rectangular array of data to write to SheetDb
 * @return {Boolean}          Returns true if the data was written successfully, and false if a database lock was not acquired.
 */
function saveMyDb_(wb, db)
{
  if (!wb || !db || !db.length || !db[0].length)
    return false;
  var lock = LockService.getScriptLock();
  lock.tryLock(30000);
  if (lock.hasLock())
  {
    // Have a lock on the db, now save.
    var SS = wb.getSheetByName('SheetDb');
    // If the new db is smaller, the old db must be cleared first.
    if (db.length < SS.getLastRow() - 1)
    {
      SS.getRange(2, 1, SS.getLastRow(), SS.getLastColumn()).clearContent();
      SpreadsheetApp.flush();
    }
    SS.getRange(2, 1, db.length, db[0].length).setValues(db).sort(1);
    SpreadsheetApp.flush();
    lock.releaseLock();
    return true
  }
  return false
}
/**
 * function UpdateDatabase  This is the main function which governs the process of updating crowns
 *                          and writing Scoreboard data. Batch sizes of 127 are the maximum, due to
 *                          the maximum length of a URL. The maximum execution time is 300 seconds,
 *                          after which Google's servers will kill the script.
 */
function UpdateDatabase()
{
  var batchSize = 127, startTime = new Date(), maxRuntime = 150;
  var wb = SpreadsheetApp.getActive();
  var db = getMyDb_(wb, 1);
  var props = PropertiesService.getScriptProperties().getProperties();
  // Database records count (may be larger than the written db on SheetDb)
  var numMembers = props.numMembers * 1;
  // The last queried member number indicates where to begin new update queries.
  var lastRan = props.lastRan * 1;
  
  // Read in the MHCC tiers as an array.
  // (If the MHCC tiers are moved, this getRange target MUST be updated!)
  try { var aRankTitle = wb.getSheetByName('Members').getRange(3, 8, numCustomTitles, 3).getValues(); }
  catch (e) { throw new Error("'Members' sheet was renamed or deleted - cannot locate crown-title relationship data."); }

  // After each Scoreboard operation, a backup of the FusionTable database should be created.
  if (lastRan >= numMembers)
  {
    UpdateScoreboard();
    PropertiesService.getScriptProperties().setProperty('lastRan', 0);
    doBackupTable_();
    if (Math.random() < 0.4)
      doWebhookPost_(props['mhDiscord']);
  }
  else
  {
    var lock = LockService.getScriptLock();
    lock.waitLock(30000);
    // Guard against concurrent execution of code which modifies LastRan (and thus, user data).
    if (!lock.hasLock())
      return;
    
    // allMembers is an array of [Name, UID]
    var allMembers = getUserBatch_(0, numMembers);
    var partialRecords = {}, updateLastRan = false;

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
        partialRecords[id] = newCrownData.partials[id];

      lastRan += batchSize;
    } while (((new Date() - startTime) < maxRuntime * 1000) && (lastRan < numMembers));

    // Convert partial records into insertable MHCC records. This involves querying the
    // FusionTable database to determine each member's last crown counts.
    var newRecords = [];
    if (Object.keys(partialRecords).length)
    {
      var existingRecords = _fetchExistingRecords_(partialRecords);
      if (!existingRecords || !existingRecords.records || !existingRecords.indices)
        throw new Error("Failed to obtain stored records");
      newRecords = _computeRecords_(partialRecords, existingRecords.records, existingRecords.indices);
      if (newRecords.length > 0)
        ftBatchWrite_(newRecords);
    }

    if (updateLastRan)
      PropertiesService.getScriptProperties().setProperty('lastRan', lastRan.toString());
  
    console.log({message: 'Through ' + lastRan + ' members, elapsed=' + (new Date() - startTime) / 1000 + ' sec. Uploaded ' + newRecords.length + ' rows.'});
    lock.releaseLock();
  }

  /**
   * Nested function which handles obtaining the previously known data for the given partial records.
   * Returns an object with the full record referenced for a given member in the partial records, indexed by UID.
   * 
   * @param {{String:{String:any}} partials
   * @return {{records:Array[], indices:{columnName:Number}}}
   */
  function _fetchExistingRecords_(partials)
  {
    if (!partials || !Object.keys(partials).length)
      return null;

    // TODO: evaluate if getLatestRows_() should be used instead of/in this function.

    // Collect the UIDs which need to be queried in the FusionTable.
    var sqlParts = [
      "SELECT * FROM " + ftid + " WHERE UID IN (",
      "",
      " ) AND LastTouched IN (",
      "",
      ")"
    ];

    var criteriaList = []
    for (var uid in partials)
      criteriaList.push({ uid: uid, lt: partials[uid].lastTouched });
    if (!criteriaList.length)
      return null;

    // Collect the full records into a UID-indexed Object (for rapid querying).
    var storedRecords = {}, indices = {}, labels = ["uid", "lastseen", "lastcrown", "lasttouched", "bronze", "silver", "gold", "mhcc", "squirrel"];
    do {
      // Construct a maximal query (8000 characters or less) from the given criteria.
      var queryUIDs = [], queryLT = [], sql = "";
      do {
        var cl = criteriaList.pop();
        queryUIDs.push(cl.uid);
        queryLT.push(cl.lt);
        sql = sqlParts[0] + queryUIDs.join(",") + sqlParts[2] + queryLT.join(",") + sqlParts[4];
      } while (sql.length < 8000 && criteriaList.length);

      // Execute the query to obtain the members' full records (and thus their previous crown count and crown change date).
      var resp = FusionTables.Query.sqlGet(sql, { quotaUser: queryUIDs[0] });
      if (!resp || !resp.rows || !resp.rows.length || !resp.rows[0].length || !resp.columns) return [];
      var headers = resp.columns.map(function (col) { return String(col).toLowerCase(); });
      if (!Object.keys(indices).length)
        labels.forEach(function (l) {
          indices[l] = headers.indexOf(l);
          if (indices[l] === -1) throw new Error("Missing column name '" + l + "'");
        });
      resp.rows.forEach(function (record) { storedRecords[record[indices.uid]] = record; });
    } while (criteriaList.length);
    return { records: storedRecords, indices: indices };
  }
  /**
   * Nested function which constructs new records based on the previously known data and the latest input data.
   * 
   * @param {any} partials         The latest crown snapshots for the members being updated.
   * @param {any} existingRecords  The most recent records for the members being updated.
   * @param {{String:Number}} indices          Mapping between column names and column position in the record.
   * @return {Array[]}             New, ready-to-insert records built from the latest crown snapshots.
   */
  function _computeRecords_(partials, existingRecords, indices)
  {
    // Now that the desired full records are available, iterate through the
    // partial list and use the stored data to construct the new record.
    var newRecords = [];
    for (var uid in partials)
    {
      var dbr = existingRecords[uid];
      if (!dbr)
        // TODO: Default construct a record (requires piping the member name into the partials object).
        throw new Error("Member with id '" + uid + "' has no existing crown records.");

      // Sort the new data in ascending order, to ensure crown change date consistency.
      if (partials[uid].newData.length > 1)
        partials[uid].newData.sort(function (a, b) { return a.lastSeen - b.lastSeen; });

      partials[uid].newData.forEach(function (newData, i, all) {
        var newRecord = [];
        newRecord[0] = dbr[0];           // Name.
        newRecord[indices.uid] = uid;    // UID.
        newRecord[indices.lastseen] = newData.lastSeen;
        newRecord[indices.bronze] = newData.bronze;
        newRecord[indices.silver] = newData.silver;
        newRecord[indices.gold] = newData.gold;
        newRecord[indices.mhcc] = newData.silver - 0 + newData.gold - 0;

        // Check for different crown counts.
        var last = (i > 0)
          ? { bronze: all[i - 1].bronze, silver: all[i - 1].silver, gold: all[i - 1].gold }
          : { bronze: dbr[indices.bronze], silver: dbr[indices.silver], gold: dbr[indices.gold] };
        // Update the crown change date to the time the member's new data was seen if any crowns have changed.
        if (newData.bronze !== last.bronze || newData.silver !== last.silver || newData.gold !== last.gold)
          newRecord[indices.lastcrown] = newData.lastSeen;
        else
          newRecord[indices.lastcrown] = (i > 0)
            ? newRecords[newRecords.length - 1][indices.lastcrown]
            : dbr[indices.lastcrown];

        // Compute a new "Squirrel" rating (the ratings may have changed even if the member's crowns did not).
        for (var k = 0; k < aRankTitle.length; ++k)
          if (newRecord[indices.mhcc] >= aRankTitle[k][0])
          {
            // Crown count meets/exceeds required crowns for this level.
            newRecord[indices.squirrel] = aRankTitle[k][2];
            break;
          }

        // Insert the computed record into the records that will be uploaded.
        newRecords.push(newRecord);
      });
    }

    // Assign a unique lastTouched value to each new record.
    var now = (new Date()).getTime();
    for (var r = 0; r < newRecords.length; ++r)
      newRecords[r][indices.lasttouched] = ++now;
    return newRecords;
  }
  /** Nested function which handles requesting crown info for a given set of members from the various data
   *  sources available:
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
   * @param {String[][]} memberSet  The subset of the global member list for which data should be queried.
   * @return {{String:{lastSeen:Number, lastCrown:Number, lastTouched:Number, newData:[{lastSeen:Number, bronze:Number, silver:Number, gold:Number}]}}}
   */
  function _getNewCrownData_(memberSet)
  {
    // Check if the two lastSeen timestamps (milliseconds) differ by more than 10 seconds.
    function _isDifferentEnough_(lastSeen1, lastSeen2)
    {
      var diff = lastSeen1 - lastSeen2;
      return Math.abs(diff) / 1000 > 10;
    }

    // Check if the record has different stored crowns than the other ones being added.
    function _hasDifferentCrowns_(record, recordsToAdd)
    {
      var hasSame = false;
      recordsToAdd.forEach(function (existing)
      {
        hasSame |= (record.gold === existing.gold
          && record.silver === existing.silver
          && record.bronze === existing.bronze);
      });
      return !hasSame;
    }

    // Assemble the IDs to query for, and create an Object in which to collect the data from each datasource.
    var urlIDs = [], data = {};
    memberSet.forEach(function (member) {
      if (!member[1]) return;
      urlIDs.push(String(member[1]));
      data[String(member[1])] = { lastSeen: 0, lastCrown: 0, lastTouched = 0, collected: [], toAdd: [] };
    });
    if (!urlIDs.length) { console.warn({ message: "No valid UIDs in member set", memberSet: memberSet }); return null; }

    // Obtain the lastSeen and lastTouched timestamps for the users in this batch.
    var uidString = urlIDs.join(",");
    var sql = 'SELECT UID, MAXIMUM(LastSeen), MAXIMUM(LastCrown), MAXIMUM(LastTouched) FROM ' + ftid;
    sql += ' WHERE UID IN (' + uidString + ') GROUP BY UID';
    var resp = FusionTables.Query.sqlGet(sql);
    if (!resp || !resp.rows || !resp.rows.length || !resp.rows[0].length) return null;
    resp.rows.forEach(function (memberRow) {
      data[memberRow[0]].lastSeen = memberRow[1] * 1;
      data[memberRow[0]].lastCrown = memberRow[2] * 1;
      data[memberRow[0]].lastTouched = memberRow[3] * 1;
    });

    // Collect the data from each datasource.
    var datasourceData = [
      QueryHTData_(uidString),
      QueryJacksData_(uidString)
    ];

    // Compare the collected data with the existing data.
    var output = {}, now = new Date();
    for (var uid in data)
    {
      // If this member's records as held by the datasources are different than the stored data, stage it.
      for (var ds = 0; ds < datasourceData.length; ++ds)
      {
        var dsr = datasourceData[ds][uid];
        if (dsr && (dsr.lastSeen > data[uid].lastSeen || (now - data[uid].lastTouched) / (1000 * 86400) > 7))
          data[uid].collected.push(dsr);
      }
      
      // Keep only unique staged records (i.e. multiple datasources may have collected the same data at a similar time).
      var latest = 0;
      data[uid].collected.forEach(function (record) {
        if (_isDifferentEnough_(record.lastSeen, latest)
            && _hasDifferentCrowns_(record, data[uid].toAdd))
        {
          latest = Math.max(latest, record.lastSeen);
          data[uid].toAdd.push(record);
        }
      });

      if (data[uid].toAdd.length)
        output[uid] = {
          lastSeen: data[uid].lastSeen,
          lastCrown: data[uid].lastCrown,
          lastTouched: data[uid].lastTouched,
          newData: data[uid].toAdd,
        };
    }

    return { partials: output, completed: true };
  }
  /** Nested function which handles querying HornTracker's MostMice endpoint and constructing the expected data object:
   *  { uid1: {
   *      lastSeen: milliseconds since epoch indicating when this record was collected
   *      bronze: number of bronze crowns
   *      silver: number of silver crowns
   *      gold:   number of gold crowns
   *    },
   *    ... }
   * @param {String} uids  A comma-joined string of member identifiers for whom to request catch totals.
   * @return {{String:{ lastSeen:Number, bronze:Number, silver:Number, gold:Number }}}
   *    
   */
  function QueryHTData_(uids)
  {
    // HornTracker returns the individual catch counts per mouse, so crown counts must be aggregated:
    // {..., mice: { mouseName1: catches1, mouseName2: catches 2, ...}}
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

    var htData = {};
    try { var resp = UrlFetchApp.fetch('http://horntracker.com/backend/mostmice.php?function=hunters&hunters=' + uids); }
    catch (e)
    {
      // If the query to HT failed in an unknown way, throw the new error.
      var msg = e.message.toLowerCase();
      var knownErrors = ["unexpected error: h", "timeout: h"];
      knownErrors.forEach(function (fragment) { if (msg.indexOf(fragment) > -1) resp = null; });
      if (resp !== null)
      {
        e.message = "HT GET - errmsg: " + e.message;
        console.error({ message: e.message, uids: uids.split(","), response: resp });
        throw e;
      }
    }
    // Validate the returned data prior to attempting to coerce it to JSON.
    if (!resp || resp.getResponseCode() !== 200)
      return htData;

    var text = resp.getContentText();
    if (!text.length || text.toLowerCase().indexOf('connect to mysql') !== -1)
      return htData;
    
    try { var mostMiceData = JSON.parse(text).hunters; }
    catch (e)
    {
      e.message = "Unhandled JSON parsing error: " + e.message;
      console.log({ message: e.message, "HT Text": text , response: resp });
      throw e;
    }

    // HornTracker's lst is given in naive Pacific Time (which may be PST or PDT). Script TZ is EST/EDT (America/New_York)
    // NOTE: Apps Script does not use arguments for `toLocaleString`, i.e. the time zone cannot be specified.
    //   Without being able to specify the time zone, it is expected any HT data created or imported during US DST
    //   changeovers is off by up to 1 hour.
    var timeOffset = (new Date()).toLocaleString().toLowerCase().indexOf("dt") > -1 ? "-0700" : "-0800";
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
      htData[uid].lastSeen = Date.parse(mostMiceData[htKey].lst.replace(/-/g, "/") + String(timeOffset));
    }
    return htData;
  }
  /** Nested function which handles querying @devjacksmith's MH Latest Crowns FusionTable and constructing the expected data object:
   * { uid1: {
   *      lastSeen: milliseconds since epoch indicating when this record was collected
   *      bronze: number of bronze crowns
   *      silver: number of silver crowns
   *      gold:   number of gold crowns
   *    },
   *    ... }
   * @param {String} uids  A comma-joined string of member identifiers for whom to request catch totals.
   * @return {{String:{ lastSeen:Number, bronze:Number, silver:Number, gold:Number }}}
   *
   */
  function QueryJacksData_(uids)
  {
    // Jack provides a queryable FusionTable which may or may not have data for the users in question.
    // His fusiontable is unique on snuid - each person has only one record.
    // snuid | timestamp (seconds UTC) | bronze | silver | gold 
    var sql = "SELECT * FROM " + alt_table + " WHERE SNUID IN (" + uids + ")";
    var jkData = {};
    try { var resp = FusionTables.Query.sqlGet(sql); }
    catch (e)
    {
      e.message = "Failed to query Jack's 'MH Crowns' FusionTable" + e.message;
      console.error({ message: e.message, query: sql, response: resp });
      throw e;
    }
    // Ensure well-formed data was obtained.
    if (!resp || !resp.rows || !resp.rows.length || !resp.rows[0].length || !resp.columns)
      return jkData;

    // Check that the SQL response has the data we want.
    var headers = resp.columns.map(function (col) { return col.toLowerCase(); });
    var indices = {};
    ["bronze", "silver", "gold", "timestamp"].forEach(function (label) { indices[label] = headers.indexOf(label); });
    if (!Object.keys(indices).every(function (val) { return indices[val] > -1; }))
    {
      console.error({
        message: "Unable to find required column headers in Jack's FusionTable.",
        response: resp, headers: headers, indices: indices
      });
      return jkData;
    }
    resp.rows.forEach(function (record) {
      jkData[String(record[0])] = {
        bronze: record[indices.bronze] * 1,
        silver: record[indices.silver] * 1,
        gold: record[indices.gold] * 1,
        lastSeen: record[indices.timestamp] * 1000
      };
    });
    return jkData;
  }
}
/**
 * function UpdateStale:          Writes to the secondary page that serves as a "Help Wanted" ad for
 *                                recruiting updates for oft-unvisited members.
 * @param {Object} wb             The MHCC SpreadsheetApp instance.
 * @param {Integer} lostTime      The number of milliseconds after which a member is considered
 *                                "in dire need of revisiting."
 */
function UpdateStale_(wb, lostTime)
{
  var lock = LockService.getScriptLock();
  lock.waitLock(30000);
  if (lock.hasLock())
  {
    // Retrieve the most recent crown snapshots in chronological order from oldest -> newest,
    // based on the last time the member's profile was seen.
    var db = getMyDb_(wb,3);
    var lostSheet = wb.getSheetByName('"Lost" Hunters');
    var staleArray = [], len = db.length, startTime = new Date().getTime();
    for (var i = 0; i < len; ++i)
    {
      if (startTime - db[i][2] > lostTime)
        staleArray.push([db[i][0],
                          Utilities.formatDate(new Date(db[i][2]), 'EST', 'yyyy-MM-dd'),
                          "https://apps.facebook.com/mousehunt/profile.php?snuid=" + db[i][1],
                          "https://www.mousehuntgame.com/profile.php?snuid=" + db[i][1]
                        ]);
      // Snapshots were ordered oldest -> newest, so quit once the first non-old record is found.
      else
        break;
    }
    // Remove pre-existing "Stale" reports.
    lostSheet.getRange(3, 3, Math.max(lostSheet.getLastRow() - 2, 1), 4).setValue('');
    // Add new Stale hunters (if possible).
    if (staleArray.length > 0)
      lostSheet.getRange(3, 3, staleArray.length, 4).setValues(staleArray);
    lock.releaseLock();
  }
}
/**
 * function UpdateScoreboard:     Write the most recent snapshots of each member's crowns to the
 *                                Scoreboard page Update the spreadsheet's snapshots of crown data
 *                                on SheetDb, and update the number of members.
 */
function UpdateScoreboard()
{
  var startTime = new Date();
  var wb = SpreadsheetApp.getActive();
  var numMembers = FusionTables.Query.sqlGet("SELECT * FROM " + utbl).rows.length;
  PropertiesService.getScriptProperties().setProperty("numMembers", numMembers.toString());
  // To build the scoreboard....
  // 1) Request the most recent snapshots of all members
  var db = getLatestRows_();
  // 2) Store it on SheetDb
  var didSave = saveMyDb_(wb, db);
  if (didSave)
  {
    // If a member hasn't been seen in the last 20 days, then request a high-priority update
    console.time('stale');
    UpdateStale_(wb, 20 * 86400 * 1000);
    console.timeEnd('stale');
    
    // 3) Sort it by MHCC crowns, then LastCrown, then LastSeen. This means the first to have a
    // particular crown total should rank above someone who (was seen) attaining it at a later time.
    var allHunters = getMyDb_(wb, [{column:9, ascending:false}, {column:4, ascending:true}, {column:3, ascending:true}]);
    var scoreboardArr = [], len = allHunters.length, rank = 1;
    var plotLink = 'https://script.google.com/macros/s/AKfycbxvvtBNQ66BBlB-md1jn_y-TlujQf1ytDkYG-7nEAG4SDaecMFF/exec?uid=';
    // 4) Build the array with this format:   Rank UpdateDate CrownChangeDate Squirrel MHCCCrowns Name Profile
    console.time('Build Scoreboard Array');
    while (rank <= len)
    {
      scoreboardArr.push([rank,
                          Utilities.formatDate(new Date(allHunters[rank - 1][2]), 'EST', 'yyyy-MM-dd'),                  // Last Seen
                          Utilities.formatDate(new Date(allHunters[rank - 1][3]), 'EST', 'yyyy-MM-dd'),                  // Last Crown
                          allHunters[rank - 1][10],                                                                      // Squirrel
                          '=HYPERLINK("' + plotLink + allHunters[rank - 1][1] + '","' + allHunters[rank - 1][8] + '")',  // # MHCC Crowns
                          allHunters[rank - 1][0],                                                                       // Name
                          'https://apps.facebook.com/mousehunt/profile.php?snuid=' + allHunters[rank - 1][1],
                          'https://www.mousehuntgame.com/profile.php?snuid=' + allHunters[rank - 1][1]
                         ])
      if (rank % 150 === 0)
        scoreboardArr.push(['Rank', 'Last Seen', 'Last Crown', 'Squirrel Rank', 'G+S Crowns', 'Hunter', 'Profile Link', 'MHG']);

      // Store the time this rank was generated.
      allHunters[rank - 1][11] = startTime.getTime();
      // Store the counter as the hunters' rank, then increment the counter.
      allHunters[rank - 1][9] = rank++;
    }
    console.timeEnd('Build Scoreboard Array');
    // 5) Write it to the spreadsheet
    var sheet = wb.getSheetByName('Scoreboard');
    sheet.getRange(2, 1, sheet.getLastRow(), scoreboardArr[0].length).setValue('');
    sheet.getRange(2, 1, scoreboardArr.length, scoreboardArr[0].length).setValues(scoreboardArr);
    // Provide estimate of the next new scoreboard posting and the time this one was posted.
    wb.getSheetByName('Members').getRange('I23').setValue((startTime - wb.getSheetByName('Members').getRange('H23').getValue()) / (24 * 3600 * 1000));
    wb.getSheetByName('Members').getRange('H23').setValue(startTime);
    // Overwrite the latest db version with the version that has the proper ranks and ranktimes.
    saveMyDb_(wb, allHunters);

  }
  else
      throw new Error('Unable to save snapshots retrieved from crown database');
  SpreadsheetApp.flush();
  console.info((new Date() - startTime) / 1000 + ' sec for all scoreboard operations');
}


/**
 * Webhook to MH Discord
 *   Lets everyone know when the scoreboard has updated
 */
function doWebhookPost_(url)
{
  var hookObject = {
    "content":"The MHCC Scoreboard just updated!\n[Check it out](https://docs.google.com/spreadsheets/d/e/2PACX-1vQG5g3vp-q7LRYug-yZR3tSwQzAdN7qaYFzhlZYeA32vLtq1mJcq7qhH80planwei99JtLRFAhJuTZn/pubhtml) or come visit us on [Facebook](<https://www.facebook.com/groups/MousehuntCenturyClub/>)",
    "avatar_url":'https://i.imgur.com/RNDe7XK.jpg'
  };
  var params = {
    "method":"post",
    "payload":hookObject
  };
  try
  {
    UrlFetchApp.fetch(url, params);
  }
  catch (e)
  {
    console.info({message:'Webhook failed', exception: e});
  }
}
