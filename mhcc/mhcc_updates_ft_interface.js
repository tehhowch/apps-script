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
      return getDbIndexMap(refreshDbIndexMap_(), numMembers, true);
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
 * function addFusionMember     Interactive dialog launched from the spreadsheet interface. Scans
 *                              the Members FusionTable to determine if anyone with the given UID
 *                              exists. If yes, informs the adder that the member is already in the
 *                              database, otherwise adds the member to a list and reprompts for
 *                              additional members to add. If members remain that should be added,
 *                              calls addMember2Fusion_(), and updates the global property nMembers.
 */
function addFusionMember()
{
  var getMembers = true, gotDb = false,
      mem2Add = [], curMems = [], curMemNames = [], curUIDs = [], curRowIDs = [],
      start = new Date();
  do {
    var name = String(getMemberName2AddorDel_());
    // Break when the adder cancels the member addition process.
    if (name === String(-1))
      break;
    
    // The adder gave a valid name and confirmed it. Get the UID.
    var UID = String(getMemberUID2AddorDel_(name));
    // Break when the adder cancels the member addition process.
    if (UID === String(-1))
      break;
    
    // The adder gave a confirmed name & UID pair. Check for duplicates in existing members.
    if (!gotDb)
    {
      curMems = FusionTables.Query.sqlGet("SELECT Member, UID, ROWID FROM " + utbl).rows || [];
      // Member names are the 1st column.
      curMemNames = curMems.map( function (value, index) { return value[0] } );
      // UIDs are the 2nd column.
      curUIDs = curMems.map( function (value, index) { return value[1] } );
      // If a name update is performed, the Member's ROWID is needed.
      curRowIDs = curMems.map( function (value, index) { return value[2] } );
      gotDb = true;
    }

    // Check elements of the array to see if any element is the UID.
    var matchedIndex = curUIDs.indexOf(UID);
    if ((matchedIndex >= 0) && (matchedIndex < curRowIDs.length))
    {
      // Specified member matched to an existing member.
      var dupStr = "New member " + name + " is already in the database as ";
      dupStr += curMemNames[matchedIndex] + ".\\nWould you like to update their name?";
      var resp = Browser.msgBox("Oops!", dupStr, Browser.Buttons.YES_NO);
      if (resp.toLowerCase() === "yes")
      {
        // Perform a database name change.
        try
        {
          FusionTables.Query.sql("UPDATE " + utbl + " SET Member = '" + name + "' WHERE ROWID = '" + curRowIDs[matchedIndex] + "'");
        }
        catch (e)
        {
          Browser.msgBox("Sorry, couldn't update " + curMemNames[matchedIndex] + "'s name.\\nPerhaps they aren't in the database yet?");
        }
      }
    }
    else if (matchedIndex >= 0)
    {
      // Matched to a member that is already being added in this session. Update the added name.
      curMemNames[matchedIndex] = name;
      // Update the name within the mem2Add array (so the updated name is actually added).
      for (var row = 0; row < mem2Add.length; ++row)
        if (mem2Add[row][1] == UID)
          mem2Add[row][0] = name;
    }
    else
    {
      mem2Add.push([name, UID]);
      // Update search arrays too.
      curUIDs.push(UID);
      curMemNames.push(name);
    }
    
    var resp = Browser.msgBox("Add more?", "Would you like to add another member?", Browser.Buttons.YES_NO);
    getMembers = (resp.toLowerCase() === "yes");
  } while (getMembers && ((new Date() - start) / 1000 < 250));

  if (mem2Add.length > 0)
  {
    var props = PropertiesService.getScriptProperties();
    var lastRan = props.getProperty("lastRan") * 1 || 0;
    // Adjust the data update start position based on the current cycle progress.
    if (lastRan >= curUIDs.length)
      lastRan += addMember2Fusion_(mem2Add);
    else
    {
      // We may be adding the member in the middle of a data update cycle.
      var origUID = getUserBatch_(lastRan, 1);
      origUID = origUID[0][1];
      addMember2Fusion_(mem2Add);
      // Seek around to determine the new lastRan value.
      lastRan = getNewLastRanValue_(origUID, lastRan, mem2Add);
    }
    SpreadsheetApp.getActiveSpreadsheet().toast("Successfully added new member(s) to the MHCC Member Crown Database", "Success!", 5);
    props.setProperties({'numMembers':curUIDs.length.toString(), 'lastRan':lastRan.toString()});
  }
}
/**
 *  function delFusionMember    Called from the spreadsheet interface in order to remove members
 *                              from MHCC crown tracking. Prompts the adder for the names / profile
 *                              links of the members who should be removed from the crown database
 *                              and runs verification checks to ensure the proper rows are removed.
 */
function delFusionMember()
{
  var getMembers = true, gotDb = false,
      mem2Del = [], curMems = [], curMemNames = [], curUIDs = [], curRowIDs = [],
      startTime = new Date();
  do {
    var name = String(getMemberName2AddorDel_());
    // Break when the deleter cancels the member deletion process.
    if (name === String(-1))
      break;
    
    // The deleter gave a valid name and confirmed it. Get the UID.
    var UID = String(getMemberUID2AddorDel_(name));
    // Break when the deleter cancels the member deletion process.
    if (UID === String(-1))
      break;
    
    // Received a confirmed name and UID pair. Find the existing record (if any).
    if (gotDb === false)
    {
      curMems = FusionTables.Query.sql("SELECT Member, UID, ROWID FROM " + utbl).rows || [];
      curMemNames = curMems.map(function (value, index) { return value[0] } );
      curUIDs = curMems.map(function (value, index) { return value[1] } );
      curRowIDs = curMems.map(function (value, index) { return value[2] } );
      gotDb = true;
    }
    
    // Check elements of the array to see if any element is the UID.
    var matchedIndex = curUIDs.indexOf(UID);
    // If a UID matches, be sure the name matches.
    if ((matchedIndex >= 0) && (name.toLowerCase() === String(curMemNames[matchedIndex]).toLowerCase()))
      mem2Del.push([name, UID, curRowIDs[matchedIndex]]);
    else
    {
      var noMatchStr = "Couldn't find " + name + " in the database with that profile URL.";
      noMatchStr += "\\nPerhaps they're already deleted, or have a different stored name?";
      Browser.msgBox("Oops!", noMatchStr, Browser.Buttons.OK);
    }

    var resp = Browser.msgBox("Remove others?", "Would you like to remove another member?", Browser.Buttons.YES_NO);
    getMembers = (resp.toLowerCase() === "yes");
  } while (getMembers);

  if (mem2Del.length > 0) {
    var props = PropertiesService.getScriptProperties();
    var lastRan = props.getProperty("lastRan") * 1 || 0;
    var delMemberArray;
    // Adjust the data update start position based on the current cycle progress.
    if (lastRan >= curUIDs.length)
      delMemberArray = delMember_(mem2Del, startTime);
    else
    {
      // We may be removing the member in the middle of a data update cycle.
      var origUID = getUserBatch_(lastRan, 1);
      origUID = origUID[0][1];
      delMemberArray = delMember_(mem2Del, startTime);
      // Seek around to determine the new lastRan value.
      lastRan = getNewLastRanValue_(origUID, lastRan, delMemberArray);
    }
    props.setProperties({'numMembers':(curUIDs.length - delMemberArray.length).toString(), 'lastRan':lastRan.toString()});
  }
}
/**
 * Helper function that gets a name from the admin on the spreadsheet's interface.
 */
function getMemberName2AddorDel_()
{
  // Repeat the process until canceled or accepted.
  while (true)
  {
    var name = Browser.inputBox("Memberlist Update", "Enter the member's name", Browser.Buttons.OK_CANCEL);
    if (name.toLowerCase() === "cancel")
      return String(-1);
    var ok2Add = Browser.msgBox("Verify name", "Please confirm that the member is named '" + name + "'", Browser.Buttons.YES_NO);
    if (ok2Add.toLowerCase() === "yes")
      return String(name).trim();
  }
}
/**
 * function getMemberUID2AddorDel_  Uses the spreadsheet's interface to confirm that the supplied
 *                                  profile link (and the UID extracted from it) are correct.
 * @param  {String} memberName      The name of the member to be added to or deleted from MHCC.
 * @return {String}                 Returns the validated UID of the member.
 */
function getMemberUID2AddorDel_(memberName)
{
  // Repeat until canceled or accepted.
  while (true)
  {
    var profileLink = Browser.inputBox("What is " + memberName + "'s Profile Link?", "Please enter the URL of " + memberName + "'s profile", Browser.Buttons.OK_CANCEL);
    if (profileLink.toLowerCase() === "cancel")
      return String(-1);
    var UID = profileLink.slice(profileLink.search("=") + 1).toString();
    var ok2Add = Browser.msgBox("Verify URL", "Please confirm that this is " + memberName + "'s Profile:\\nhttp://apps.facebook.com/mousehunt/profile.php?snuid=" + UID, Browser.Buttons.YES_NO);
    if (ok2Add.toLowerCase() === "yes")
      return String(UID).trim();
  }
}
/**
 * function addMember2Fusion_   Inserts the given members into the Members FusionTable. Duplicate
 *                              member checking was handled in addFusionMember(). Users are added by
 *                              assembling into a CSV format and then passing to Table.importRows.
 *                              To minimize errors in UpdateDatabase(), the added members are also
 *                              pushed to SheetDb to guarantee a dbKeys reference will exist.
 * @param {Array[]} memList     2D array with the format [Member, UID].
 * @return {Integer}            The number of rows that were added to the Members database
 */
function addMember2Fusion_(memList)
{
  if (memList.length == 0)
    return;

  var rt = new Date(), resp = [], memCsv = [], crownCsv = [], readded = [];
  var dbSheet = SpreadsheetApp.getActive().getSheetByName('SheetDb');
  var newRank = dbSheet.getLastRow();
  
  // Create two arrays of the new members' data for CSV upload via importRows.
  do {
    var user = memList.pop();
    memCsv.push(user);
    // Query existing MHCC data to determine if a placeholder record is needed.
    var existing = getMostRecentRecord_(user[1]);
    if(existing.length > 0)
        readded.push(existing);
    else
    {
        var changeRankTime = rt.getTime();
        // Create a placeholder record.
        crownCsv.push([].concat(user, rt.getTime()-5000000000, changeRankTime, new Date().getTime(), 0, 0, 0, 0, newRank, "Weasel", changeRankTime));
        // Sleep a bit, to minimize LastTouched collisions. Does not prevent concurrent access or
        // modification by UpdateDatabase, which could generate a collision (but would be quite rare).
        Utilities.sleep(5);
    }
  } while (memList.length > 0)
  
  // Convert arrays into CSV strings and Blob for app-script class exchange.
  try
  {
    var uUpload = Utilities.newBlob(array2CSV_(memCsv), 'application/octet-stream');
    var cUpload = Utilities.newBlob(array2CSV_(crownCsv), 'application/octet-stream');
  }
  catch (e)
  {
    e.message = 'Unable to convert array into CSV format: ' + e.message;
    console.error({message: e.message, members: memCsv, crowns: crownCsv});
    throw e;
  }

  // Upload data to SheetDb to create an entry for getDbIndexMap_().
  try
  {
    while (crownCsv.length) { dbSheet.appendRow(crownCsv.pop()); }
    while (readded.length) { dbSheet.appendRow(readded.pop()); }

    // Upload data to the FusionTable databases
    resp[0] = FusionTables.Table.importRows(utbl, uUpload);
    resp[1] = FusionTables.Table.importRows(ftid, cUpload);
    return resp[0].numRowsReceived * 1;
  }
  catch (e)
  {
    e.message = "Unable to upload new members' rows: " + e.message;
    console.error({message: e.message, csv_crown: crownCsv, csv_member: memCsv, responses: resp});
    throw e;
  }
}
/**
 *   function delMember_          Prompts for confirmation to delete all crown records and then the
 *                                user's record. Will rate limit and also quit before timing out.
 *   @param {Array} memList       Array containing the name of the member(s) to delete from the
 *                                members and crown-count dbs as [Name, UID utblROWID]
 *   @param {Long} startTime      The beginning of the user's script time.
 *   @return {Array[]}              The [Name, UID] array of successfully deleted members.
 **/
function delMember_(memList, startTime)
{
  if (!memList || !memList.length || !memList[0].length)
    return [];

  var skippedMems = [], delMemberArray = [];
  for (var mem = 0; mem < memList.length; ++mem)
  {
    // If nearly out of time, report the skipped members.
    if ((new Date() - startTime) / 1000 >= 250)
      skippedMems.push("\\n" + memList[mem][0].toString());
    else
    {
      FusionTables.Query.sql("DELETE FROM " + utbl + " WHERE ROWID = '" + memList[mem][2] + "'");
      FusionTables.Query.sql("DELETE FROM " + ftid + " WHERE UID = '" + memList[mem][1]+ "'");
      console.log("Deleted user '" + memList[mem][0] + "' from the Members and Crowns tables.");
      delMemberArray.push(memList[mem]);
      // Obey rate limits.
      Utilities.sleep(memList.length > 29 ? 2000 : 200);
    }
  }
  if (skippedMems.length > 0)
  {
    var skipStr = "Of the input members, the following were not deleted due to time overrun:";
    skipStr += skippedMems.toString() + '\\nMore time is available, simply repeat the process.'
    Browser.msgBox("Some Not Deleted", skipStr, Browser.Buttons.OK);
  }
  return delMemberArray;
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
