/**
* Google FusionTables to serve as the backend
* Structural changes: since updating/deleting rows is a 1-by-1 operation (yes, really >_> ), we will have multiple entries for each person.
* To prevent excessive table growth, we will store only 30 snapshots per user, removing any duplicates which occur when the script updates
* more often than the person's data is refreshed.
* Bonus: this means we can serve new data such as most crowns in the last 30 days.
*/
var utbl = '1O4lBLsuvfEzmtySkJ-n-GjOSD_Ab2GsVgNIX8uZ-'; // small table containing user names, user ids, and rank (unique on user ids)
var ftid = '1hGdCWlLjBbbd-WW1y32I2e-fFqudDgE3ev-skAff'; // large table containing names, user ids, and crown counts data (no unique key!)
/**
 * function getUserBatch_       Return # limit members, starting with the index number start
 * @param  {integer} start      The most recently updated member, from an alphabetical sort of the database
 * @param  {integer} limit      The number of members to return
 * @return {String[][]}         [Name, UID, Rank]
 */
function getUserBatch_(start,limit){
  var sql = "SELECT Member, UID FROM "+utbl+" ORDER BY Member ASC OFFSET "+start+" LIMIT "+limit;
  var miniTable = FusionTables.Query.sql(sql);
  return miniTable.rows;
}
/**
 * function getDbIndex_     Assemble an indexing object for the db based on the user's UID (which should be unique)
 *                          For the MHCC SheetDb db, the UID is array index [1]
 * @param  {Array} db       a rectangular array of the most recent Scoreboard update
 * @return {object}         a simple object with the key-value pair of {UID: dbIndex}
 */
function getDbIndex_(db){
  var output = {}
  for (var i=0;i<db.length;i++){
    output[String(db[i][1])]=i;
  }
  if (Object.keys(output).length-db.length != 0) {
    // Should have been 0 if all UIDs appeared just once.
    throw new Error('Unique ID failed to be unique when indexing SheetDb by UID');
  }
  return output;
}
/**
 * function getLatestRows_    Returns the record associated with the most recently touched snapshot for each
 *                            member. Called by UpdateScoreboard. Should return a single record per member
 *                            so long as every record has a different LastTouched value. Accomplishes the
 *                            uniqueness by first querying for each UID's maximum LastTouched value, then
 *                            querying for the specific LastTouched values. Due to SQL string length limits,
 *                            only 571 LastTouched values can be queried at a time (len=8092, maxlen=8100)
 * @param  {integer} nMembers The total number of members in the database
 * @return {Array[][]}        N-by-12 array of data for UpdateScoreboard to parse and order.
 */
function getLatestRows_(nMembers){
  var sql = "SELECT UID, MAXIMUM(LastTouched) FROM "+ftid+" GROUP BY UID";
  var mostRecentRecordTimes = FusionTables.Query.sql(sql);
  var numReturnedMembers = mostRecentRecordTimes.rows.length;
  if (numReturnedMembers > 0) {
    if (numReturnedMembers < nMembers) {
      throw new Error((nMembers - numReturnedMembers).toString()+" members are missing scoreboard records");
    } else if ( numReturnedMembers > nMembers) {
      throw new Error("Script membercount is "+(numReturnedMembers-nMembers).toString()+" too low");
    } else {
      var batchSize = 571, snapshots = [], batchResult = [], nReturned = 0;
      var totalQueries = 1+Math.ceil(numReturnedMembers/batchSize);
      while ( mostRecentRecordTimes.rows.length > 0) {
        var ltArray = [], batchStartTime = new Date().getTime();
        for (var row=0;row<batchSize;row++){
          if (mostRecentRecordTimes.rows.length > 0) {
            var member = mostRecentRecordTimes.rows.pop();
            ltArray.push(member[1]);
          }
          sql = "SELECT * FROM "+ftid+" WHERE LastTouched IN ("+ltArray.join(",")+") ORDER BY Member ASC";
          try {
            batchResult = FusionTables.Query.sql(sql);
            nReturned += batchResult.rows.length*1;
            snapshots = [].concat(snapshots, batchResult.rows);
            // Avoid exceeding API rate limits (30 / min and 5 / sec)
            var elapsedMillis = new Date().getTime() - batchStartTime;
            if ( totalQueries > 29 ) {
              Utilities.sleep(2001-elapsedMillis);
            } else if ( elapsedMillis < 200 ) {
              Utilities.sleep(201-elapsedMillis);
            }
          }
          catch(e){
            Logger.log(e);
            throw new Error('Batchsize likely too large. SQL length was ='+sql.length);
          }
        }
      }
      if ( snapshots.length != numReturnedMembers ) {
        throw new Error('Did not receive the proper number of scoreboard records');
      } else {
        return snapshots.rows;
      }
    }
  }
  return [];
}
/**
 * function getUserHistory_   querys the crown data snapshots for the given user and returns their crown
 *                            counts as a function of when they were seen by Horntracker's MostMice. This
 *                            function may be useful for a future webpage which could plot the data,
 *                            either for just the specified hunter, or for all MHCC members. This function may
 *                            be deprecated if the Visualization API is used, as described here:
 *                            https://developers.google.com/fusiontables/docs/sample_code#chartTools
 * @param  {String} UID the user id of a specific member for which the crown data snapshots are returned
 * @return {Object}     an object containing "user" (member's name), "headers" (the data headers), and "dataset" (the data) for the member
 */
function getUserHistory_(UID){
  var sql = "SELECT Member, LastSeen, Bronze, Silver, Gold, MHCC, Rank, RankTime FROM "+ftid+" ORDER BY LastSeen ASC WHERE UID = '"+UID.toString()+"' LIMIT 30";
  var resp = FusionTables.Query.sql(sql);
  if (resp.rows.length > 0) {
    var columns = resp.columns;
    var data = resp.rows;
    var history = {"user":resp.rows[0][0],"headers":columns,"dataset":data};
    return history;
  }
  return "";
}
/**
 * function ftBatchWrite_     Converts the passed data array into a CSV blob and uploads it to the crown database
 * @param  {Array[][]} hdata  The array of data that will be written to the database.
 * @return {integer}          Returns the number of rows that were added to the database.
 */
function ftBatchWrite_(hdata){
  // hdata[][] [Member][UID][Seen][Crown][Touch][br][si][go][si+go][squirrel][RankTime]
  var crownCsv = array2CSV_(hdata);
  try {
    var cUpload = Utilities.newBlob(crownCsv,'application/octet-stream');
    try {
      var numAdded = FusionTables.Table.importRows(ftid,cUpload);
      return numAdded.numRowsReceived;
    }
    catch(e){
      throw new Error('Unable to upload rows');
    }
  }
  catch(e){
    throw new Error('Unable to convert array into CSV format');
  }
}
/**
 * function addFusionMember     Interactive dialog launched from the spreadsheet interface.
 *                              Scans the Members FusionTable to determine if anyone with the given UID
 *                              exists. If yes, then informs the adder that the member is already in the
 *                              database. If no, adds the member to a list and prompts for additional members
 *                              to add. If members remain that should be added, calls addMember2Fusion_()
 *                              with the member list, and updates the global property nMembers.
 */
function addFusionMember(){
  var getMembers = true, gotDb=false, hasMatch = true;
  var mem2Add = [], curMems=[], matchedIndex = -1;
  while (getMembers) {
    var name = String(getMemberName2AddorDel_());
    var UID = "";
    if (name == String(-1)) {
      getMembers = false;      // adder canceled the add
    } else {                     // adder gave a valid name and confirmed it
      UID = String(getMemberUID2AddorDel_(name));
      if (UID == String(-1)) {
        getMembers = false;  // adder canceled the add
      } else {                 // have a confirmed name & UID pair
        if (gotDb==false) {  // need to get the existing database records
          curMems = FusionTables.Query.sql("SELECT Member, UID, ROWID FROM "+utbl).rows||[];
          var curMemNames = curMems.map(function(value,index){return value[0]});  // member names are the 1st column in curMems array
          var curUIDs = curMems.map(function(value,index){return value[1]});      // UIDs are the 2nd column in curMems array
          var curRowIDs = curMems.map(function(value,index){return value[2]});    // used if a name update is wanted
          gotDb=true;
        }
        // Check elements of the array to see if any element is the UID
        matchedIndex = curUIDs.indexOf(UID);
        if ( matchedIndex >= 0 ) {
          // Specified member matched to an existing member
          var resp = Browser.msgBox("Oops!","New member "+name+" is already in the database as "+curMemNames[matchedIndex]+".\\nWould you like to update their name to "+name+"?",Browser.Buttons.YES_NO);
          if (resp.toLowerCase()=="yes") {
            // Perform a name change (only valid for those who are already in the db)
            try {FusionTables.Query.sql("UPDATE "+utbl+" SET Member = '"+name+"' WHERE ROWID = '"+curRowIDs[matchedIndex]+"'");}
            catch(e) {Browser.msgBox("Sorry, couldn't update "+curMemNames[matchedIndex]+"'s name.\\nPerhaps it isn't in the database yet?");}
          }
        } else {
          mem2Add.push([name,UID]);
          // Update search arrays too
          curUIDs.push(UID);
          curMemNames.push(name);
        }

      }
    }
    var resp = Browser.msgBox("Add more?","Would you like to add another member?",Browser.Buttons.YES_NO);
    if (resp.toLowerCase()=="no") getMembers = false;
  }
  // perhaps we've now got a list of only new members to add to the database
  if (mem2Add.length > 0) {
    var props = PropertiesService.getScriptProperties().getProperties();
    var lastRan = props.lastRan*1||0;
    var origUID = getUserBatch_(lastRan,1);
    origUID = origUID[0][1];
    var n = addMember2Fusion_(mem2Add);
    lastRan = getNewLastRanValue_(origUID,lastRan,mem2Add);
    SpreadsheetApp.getActiveSpreadsheet().toast("Successfully added "+n.toString()+" new member(s) to the MHCC Member Crown Database","Success!",5);
    PropertiesService.getScriptProperties().setPropertie({'numMembers':curUIDs.length.toString(),'lastRan':lastRan.toString()});
  }
}
/**
 *  function delFusionMember    Called from the spreadsheet main interface in order to remove members
 *                              from MHCC crown tracking. Prompts the adder for the names / profile
 *                              links of the members who should be removed from the crown database
 *                              and runs verification checks to ensure the proper rows are removed.
 */
function delFusionMember(){
  var startTime = new Date().getTime(), getMembers = true, gotDb=false, mem2Del = [], curMems=[], matchedIndex = -1;
  while (getMembers) {
    var name = String(getMemberName2AddorDel_());
    var UID = "";
    if (name == String(-1)) {
      getMembers = false;      // deleter canceled the del
    } else {
      UID = String(getMemberUID2AddorDel_(name));
      if (UID == String(-1)) {
        getMembers = false;  // deleter canceled the del
      } else {
        if (gotDb==false) {  // need to get the existing database records
          curMems = FusionTables.Query.sql("SELECT Member, UID, ROWID FROM "+utbl).rows||[];
          var curMemNames = curMems.map(function(value,index){return value[0]});
          var curUIDs = curMems.map(function(value,index){return value[1]});
          var curRowIDs = curMems.map(function(value,index){return value[2]});
          gotDb=true;
        }
        // Check elements of the array to see if any element is the UID
        matchedIndex = curUIDs.indexOf(UID);
        // If a UID matches, be sure the name matches
        if ( (matchedIndex >= 0) && (name.toLowerCase() === String(curMemNames[matchedIndex]).toLowerCase()) ) {
          mem2Del.push([name,UID,curRowIDs[matchedIndex]]);
        } else {
          Browser.msgBox("Oops!","Couldn't find "+name+" in the database with that profile URL.\\nPerhaps they're already deleted, or have a different stored name?",Browser.Buttons.OK);
        }
      }
    }
    var resp = Browser.msgBox("Remove others?","Would you like to remove another member?",Browser.Buttons.YES_NO);
    if (resp.toLowerCase()=="no") getMembers = false;
  }
  // perhaps we've now got a list of members to remove from the database
  if (mem2Del.length > 0) {
    var props = PropertiesService.getScriptProperties().getProperties();
    var lastRan = props.lastRan*1||0;
    var origUID = getUserBatch_(lastRan,1);
    origUID = origUID[0][1];
    var delMemberArray = delMember_(mem2Del,startTime);
    lastRan = getNewLastRanValue_(origUID,lastRan,delMemberArray);
    PropertiesService.getScriptProperties().setProperties({'numMembers':(curUIDs.length-delMemberArray.length).toString(),'lastRan':lastRan.toString()});
  }
}
/**
 * helper function that gets a name from the admin on the spreadsheet's interface
 */
function getMemberName2AddorDel_(){
  var name = Browser.inputBox("Memberlist Update","Enter the member's name",Browser.Buttons.OK_CANCEL);
  if (name.toLowerCase()=="cancel") return -1;
  var ok2Add = Browser.msgBox("Verify name","Please confirm that the member is named '"+name+"'",Browser.Buttons.YES_NO);
  if (ok2Add.toLowerCase()=="yes") {
    return String(name).trim();
  } else {
    // recurse until canceled or accepted
    return getMemberName2AddorDel_();
  }
}
/**
 * function getMemberUID2AddorDel_  Uses the spreadsheet's interface to confirm that the supplied profile link (and the
 *                                  UID extracted from it) belong to the person that the admin is trying to modify
 * @param  {String} memberName      The name of the member to be added to or deleted from MHCC
 * @return {String}                 Returns the validated UID of the member.
 */
function getMemberUID2AddorDel_(memberName){
  var profileLink = Browser.inputBox("What is "+memberName+"'s Profile Link?","Please enter the URL of "+memberName+"'s profile",Browser.Buttons.OK_CANCEL);
  if (profileLink.toLowerCase()=="cancel") return -1;
  var UID = profileLink.slice(profileLink.search("=")+1).toString();
  var ok2Add = Browser.msgBox("Verify URL","Please confirm that this is "+memberName+"'s Profile:\\nhttp://apps.facebook.com/mousehunt/profile.php?snuid="+UID,Browser.Buttons.YES_NO);
  if (ok2Add.toLowerCase()=="yes") {
    return String(UID).trim();
  } else {
    // recurse until canceled or accepted
    return getMemberUID2AddorDel_(memberName);
  }
}
/**
 * function addMember2Fusion_   Inserts the given members into the Members database. Duplicate member checking is
 *                              handled in addFusionMember(). Users are added by assembling into a CSV format and
 *                              then passing to Table.importRows
 * @param {Array[][]} memList   Two-column array of the members' names and the corresponding UIDs.
 * @return {Integer}            The number of rows that were added to the Members database
 */
function addMember2Fusion_(memList){
  var rt = new Date(), resp = [], memCsv = [], crownCsv = [];
  if (memList.length != 0) {
    // create two arrays of the new members' data for CSV upload via importRows
    while (memList.length > 0) {
      var user = memList.pop();
      memCsv.push(user);
      crownCsv.push([].concat(user,0,rt.getTime(),new Date().getTime(),0,0,0,0,20000,"Weasel"));
    }
    try {
      // Convert arrays into CSV strings and Blob for app-script class exchange
      var uUpload = Utilities.newBlob(array2CSV_(memCsv),'application/octet-stream');
      var cUpload = Utilities.newBlob(array2CSV_(crownCsv),'application/octet-stream');
      try {
        // Upload data to the FusionTable databases
        resp[0] = FusionTables.Table.importRows(utbl,uUpload);
        resp[1] = FusionTables.Table.importRows(ftid,cUpload);
        var numAdded = resp[0].numRowsReceived*1;
        return numAdded;
      }
      catch(e){ throw new Error("Unable to upload new members' rows") }
    }
    catch(e){ throw new Error('Unable to convert array into CSV format') }
  } else {
    return
  }
}
/**
 *   function delMember_          Prompts for confirmation to delete all crown records and then
 *                                the user's record. Will rate limit and also quit before timeout.
 *   @param {Array} memList       Array containing the name of the member(s) to delete from the
 *                                members and crown-count dbs as [Name, UID utblROWID]
 *   @param {Long} startTime      The beginning of the user's script time.
 *   @return {Array}              The [Name, UID] array of successfully deleted members.
 **/
function delMember_(memList,startTime){
  var skippedMems = [], delMemberArray = [];
  if ( memList.length != 0 ) {
    for (var mem=0;mem<memList.length;mem++){
      if ( (new Date().getTime()-startTime)/1000 >= 250 ) {
        // Out of time: report skipped members
        skippedMems.push("\\n"+memList[mem][0].toString());
      } else {
        var sql = "SELECT ROWID, UID FROM "+ftid+" WHERE UID = '"+memList[mem][1]+"'";
        var snapshots = FusionTables.Query.sql(sql).rows||[];
        var confirmString = "The member named '"+memList[mem][0]+"' has "+snapshots.length+" records."
        confirmString += "\\nThese cannot be removed faster than 30 per minute, requiring at least ";
        confirmString += Math.floor(1+snapshots.length/30*60)+"seconds.\\nBegin deletion?";
        var resp = Browser.msgBox("Confirmation Required",confirmString,Browser.Buttons.YES_NO);
        if (resp.toLowerCase()=="yes") {
          var sqlBase = "DELETE FROM "+ftid+" WHERE ROWID = '";
          var row = 0;
          while ( ((new Date().getTime()-startTime)/1000 <= 280 ) && ( row < snapshots.length) ) {
            FusionTables.Query.sql(sqlBase+snapshots[row][0]+"'");
            Utilities.sleep(2002);                                  // Limit the rate to <30 FusionTable queries per minute
            row++;
          }
          Logger.log("Deleted "+row.toString()+" crown records for former member '"+memList[mem][0]+"'.");
          if ( row >= snapshots.length ) {
            FusionTables.Query.sql("DELETE FROM "+utbl+" WHERE ROWID = '"+memList[mem][2]+"'");
            Logger.log("Deleted user '"+memList[mem][0]+"' from the Members table.");
            delMemberArray.push(memList[mem]);
          } else {
            skippedMems.push("\\n"+memList[mem][0].toString());
          }
        } else {
          skippedMems.push("\\n"+memList[mem][0].toString());
        }
      }
    }
    if (skippedMems.length > 0) {
      Browser.msgBox("Some Not Deleted","Of the input members, the following were not deleted due to lack of confirmation or insufficient time:"+skippedMems.toString(),Browser.Buttons.OK);
    }
    return delMemberArray;
  }
}
/**
 * maintenance script, run weekly/daily
 * finds all members with more than @maxRecords crown count snapshots, and trims them down to have at most 30 distinct LastSeen records.
 * will also trim out duplicated LastSeen records even if the user does not have 30+ records in total
 */
function doRecordsMaintenance(){
  var maxRecords = 10, startTime = new Date().getTime();
  var mem2Trim = getMembers2trim_(maxRecords);
  var mem = 0;
  while ( (mem<mem2Trim.length) && ((new Date().getTime()-startTime)/1000 < 250) ) {
    trimHistory_(mem2Trim[mem],startTime);
    mem++;
  }
}
/**
 *  function getMembers2trim_            Queries for a per-member count of crown data records, then queries those with > max records to
 *                                       determine an array of member, lastseen value, and times to delete that member-value pairing
 *  @param {Integer} maxDiffSeenRecords  The number of maximum different LastSeen crown snapshots a user can have
 *  @return {Array[][]}                  An array of [UID,LastSeen,n] where n is the number of times to delete the Member-LastSeen pair
 */
function getMembers2trim_(maxDiffSeenRecords){
  var sql1 = "SELECT UID, COUNT() FROM "+ftid+" GROUP BY UID";
  var resp = FusionTables.Query.sql(sql1);     // gets a count of all members and their total number of records
  var mem2Trim = [];
  // Scan through the total number of records to find members that have more than the allowed number of different LastSeen values
  for (var row=0;row<resp.rows.length;row++){
    if (resp.rows[row][1] > maxDiffSeenRecords) {
      mem2Trim.push(resp.rows[row][0].toString());
    }
  }
  if (mem2Trim.length > 0) {
    // Now have an array of UIDs belonging to members with more than the maximum number of records to be kept
    // Query again for only them, and add any LastSeen having more than 1 count, or any LastSeen after the
    // total max number allowed, to a deletion list, e.g. [UID, LastSeenVal, Num2Del]
    var sql2 = "SELECT UID, LastSeen, COUNT() FROM "+ftid+" WHERE UID IN ("+mem2Trim.toString()+") GROUP BY UID, LastSeen ORDER BY UID ASC";
    resp = FusionTables.Query.sql(sql2);
    var rows2Del = [];
    for (var row=0;row<resp.rows.length;row++){
      var mem = resp.rows[row][0].toString();
      var numDiffSeen = 0, memSeen = [];
      while (resp.rows[row][0].toString()==mem.toString()) { // each member is on multiple rows in resp.rows
        numDiffSeen++;
        var num2Del = numDiffSeen - maxDiffSeenRecords;
        if ((resp.rows[row][2] > 1) || (num2Del > 0)) {
          num2Del = Math.max(num2Del,resp.rows[row][2]-1);
          rows2Del.push([mem, resp.rows[row][1], num2Del]);
        }
        row++; // step to the next row
        if (row >= resp.rows.length) break;  // but make sure that's even possible
      }
      row--; // and drop back one row for the next user, due to the for loop's natural increment
    }
  }
  return rows2Del;
}
/**
 * function trimHistory_            For the specified UID-LastSeen pairs, it will delete num2Trim records,
 *                                  beginning with the oldest LastSeen values.
 * @param {Array} mem2TrimRow       A row from the array mem2Trim, containing [0]: UID, [1]: LastSeen, and
 *                                  [2]: integer of the # of pairs to delete
 * @param {Long} startTime          When the script began execution
 * @return {Integer}                How many rows were removed
 */
function trimHistory_(mem2TrimRow,startTime){
  var sqlBase = "DELETE FROM "+ftid+" WHERE ROWID = '";
  var sql = "", nRemoved = 0;
  var num2Trim = mem2TrimRow[2];
  if (num2Trim > 0) {
    // query for the rowids to delete and add them to the matrix rows2del, grabbing only the number of them to be trimmed
    sql = "SELECT ROWID, LastTouched FROM "+ftid+" WHERE UID = '"+mem2TrimRow[0]+"' AND LastSeen = "+mem2TrimRow[1].toString()+" ORDER BY LastTouched ASC LIMIT "+num2Trim;
    try{
      var resp = FusionTables.Query.sql(sql);
      var row = 0;
      while ( (row < resp.rows.length) && ((new Date().getTime()-startTime)/1000 < 250) ) {
        // loop over the returned rows and schedule their deletion (30 query/min max)
        sql = sqlBase+resp.rows[i][0].toString()+"'";
        nRemoved += FusionTables.Query.sql(sql).rows[0]*1;
        Utilities.sleep(2002);
        row++;
      }
    }
    catch(e){ throw new Error(e.message)}
  }
  return nRemoved;
}
/**
 * function getByteCount_   Computes the size in bytes of the passed string
 * @param  {String} str     The string to analyze
 * @return {Long}           The bytesize of the string, in bytes
 */
function getByteCount_(str){
  return Utilities.base64EncodeWebSafe(str).split(/%(?:u[0-9A-F]{2})?[0-9A-F]{2}|./).length-1;
}
/**
 * function val4CSV_        Inspect all elements of the array and ensure the values are strings
 * @param  {Object} value   the element of the array to be escaped for encapsulation into a string
 * @return {String}         A string representation of the passed element, with special character
 *                          escaping and double-quoting where needed
 */
function val4CSV_(value){
  var str = (typeof(value) === 'string') ? value : value.toString();
  if (str.indexOf(',') != -1 || str.indexOf("\n") != -1 || str.indexOf('"') != -1) {
    return '"'+str.replace(/"/g,'""')+'"';
  }
  else {
    return str;
  }
}
/**
 * function row4CSV_        Pass every element of the given row to a function to ensure the elements
 *                          are escaped strings and then join them into a CSV string with commas
 * @param  {Array} row      a 1-D array of elements which may be strings or other types
 * @return {String}         a string of the joined array elements
 */
function row4CSV_(row){
  return row.map(val4CSV_).join(",");
}
/**
 * function array2CSV_      Pass every row of the input array to a function that converts them into
 *                          escaped strings, join them with CRLF, and return the CSV string.
 * @param  {Array} myArr    a rectangular array to be converted into a string representing a CSV object.
 * @return {String}         A string representing the rows of the input array joined by newline characters
 */
function array2CSV_(myArr){
  return myArr.map(row4CSV_).join("\r\n");
}
/**
 * function getNewLastRanValue_     Determines the new lastRan parameter value based on the existing lastRan
 *                                  parameter, the original value of the lastRan user, and the slice of the
 *                                  memberlist near the old lastRan value ± number of changed rows
 * @param {String} origUID          The UID of the most recently updated member
 * @param {Long} origLastRan        The original value of lastRan prior to any memberlist updates
 * @param {Array} diffMembers       The member names that were added or deleted: [Member,UID]
 * @return {Long}                   The value of lastRan that ensures the next update will not skip/redo any
 *                                  preexisting member
 */
function getNewLastRanValue_(origUID,origLastRan,diffMembers){
  if ( origLastRan == 0 ) {
    return 0;
  } else {
    var newLastRan = -10000;
    differential = diffMembers.length;
    var newUserBatch = getUserBatch_(origLastRan-differential,2*differential+1);
    if ( newUserBatch.length > 0 ) {
      var newUIDs = newUserBatch.map(function(value,index){return value[1]});
      var diffIndex = newUIDs.indexOf(origUID);
      if ( diffIndex > -1 ) {
        newLastRan = origLastRan + (diffIndex-differential)
      } else {
        // very low likelihood code here (requires both deleting action, and lastRan points at one of the deleted members
        Logger.log("Exactly removed the lastRan member. Not changing lastRan, even if it causes issues (which it shouldn't).");
        if ( diffMembers.length == 1 ) {
          // Only removed 1 member, and that was the very next member in line for updating. Therefore, no change is needed.
          newLastRan = origLastRan
        } else {
          // Tough case here: we lost the exact point of reference needed for determining if deletions were before or after
          // where we've updated.

          // shortcut assumption, to be changed at a later date
          newLastRan = origLastRan
        /*
        // The lastRan member was one of those who was deleted.
        // 1) Concat newUserBatch and diffMembers
        // 2) Sort by Member (newUserBatch is already sorted)
        diffMembers.sort();
        // 3) Inspect to determine which elements of diffMembers fall within newUserBatch
        for (var i=0;i<diffMembers.length;i++){
          var dmVal = diffMembers[i][0];
          // The deleted members can come from all regions of the Memberlist, but newUserBatch is *very* selective.
          // Perform a lexical comparison to determine the first and last names that match into newUserBatch
          if ( dmVal > newUserBatch[0][0] ) {
            var startDiffMemberIndex = i;
            break;
          }
        }
        for (var i=diffMembers.length-1;i>0;i--) {
          var dmVal = diffMembers[i][0];
          if ( dmVal < newUserBatch[newUserBatch.length-1][0] ) {
            var endDiffMemberIndex = i;
            break;
          }
        }
        // 4) Determine the diffIndex
        if ( startDiffMemberIndex == endDiffMemberIndex ) {
          //
        } else if (false) {
          //
        } else {
          //
        }
        // 5) Compute newLastRan
        */
        }
      }
    } else {
      // lastRan is beyond the scope of the memberlist - e.g. pending scoreboard update.
      newLastRan = origLastRan;
      return newLastRan;
    }
    if ( newLastRan < 0 ) {
      return 0; // Start over entirely
    } else {
      return newLastRan;
    }
  }
}
