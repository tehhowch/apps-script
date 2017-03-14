/* Google FusionTables to serve as the backend
/ Structural changes: since updating/deleting rows is a 1-by-1 operation (yes, really >_> ), we will have multiple entries for each person in MHCC.
/ To prevent excessive table growth, we will remove rows that were uploaded more than 4 weeks earlier.
/ Bonus: this means we can serve new data such as most crowns in the last 30 days
*/
var utbl = '1O4lBLsuvfEzmtySkJ-n-GjOSD_Ab2GsVgNIX8uZ-' // small table containing user names and user ids (unique on user ids)
var ftid = '1hGdCWlLjBbbd-WW1y32I2e-fFqudDgE3ev-skAff' // large table containing names, user ids, and crown counts data (no unique key!)
/**
/ returns the member and uid as an rectangular array
/ [name , UID]
**/
function getUserBatch_(LastRan,BatchSize){
  var sql = "SELECT Member, UID FROM " + utbl + " ORDER BY Member ASC OFFSET " + LastRan + " LIMIT " + BatchSize
  var minitable = FusionTables.Query.sql(sql);
  return minitable.rows;
}
/**
/ called by UpdateScoreboard to get a unique listing of hunters and crown data - only reporting the result of the latest touch per hunter
/ returns an UNSORTED and NON-RANKED array of the most recent stored crown data
**/
function getLatestRows_(nMembers){
  var sql = '', resp="", wt = new Date();
  sql = "SELECT * FROM " + ftid + " ORDER BY LastTouched DESC LIMIT " + nMembers
  resp = FusionTables.Query.sql(sql);
  var sbd = resp.rows
  return sbd;
}
/**
/ called by new code that looks into a specific user and reports their crown growth total as a function of LastSeen times
/ LastSeen used instead of LastTouched since database maintenance is more often than users get Seen
/ could be called from a webapp that gets the hunters uid as a parameter, does a lookup to the table, and serves a line
/ chart of progress over various days
/ look into using this sample reference instead: https://developers.google.com/fusiontables/docs/sample_code#chartTools
**/
function getUserHistory_(uid){
  var sql = "SELECT Member, LastSeen, Bronze, Silver, Gold, MHCC FROM "+ftid+" ORDER BY LastSeen ASC WHERE UID = '"+uid.toString()+"' LIMIT 30";
  var resp = FusionTables.Query.sql(sql);
  if (resp.rows.length > 0) {
    var columns = resp.columns;
    var data = resp.rows;
    var history = {"user":resp.rows[0][0],"headers":columns,"dataset":data};
    return history;
  }
  return ""
}
/**
/ called by UpdateDatabase to write grabbed data back to the database after several batches of hunters were updated
**/
function batchwrite(hdata){
  // hdata[][] [Member][UID][Seen][Crown][Touch][br][si][go][si+go][squirrel]
  var sqlbase = "INSERT INTO " + ftid + " (Member, UID, LastSeen, LastCrown, LastTouched, Bronze, Silver, Gold, MHCC, Squirrel) VALUES ('";
  var query = "", numadded=0, resp="";

  while (hdata.length > 0) {         // while there are non-inserted users
    for (var i=0;i<500;i++) {        // max 500 INSERT per query
      if (hdata.length > 0) {        // We might have less than 500 to add
        var user = hdata.pop();
        query += sqlbase + user[0] + "', '" + user[1] + "', " + user[2] + ", " + user[3] + ", " + user[4]
        query += ", " + user[5] + ", " + user[6] + ", " + user[7] + ", " + user[8] + ", '" + user[9] +"');";
      } else {
        break;                         // exit the for loop early
      }
    }                                  // prepped a query to execute
    resp[0] = FusionTables.Query.sql(query);
    numadded+=resp.rows.length*1;   // track how many new rows were added and give back to the parent
    query = "";                        // reset the query for the next trip through
  }
  return numadded;
}
/**
/ scans the fusion table member list to determine if anyone with the given UID exists already.
/ if yes, then informs the adder that the member is already in the database.
/ if no, adds the member to a list and prompts for additional members to add
/ Finally, if members remain that should be added, calls addMember2fusion_() with the member list
/
**/
function addFusionMember(){
    var getMembers = true, gotDb=false, hasMatch = true
    var mem2add = [], strUIDmatch="", curmems=[], matchedIndex = -1;
    while getMembers {
        var name = String(getMemberName2AddorDel_());
        var UID = "";
        if (name == -1) {
            getMembers = false;      // adder canceled the add
        } else {                     // adder gave a valid name and confirmed it
            UID = String(getMemberUID2AddorDel_(name));
            if (UID == '-1') {
                getMembers = false;  // adder canceled the add
            } else {                 // have a confirmed name & UID pair
                if (gotDb==false) {  // need to get the existing database records
                    curmems = FusionTables.Query.sql("SELECT Member, UID, ROWID FROM "+utbl+).rows;
                    var curMemNames = curmems.map(function(value,index){return value[0]});  // member names are the 1st column in curmems array
                    var curUIDs = curmems.map(function(value,index){return value[1]});      // UIDs are the 2nd column in curmems array
                    var curRowIDs = curmems.map(function(value,index){return value[2]});    // used if a name update is wanted
                    gotDb=true;
                }
                matchedIndex = curUIDs.indexOf(UID);  // check elements of the array to see if any element is the UID
                hasMatch = ( matchedIndex >= 0 ) ? true :false;
                if (hasMatch) {
                  // user matched to someone already in the database
                  var resp = Browser.msgBox("Oops!","New member "+name+" is already in the database as "+curMemNames[matchedIndex]+".\n Would you like to update their name to "+name+"?",Browser.Buttons.YES_NO);
                  if (resp.toLowerCase()=="yes") { // Perform a name change (only valid for those who are already in the db)
                    try {FusionTables.Query.sql("UPDATE "+utbl+"SET Member = '"+name+"' WHERE ROWID ="curRowIDs[matchedIndex]);}
                    catch(e) {Browser.msgBox("Sorry, couldn't update "+curMemNames[matchedIndex]+"'s name.\n Perhaps it isn't in the database yet?");}
                  }
                } else {
                  // we have a new member! update our search arrays too
                    mem2add.push([name,UID])
                    curUIDs.push(UID);
                    curMemNames.push(name);
                }

            }
        }
        var resp = Browser.msgBox("Add more?","Would you like to add another member?",Browser.Buttons.YES_NO);
        if (resp.toLowerCase()=="no") getMembers = false;
    }
    // perhaps we've now got a list of only new members to add to the database
    if (mem2add.length > 0) {
        var n = addMember2fusion_(mem2add);
        SpreadsheetApp.getActiveSpreadsheet().toast("Successfully added "+n+" new member(s) to the MHCC Member Crown Database")
        PropertiesService.getScriptProperties().setProperty('numMembers',curUIDs.length.toString()); // saves a query against the table for a necessary property
    }
}
/**
/  prompt the adder for the names / profile links of the members who should be removed from the crown database
**/
function delFusionMember(){

}
/**
/ helper function that gets a name from the spreadsheet main interface
**/
function getMemberName2AddorDel_(){
    var name = Browser.inputBox("Adding a Member","Enter the new member's name",Browser.Buttons.OK_CANCEL);
    if (name.toLowerCase()=="cancel") return -1
    var ok2add = Browser.msgBox("Verify name","Please confirm that the member is named '"+name+"'",Browser.Buttons.YES_NO)
    if (ok2add.toLowerCase()=="yes") {
        return String(name).trim()
    } else {
        // recurse until canceled or accepted
        return getMemberName2Add_()
    }
}
/**
/ helper function that ensures the provided UID belongs to who the adder thinks it does
**/
function getMemberUID2AddorDel_(memberName){
    var profileLink = Browser.inputBox("What is " +memberName +"'s Profile Link?","Please enter the URL of "+memberName+"'s profile",Browser.Buttons.OK_CANCEL);
    if (profileLink.toLowerCase()=="cancel") return -1;
    var UID = profileLink.slice(mplink.search("=")+1).toString();
    var ok2add = Browser.msgBox("Verify URL","Please confirm that this is "+memberName+"'s Profile:\n"+" http://apps.facebook.com/mousehunt/profile.php?snuid="+UID,Browser.Buttons.YES_NO);
    if (ok2add.toLowerCase()=="yes") {
        return String(UID).trim();
    } else {
        // recurse until canceled or accepted
        return getMemberUID2Add_(memberName)
    }
}
/**
/ inserts the given members into the members db (dup checking done by parent)
/ inserts a blank crown record into the crown database with their UID(s)
/ @param Array[][] memlist  - [[Name1, UID1], [Name2,UID2],...]
**/
function addMember2fusion_(memlist){
  var usql = 'INSERT INTO ' + utbl + " (Member, UID) VALUES ('";
  var csql = 'INSERT INTO ' + ftid + " (Member, UID, LastSeen, LastCrown, LastTouched, Bronze, Silver, Gold, MHCC, Squirrel) VALUES ('";
  var query1 = '', query2 = '', rt = new Date(), resp = [], numadded=0;
  while (memlist.length > 0) {         // while there are non-inserted users
    for (var i=0;i<500;i++) {          // max 500 INSERT per query
      if (memlist.length > 0) {        // We might have less than 500 to add
        var user = memlist.pop();
        query1 += usql + user[0] + "', '" + user[1] + "');";
        query2 += csql + user[0] + "', '" + user[1] + "', 0, " + rt.getTime() + ", " + rt.getTime() + ", 0, 0, 0, 0, 'Weasel');";
      } else {
        break;                         // exit the for loop early
      }
    }                                  // prepped a query to execute
    resp[0] = FusionTables.Query.sql(query1);
    resp[1] = FusionTables.Query.sql(query2);
    numadded+=resp[0].rows.length*1;   // track how many new rows were added and give back to the parent
    query1 = '';query2 = '';           // reset the query for the next trip through
  }
  return numadded;
}
/**
/ @param memlist - array containing the name of the member(s) to delete from the members and crown-count dbs
**/
function delMember_(memlist){
  var sql = "SELECT ROWID, UID, Member FROM " + utbl + " WHERE Member IN " + memlist.toString();
  var users = FusionTables.Query.sql(sql).rows;
  var skippedMems = [];
  Logger.log("Will be deleting " + users.length + " rows from Member table")
  for (var i=0;i<users.length;i++){
    sql = "SELECT ROWID, UID FROM " + ftid + " WHERE UID = " + users[i][1]
    var snapshots = FusionTables.Query.sql(sql).rows;
    var resp = Browser.msgBox("Confirmation Required","User "+users[i][3]+" has " +snapshots.length+" records. Delete?",Browser.Buttons.YES_NO);
    if (resp.toLowerCase()=="yes") {
      var sqlbase = "DELETE * FROM " + ftid + " WHERE ROWID = ";
      for (var j=0;j<snapshots.length;j++){
          FusionTables.Query.sql(sqlbase+snapshots[j][0]);
      }
      Logger.log("Deleted "+snapshots.length+" crown records for former member "+users[i][3]);
      FusionTables.Query.sql("DELETE * FROM "+utbl+" WHERE ROWID = "+users[i][0]);
      Logger.log("Deleted user "+users[i][3]+" from the Members table.");
    } else {
      skippedMems.push(users[i][3].toString()+"\n");
    }
  }
  if (skippedMems.length > 0) {
    Browser.msgBox("Some Not Deleted","Of the input members, the following were not deleted due to lack of confirmation\n "+skippedMems.toString,Browser.Buttons.OK);
  }
}
/**
/ maintenance script, run weekly/daily
/ finds all members with more than @maxRecords crown count snapshots, and trims them down to have at most 30 distinct LastSeen records.
/ will also trim out duplicated LastSeen records even if the user does not have 30+ records in total
**/
function doRecordsMaintenance(){
  var maxRecords = 30;
  var mem2trim = getMembers2trim_(maxRecords);
  for (var i=0;i<mem2trim.length;i++) {
    trimHistory_(mem2trim[i]);
  }
}
/**
/    queries for a per-member count of crown data records, then queries those with > max records to determine
/    an array of member, lastseen value, and times to delete that member-value pairing
/  @param Integer maxDiffSeenRecords  - the number of maximum different LastSeen crown snapshots a user can have
/  @return Array[][] - an array of [UID,LastSeen,n] where n is the number of times to delete the Member-LastSeen pair
**/
function getMembers2trim(maxDiffSeenRecords){
  var sql1 = "SELECT UID, COUNT() FROM " + ftid + " GROUP BY UID";
  var sql2 = "SELECT UID, LastSeen, COUNT() FROM " + ftid + " GROUP BY UID, LastSeen ORDER BY UID ASC";
  var resp = FusionTables.Query.sql(sql1) // gets a count of all members and their total number of records
  var mem2trim = [];
  // Scan through the total number of records to find members that have more than the allowed number of different
  // LastSeen values
  for (var i=0;i<resp.rows.length;i++){
    if (resp.rows[i][1] > maxDiffSeenRecords) {
      mem2trim.push(resp.rows[i][0].toString());
    }
  }
  // Now have an array of UIDs belonging to members with more than the maximum number of records to be kept
  // Query again for only them, and add any LastSeen having more than 1 count, or any LastSeen after the
  // total max number allowed, to a deletion list, e.g. [UID, LastSeenVal, Num2Del]
  sql2 += " WHERE UID IN " + mem2trim.toString();
  resp = FusionTables.Query.sql(sql2);
  mem2trim = [];
  for (var i=0;i<resp.rows.length;i++){
    var mem = resp.rows[i][0].toString();
    var numDiffSeen = 0, memSeen = [];
    while (resp.rows[i][0].toString()==mem.toString()) { // each member is on multiple rows in resp.rows
      numDiffSeen++;
      var num2del = numDiffSeen - maxDiffSeenRecords;
      if ((resp.rows[i][2] > 1) || (num2del > 0)) {
        num2del = Math.max(num2del,resp.rows[i][2]-1);
        mem2trim.push([mem, resp.rows[i][1], num2del]);
      }
      i++; // step to the next row
      if (i >= resp.rows.length) break;  // but make sure that's even possible
    }
    i--; // and drop back one row for the next user, due to the for loop's natural increment
  }
  Logger.log(mem2trim.length)
  Logger.log(mem2trim[0])
  return mem2trim
}
/**
/   for the specified UID-LastSeen pairs, it will delete num2trim records, beginning with the oldest LastSeen values
/ @param Array[] mem2trimRow  - a row from the array mem2trim, containing [0]: UID, [1]: LastSeen, and [2]: integer of the # of pairs to delete
**/
function trimHistory_(mem2trimRow){
  var sqlbase = "DELETE FROM " + ftid + " WHERE ROWID = ";
  var sql = "", r = 0
  var num2trim = mem2trimRow[2];
  if (num2trim > 0) {
    // query for the rowids to delete and add them to the matrix rows2del, grabbing only the number of them to be trimmed
    sql = "SELECT ROWID, LastTouched WHERE UID = '" + mem2trimRow[0] + "' AND LastSeen = " + mem2trimRow[1].toString() + " ORDER BY LastTouched ASC LIMIT "+num2trim
    var resp = FusionTables.Query.sql(sql);
    for (var i=0;i<resp.rows.length;i++) {
      // loop over the returned rows and schedule their deletion
      sql = sqlbase+resp.rows[i][0].toString();
      r = FusionTables.Query.sql(sql)
      Logger.log(r);
    }
  }
  return r;
}
