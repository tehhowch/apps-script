/**
**  This spreadsheet uses Google Apps Script, Spreadsheets API, and the "experimental" FusionTables API to maintain a record of all MHCC members that opted into crown
**  tracking. Via the UpdateDatabase script and the external Horntracker.com website, the crowns of all members can be updated in an
**  automated fashion. These members are processed in chunks of up to 127 at a time (higher batch sizes overload the maximum URL length), with
**  an unspecified number of batches processed per execution.  The script will update as many batches as it can without exceeding a specified execution
**  time, in order to avoid triggering the Maximum Execution Time Exceeded error (at 300 seconds).
**
  Tracking Progress
**
**  As the unspecified and unresolved issue with a range-specific paste rendered the previous method unworkable, the move to host updates on FusionTables effectively
**  hides the progress of updates from those with access to this workbook. SheetDb now will only ever hold the same data as that used to construct the currently-visibile
**  Scoreboard page, but sorted alphabetically. Whoever has set up the triggered events will likely receive daily emails about failed executions of the script, due to
**  timeouts from Horntracker or Service Errors from Google (and maybe daily/hourly Quota overruns from FusionTables)
**  If you wish to know the current status of the update, you can view the LastRan parameter via File -> Project Properties -> Project Properties

  Forcing a Scoreboard Update
**
**  If you must update the scoreboard immediately, you can manually run the UpdateScoreboard function via "Run" -> "UpdateScoreboard" here, or use the 'Administration'
**  tab on from the spreadsheet. Doing so will commit the current state of the member list to the scoreboard sheet. This will also reset the LastRan parameter, 
**  effectively restarting the update cycle. It may also trigger the FusionTable maintenance script, which trims out old or duplicated records from the crown database.

  Forcing a Restart (Getting the database to restart crown updates instead of continuing)
**
**  This is easiest done by forcing a scoreboard update, but can also be achieved by editing the LastRan parameter.
**  Click "File" -> "Project Properties" -> "Project Properties", and you should now see a table of fields and values.  Click the current value for LastRan (e.g. 2364)
**  and replace it with 0.  Click "Save" to commit your change.

*/
var mhccSSkey = '1P8UDv4j2lPM0hAKw4EbBT_GtvlOgFYeARV16NzWA6pc';
function onOpen(){
  SpreadsheetApp.getActiveSpreadsheet().addMenu('Administration', [{name:"Add Members",functionName:"addFusionMember"},
                                                                   {name:"Delete Members",functionName:"delFusionMember"},
                                                                   {name:"Refresh Scoreboard",functionName:"UpdateScoreboard"},
                                                                   {name:"Perform Crown Update",functionName:"UpdateDatabase"},
                                                                   {name:"Perform RecordCount Maintenance",functionName:"doRecordsMaintenance"}]);
}
function getMyDb_(wb,sortObj) {
  var SS = wb.getSheetByName('SheetDb');
  var db = SS.getRange(2, 1, SS.getLastRow()-1, SS.getLastColumn()).sort(sortObj).getValues();
  return db;
}
function saveMyDb_(wb,db) {
  if ( db == null ) return 1;
  var lock = LockService.getPublicLock();
  lock.tryLock(30000);
  if ( lock.hasLock() ) {
    // Have a lock on the db, now save
    var SS = wb.getSheetByName('SheetDb');
 //   if ( range == null) {
      // No position input -> full db write -> no sorting needed
    if ( db.length < SS.getLastRow()-1 ) {
      // new db is smaller than old db, so clear it out
      SS.getRange(2, 1, SS.getLastRow(), SS.getLastColumn()).setValue('');
    }
    SS.getRange(2, 1, db.length, db[0].length).setValues(db).sort(1);
//    } else {
//     // supplied position to save to, e.g. minidb save -> alphabetical sort required before saving
//      SS.getRange(2, 1, SS.getLastRow(), SS.getLastColumn()).sort(1);
//      SS.getRange(range[0],range[1],range[2],range[3]).setValues(db);
//    }
    SpreadsheetApp.flush(); // Commit changes (causing immediate failure as of 2017-02-21 for minidb writes
    lock.releaseLock();
    return true
  }
  return false
}
/**function AddMemberToDB_(wb,dbList) {
  // This function compares the list of members on the Members page to the received list of members (from the SheetDb page).  Any members missing are added.
  if ( dbList == null ) return 1;
  // MemberRange = [[Name, MH Rank, Profile Link],[Name2, MHRank2, ProfLink2],...]
  var memSS = wb.getSheetByName('Members');
  var memList = memSS.getRange(2, 1, memSS.getLastRow()-1, 3).sort(1).getValues();  // Get an alphabetically sorted list of members
  if ( memList.length == 0 ) return 1;
  for ( var i = 0; i<dbList.length; i++ ) {
    // Loop over all names on the database list
    var dbID = dbList[i][1].toString();
    // Check against an ever-shortening list (derived from the Members sheet)
    for ( var j = 0; j<memList.length; j++ ) {
      var mplink = memList[j][2];
      var mpID = mplink.slice(mplink.search("=")+1).toString();
      if ( mpID === dbID ) {
        memList.splice(j,1);
        break;
      }
    }
  }
  // After memList is only the new items to add to SheetDb, this runs:
  var UID = '';
  for (i = 0;i<memList.length;i++) {
    UID=memList[i][2].slice(memList[i][2].search("=")-0+1).toString();
    dbList.push([memList[i][0],
                 UID,
                 'http://apps.facebook.com/mousehunt/profile.php?snuid='+UID,
                 0, // LastSeen
                 0, // LastCrown
                 0, // LastTouched
                 0, // Bronze
                 0, // Silver
                 0, // Gold
                 0, // MHCC Crowns
                 dbList.length-0+1, // Rank
                 'Weasel', // Squirrel
                 'Old'  // Status
                ]);
  }
  saveMyDb_(wb,dbList)  // write the new db
  return 0;
}**/
function UpdateDatabase() {
  // This function is used to update the database's values, and runs frequently on small sets of data
  var BatchSize = 127;                                                        // Number of records to process on each execution
  var db = [];
  var wb = SpreadsheetApp.openById(mhccSSkey);
  // db = getMyDb_(wb,1);             // Get the db, sort on col 1 (name)
  var props = PropertiesService.getScriptProperties().getProperties();
  var nMembers = props.nMembers;                                              // Database records count
  var LastRan = props.LastRan*1;                                              // Determine the last successfully processed record
  var sheet = wb.getSheetByName('Members');
  // var nRows = sheet.getLastRow()-1;                                                     // Spreadsheet records count
  // Read in the MHCC tiers as a 13x3 array
  // If the MHCC tiers are moved, this getRange MUST be updated!
  var aRankTitle = sheet.getRange(3, 8, 13, 3).getValues();

  if ( LastRan >= nMembers ) {                                                
    // Perform scoreboard update check / progress reset
    UpdateScoreboard();
    PropertiesService.getScriptProperties().setProperty('LastRan', 0);        // Point the script back to the start
  } else {
      // Grab a subset of the alphabetized member record
      var lock = LockService.getPublicLock();
      lock.waitLock(30000);
      if ( lock.hasLock() ) {
        var starttime = new Date().getTime();
        Logger.log('Started with '+LastRan+' completed member updates');
        // Loop over members in sets of BatchSize. Stop looping when we've updated all members, or exceeded 180s of runtime.
        while ( ((new Date().getTime()) - starttime)/1000 < 180 && (LastRan < nMembers)) {
          var btime = new Date().getTime();
          var dHunters = getUserBatch_(LastRan,BatchSize); // Queries for the next set of members to update
          var sIDstring = dHunters[0][1].toString();       // Get the first ID for the string
          for (var i=1;i<dHunters.length;i++ ) {
            if ( dHunters[i][1] != '' ) {
              sIDstring += ","+dHunters[i][1].toString();  // Concatenate all the remaining non-null IDs
            }
          }
          // Have built the ID string, now query HT's MostMice.php
          var MM = UrlFetchApp.fetch('http://horntracker.com/backend/mostmice.php?function=hunters&hunters='+sIDstring).getContentText();
          MM = JSON.parse(MM); // Separate line for debug purposes
          // Loop over our db subset dHunters and parse the corresponding MM entry
          Logger.log(Object.keys(MM.hunters).length+' returned hunters out of '+dHunters.length)
          for (var i=0;i<dHunters.length;i++) {
            var j = 'ht_'+dHunters[i][1];
            if ( typeof MM.hunters[j] != 'undefined' ) {
              // The hunter's ID was found in the MostMice object, and the update can be performed
              var nB = 0, nS = 0, nG = 0;
              for ( var k in MM.hunters[j].mice ) {
                // Assign crowns by summing over all mice
                if ( MM.hunters[j].mice[k] >= 500 ) nG++;
                else if ( MM.hunters[j].mice[k] >= 100 ) nS++;
                else if ( MM.hunters[j].mice[k] >= 10 ) nB++;
              }
              dHunters[i][2] = Date.parse((MM.hunters[j].lst).replace(/-/g,"/"));
              if ( dHunters[i][7] != nG || dHunters[i][6] != nS || dHunters[i][5] != nB ) dHunters[i][3] = new Date().getTime();
              dHunters[i][4] = new Date().getTime();    // Time of last update, the 'touched' value
              dHunters[i][5] = nB                       // Bronze
              dHunters[i][6] = nS                       // Silver
              dHunters[i][7] = nG                       // Gold
              dHunters[i][8] = nG-0 + nS-0;             // MHCC Crowns
              // Determine the MHCC rank & squirrel of this hunter
              for ( var k = 0; k<aRankTitle.length; k++ ) {
                if ( dHunters[i][8] >= aRankTitle[k][0] ) {
                  // Crown count meets/exceeds required crowns for this level
                  dHunters[i][9] = aRankTitle[k][2];    // Set the Squirrel value
                  break;
                }
              }
              // if ( dHunters[i][2] >= (new Date().getTime() - 20*86400*1000) ) dHunters[i][12] = 'Current';
              // else dHunters[i][12] = 'Old';  // move the old/lost characterization to UpdateScoreboard
            } else {
              // The hunter is not found in the MM object, he is lost.  Set his status to "Lost"
              // dHunters[i][12] = 'Lost';  // move to UpdateScoreboard
            }
          }
          // Have now completed the loop over the dHunters subset.  Save the new rows to the db
          ftBatchWrite(dhunters);//  saveMyDb_(wb,dHunters,[2+LastRan-0,1,dHunters.length,dHunters[0].length]);
          LastRan = LastRan-0 + BatchSize-0;            // Increment LastRan for next batch's usage
          PropertiesService.getScriptProperties().setProperty('LastRan',LastRan.toString());
          Logger.log('Batch time of '+((new Date().getTime())-btime)/1000+' sec');
        }
        Logger.log('Through '+LastRan+' hunters, script time '+((new Date().getTime())-starttime)/1000+' sec');
        lock.releaseLock();
      }
  }
}
/**
/ function UpdateStale: 
/           Writes to the secondary page that serves as a "Help Wanted" ad for getting updates for oft-unvisited members 
/ @param lostTime Integer  - the number of milliseconds fter which a member is considered "in dire need of revisiting"
/
**/
function UpdateStale_(lostTime) {
  var lock = LockService.getPublicLock();
  lock.waitLock(30000);
  if ( lock.hasLock() ) {
    var wb = SpreadsheetApp.openById(mhccSSkey);
    var dbSheet = wb.getSheetByName('SheetDb');
    var db = getMyDb_(wb,4);                        // Sorts the most recently written snapshot of crown data by the date of last MostMice inspection, in ascending order
    var lostSheet = wb.getSheetByName('"Lost" Hunters');
    var StaleArray = [];
    var starttime = new Date().getTime();
    for (var i = 0;i<db.length;i++ ) {
        if ( starttime - db[i][3] > lostTime ) {
            StaleArray.push([db[i][0], Utilities.formatDate(new Date(db[i][3]), 'EST', 'yyyy-MM-dd'),"https://apps.facebook.com/mousehunt/profile.php?snuid="+db[i][1],"https://www.mousehuntgame.com/profile.php?snuid="+db[i][1]]);
        }
    }
    // Write the new Stale Hunters to the sheet
    lostSheet.getRange(3,3,Math.max(lostSheet.getLastRow()-2,1),4).setValue('');  // Remove old Stale hunters
    if (StaleArray.length > 0) {
      lostSheet.getRange(3,3,StaleArray.length,4).setValues(StaleArray); // Add new Stale hunters
    }
    lock.releaseLock();
  }
}
/**
/ function UpdateScoreboard:
/           Write the most recent snapshot of each member's crowns to the Scoreboard page 
/
**/
function UpdateScoreboard() {
  UpdateStale(20*86400*1000);                   // If a member hasn't been seen in the last 20 days, then request a high-priority update
  var start = new Date().getTime();
  var wb = SpreadsheetApp.openById(mhccSSkey);
  var props = PropertiesService.getScriptProperties().getProperties();
  var nMembers = props.nMembers||0;
  if (nMembers == 0) {
      nMembers = FusionTables.Query.sql("SELECT * FROM "+utbl).rows.length;
      PropertiesService.getScriptProperties().setProperty("nMembers",nMembers.toString());
  }
  // To build the scoreboard....
  // 1) Request the most recent snapshot of all members
  var db = getLatestRows_(nMembers);
  // 2) Store it on SheetDb
  saveMyDb_(wb,db);
  // 3) Sort it by MHCC crowns, then Golds, then LastSeen, then LastCrown: {column:10,ascending:false},{column:9,ascending:false},{column:8,ascending:false},{column:7,ascending:false}]);
  var AllHunters = getMyDb_(wb,[{column:10,ascending:false},{column:5,ascending:true},{column:4,ascending:true}]);
  var Scoreboard = [];
  var i = 1;
  // Scoreboard format:   i UpdateDate CrownChangeDate Squirrel MHCCCrowns Name Profile
  while ( i <= AllHunters.length ) {
    Scoreboard.push([i,
                     Utilities.formatDate(new Date(AllHunters[i-1][3]), 'EST', 'yyyy-MM-dd'), // Last Seen
                     Utilities.formatDate(new Date(AllHunters[i-1][4]), 'EST', 'yyyy-MM-dd'), // Last Crown
                     AllHunters[i-1][11], // Squirrel
                     AllHunters[i-1][9],  // #MHCC Crowns
                     AllHunters[i-1][0],  // Name
                     AllHunters[i-1][2]   // Profile Link (fb)
                    ])
    if ( i%150 == 0 ) Scoreboard.push(['Rank','Last Seen','Last Crown','Squirrel Rank','G+S Crowns','Hunter','Profile Link'] )
    AllHunters[i-1][10]=i++;  // Store the hunter's rank in the db listing
  }
  saveMyDb_(wb,AllHunters);    // Store & alphabetize the new ranks
  // Clear out old scoreboard data
  var SS = wb.getSheetByName('Scoreboard');
  SS.getRange(2, 1, SS.getLastRow(), Scoreboard[0].length).setValue('');

  // Write new data
  SS.getRange(2, 1, Scoreboard.length, Scoreboard[0].length).setValues(Scoreboard);

  // Force full write before returning
  SpreadsheetApp.flush();
  wb.getSheetByName('Members').getRange('I23').setValue(((new Date())-wb.getSheetByName('Members').getRange('H23').getValue())/(24*60*60*1000));
  wb.getSheetByName('Members').getRange('H23').setValue(new Date());
  Logger.log((new Date().getTime() - start)/1000 + ' sec for scoreboard operations');
}
function ReverseMemberFind() {
  // Used to determine which members are on the scoreboard but not in the Member list
  var SSwb = SpreadsheetApp.openById(mhccSSkey);
  var SBList = getMyDb_(SSwb,1);  // [[Name1, Link1],[Name2, Link2]]
  var GoneNotYet = [];
  SS = SSwb.getSheetByName('Members');
  var MemberList = SS.getRange(2, 1, SS.getLastRow()-1, 3).sort(1).getValues(); // [[Name1, Rank1, Link1],[Name2 ... ]]
  for ( var i = 0; i<SBList.length; i++ ) {
    // Loop over all names on the scoreboard list
    var splink = SBList[i][2];
    var spID = splink.slice(splink.search("=")+1).toString();
    var hasmatch = false;
    for ( var j = 0; j<MemberList.length; j++ ) {
      // Loop over all names on the member list
      var mplink = MemberList[j][2];
      var mpID = mplink.slice(mplink.search("=")+1).toString();
      if ( mpID === spID ) {
        hasmatch = true;
        MemberList.splice(j,1);
        break;
      }
    }
    if ( !hasmatch ) GoneNotYet.push([SBList[i][0],SBList[i][2]])
  }
  SS.getRange(66, 7, 100, 2).setValue('');
  if ( GoneNotYet.length > 0 ) {
    SS.getRange(66, 7, GoneNotYet.length, 2).setValues(GoneNotYet);
  }
}
function rewriteSDB_(){
  var wb = SpreadsheetApp.getActiveSpreadsheet();
  var s = wb.getSheetByName('SheetDb');
  var db = getMyDb_(wb,1);
//  s.deleteRows(2, db.length)
//  s.insertRowAfter(1);
  s.setName('oldSDB');
  s=wb.insertSheet('SheetDb',wb.getSheets().length);
  s.getRange(1, 1, 1, 13).setValues([['Member','UID','Profile','LSeen','LCrown','LUpdate','Br','Si','Go','MHCC','Rank','Squirrel','Status']]);
  saveMyDb_(wb,db)
}
