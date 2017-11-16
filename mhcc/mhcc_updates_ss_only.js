/***  
**  This spreadsheet uses Google Apps Script and the Spreadsheets API functionality to maintain a record of all MHCC members placed on the
**  'Members' sheet.  Via the UpdateDatabase script and the external Horntracker.com website, the crowns of all members can be updated in an 
**  automated fashion.  These members are processed in chunks of up to 127 at a time (higher batch sizes overload the maximum URL length), with
**  an unspecified number of batches processed per execution.  The script will update as many batches as it can without exceeding a specified execution
**  time (default 30 seconds), in order to avoid triggering the Maximum Execution Time Exceeded error (at 300 seconds).
**
**  "Lost" hunters belong to at least one of two groups: MHCC members who have never been seen by anyone with the MostMice tracking tool enabled, and/or 
**  MHCC members who, for some reason, have not been returned in MostMice queries within a set timeframe (chooseable by you, currently set at about 10 hours)
**  "Stale" hunters are those who have not been seen recently by someone with the MostMice tracking tool enabled (e.g. their profile exists, but the data in it is old).
**  For Stale hunters and the first group of Lost hunters, the issues associated with their member records can be easily amended by anyone with the MostMice tracking tool enabled,
**  and as such, their profile links are placed on a sheet for all MHCC members to see.  
 
  Tracking Progress
**
**  Whoever has set up the triggered events will likely receive daily emails about failed executions of the script, due likely to timeouts from Horntracker 
**  (during its maintenance period, or high load).  If you wish to know the current status of the update, you can unhide the SheetDb worksheet, and scroll down  
**  the LastTouched column, which is the last time the UpdateDatabase script manipulated data in that row (measured in milliseconds).  You can alternately view
**  the LastRan parameter via File -> Project Properties -> Project Properties
 
  Forcing a Scoreboard Update
**
**  If you must update the scoreboard immediately, you can manually run the UpdateScoreboard function via "Run" -> "UpdateScoreboard"  Doing so will commit 
**  the current state of the member list to the scoreboard sheet, and may generate a significant number of "Lost" hunters for the sole reason that they hadn't 
**  been updated yet this cycle.  This will not reset the current progress of the database update script.  The scoreboard normally will update about every 2 hours,
**  so a forced update is rather unlikely to be necessary.
 
  Forcing a Restart (Getting the database to restart crown updates instead of continuing)
**
**  If the requirements for a title are changed, the true status for each member will not be reflected until the scoreboard has updated twice.  This is 
**  because any hunters processed before the change are not re-processed upon changing of the titles / number of mice.  To restart the database update 
**  process from square 1, you must edit the LastRan parameter.  Click "File" -> "Project Properties…" -> "Project Properties", and you should now see
**  a table of fields and values.  Click the current value for LastRan (e.g. 2364) and replace it with 0.  Click "Save" to commit your change.  You should
**  verify that your change was accepted, and this can be done by waiting 15 minutes.  View the LastRan parameter after 10 minutes, and if it is greater 
**  than 300 then your first edit was performed while the script was running, and promptly overwritten when the script finished.  Repeat this cycle of 
**  waiting 15 minutes and checking the LastRan value until it is apparent that the script has accepted the new value.

  Function list
**
**  AddMemberToDB:  Two inputs, called from UpdateDatabase whenever the number of member rows on 'Members' is greater than the number of members in the 
**                  hidden database.  It will scan over the entire member list in order to determine which members are new, and will add them.
**
**  UpdateDatabase:  This is the main function.  It should be called by a time-based trigger, running either every 5 or 10 minutes.  Refer to inline comments 
**                   for details of how the script runs; in general it retrieves a subset of MHCC members, asks Horntracker for their mouse catchess, and then
**                   adds up catches, assigns MHCC squirrel, determines whether or not the number of current crowns is different than the number of 
**                   stored crowns, and touches the record so that it is known whether or not the record is up-to-date.  These changes are then saved back to
**                   the database.  When new member-info rows are detected, this script will partner up with AddMemberToDB to add these members to the fold.
**
**  UpdateScoreboard:  This function manages the updates for the Lost hunters, Stale hunters, and Scoreboard. It does not update any data in the ScriptDb
**                     database.  If alternate/other information is desired on the Scoreboard, the relevant object properties (e.g. hunter.____ ) must be 
**                     added to the Scoreboard array created.  This is a simple change to make, as the write functions automatically determine the size.


*/           //  1P8UDv4j2lPM0hAKw4EbBT_GtvlOgFYeARV16NzWA6pc
var mhccSSkey = '1P8UDv4j2lPM0hAKw4EbBT_GtvlOgFYeARV16NzWA6pc';
function onOpen(){
  SpreadsheetApp.getActiveSpreadsheet().addMenu('MHCC', [{name:"Request New Update",functionName:"UpdateDatabase"},
                                                         {name:"Refresh Scoreboard",functionName:"UpdateScoreboard"}]);
//                                                         {name:"Repair SheetDb Rows",functionName:"rewriteSDB"}]);
}
function getMyDb_(wb,sortObj) {
  var SS = wb.getSheetByName('SheetDb');
  var db = SS.getRange(2, 1, SS.getLastRow()-1, SS.getLastColumn()).sort(sortObj).getValues();
  return db
}
function saveMyDb_(wb,db,range) {
  if ( db == null ) return 1
  var lock = LockService.getPublicLock();
  lock.tryLock(30000);
  if ( lock.hasLock() ) {
    // Have a lock on the db, now save
    var SS = wb.getSheetByName('SheetDb');
    if ( range == null) {
      // No position input -> full db write -> no sorting needed
      if ( db.length < SS.getLastRow()-1 ) {
        // new db is smaller than old db, so clear it out
        SS.getRange(2, 1, SS.getLastRow(), SS.getLastColumn()).setValue('');
      }
      SS.getRange(2, 1, db.length, db[0].length).setValues(db).sort(1);
    } else {
      // supplied position to save to, e.g. minidb save -> alphabetical sort required before saving
      SS.getRange(2, 1, SS.getLastRow(), SS.getLastColumn()).sort(1);
      SS.getRange(range[0],range[1],range[2],range[3]).setValues(db);
    }
    SpreadsheetApp.flush(); // Commit changes (causing immediate failure as of 2017-02-21, commenting out means error shows up on line 79 on execution #2 -> 127 hunters updated per UpdateDatabase call
    lock.releaseLock();
    return true
  }
  return false
}
function AddMemberToDB_(wb,dbList) { 
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
}
function UpdateDatabase() {
  // This function is used to update the database's values, and runs frequently on small sets of data
  var BatchSize = 127;                                                               // Number of records to process on each execution
  var LastRan = 1*PropertiesService.getScriptProperties().getProperty('LastRan');                           // Determine the last successfully processed record
  var db = []; var wb = SpreadsheetApp.openById(mhccSSkey);
  db = getMyDb_(wb,1);             // Get the db, sort on col 1 (name)
  var nMembers = db.length               // Database records count
  var sheet = wb.getSheetByName('Members');
  var nRows = sheet.getLastRow()-1;                                                     // Spreadsheet records count
  // Read in the MHCC tiers as a 13x3 array
  // If the MHCC tiers are moved, this getRange MUST be updated!
  var aRankTitle = sheet.getRange(3, 8, 13, 3).getValues();   // (row column numberrows numbercolumns)
  // New Member check
  if ( nMembers < nRows ) {
    var rs = AddMemberToDB_(wb,db);
    return rs;
  }
  
  // Perform scoreboard update check / progress reset
  if ( LastRan >= nMembers ) {
    UpdateScoreboard();
    PropertiesService.getScriptProperties().setProperty('LastRan', 0);        // Point the script back to the start
    return 0;
  }
  
  // Grab a subset of the alphabetized member record
  var lock = LockService.getPublicLock();
  lock.waitLock(30000);
  if ( lock.hasLock() ) {
    var starttime = new Date().getTime();
    Logger.log('Started with ' + LastRan + ' completed rows')// Perform time-remaining check
    while ( ((new Date().getTime()) - starttime)/1000 < 180 && LastRan < nMembers) {
      // Set up loop beginning at index LastRan (0-valued) and proceeding for BatchSize records.  Use start/stop times to determine if multiple
      // batches can be run without exceeding usage limits
      var btime = new Date().getTime();
      var dHunters = db.slice(LastRan,LastRan-0+BatchSize-0);  // Create a new array from LastRan to LastRan + BatchSize. 
      var sIDstring = dHunters[0][1].toString();            // Get the first ID for the string
      var i = 0;
      for ( i=1;i<dHunters.length;i++ ) {
        if ( dHunters[i][1] != '' ) {
          sIDstring = sIDstring + ',' + dHunters[i][1].toString();  // Concatenate all the remaining non-null IDs
        }
      }
      // Have built the ID string, now query HT's MostMice.php
      var MM = UrlFetchApp.fetch('http://horntracker.com/backend/mostmice.php?function=hunters&hunters=' + sIDstring).getContentText();
      MM = JSON.parse(MM); // Separate line for debug purposes
      // Cannot requery the returned batch to ensure exact matching IDs, so have to requery the returned MM object
      // by looping over our db subset dHunters
      Logger.log(Object.keys(MM.hunters).length + ' returned hunters out of ' + BatchSize)
      for ( i=0;i<dHunters.length;i++ ) {
        var j = 'ht_'+dHunters[i][1];
        if ( typeof MM.hunters[j] != 'undefined' ) {
          // The hunter's ID was found in the MostMice object, he is not "lost"
          // Thus, the update can be performed
          var nB = 0;
          var nS = 0;
          var nG = 0;
          for ( var k in MM.hunters[j].mice ) {
            // Assign crowns by summing over all mice
            if ( MM.hunters[j].mice[k] >= 500 ) nG = nG + 1;
            else if ( MM.hunters[j].mice[k] >= 100 ) nS = nS + 1;
            else if ( MM.hunters[j].mice[k] >= 10 ) nB = nB + 1;
          }
          dHunters[i][3] = Date.parse((MM.hunters[j].lst).replace(/-/g,"/"));
          if ( dHunters[i][8] != nG || dHunters[i][7] != nS || dHunters[i][6] != nB ) dHunters[i][4] = new Date().getTime();
          dHunters[i][5] = new Date().getTime();  // Time of last update, the 'touched' value
          dHunters[i][6] = nB // Bronze
          dHunters[i][7] = nS // Silver
          dHunters[i][8] = nG // Gold
          dHunters[i][9] = nG-0 + nS-0; // MHCC Crowns
          // Determine the MHCC rank & squirrel of this hunter
          for ( var k = 0; k<aRankTitle.length; k++ ) {
            if ( dHunters[i][9] >= aRankTitle[k][0] ) {
              // Crown count meets/exceeds required crowns for this level
              dHunters[i][11] = aRankTitle[k][2] // Set the Squirrel value
              break;
            }
          }
          if ( dHunters[i][3] >= (new Date().getTime() - 2000000000) ) dHunters[i][12] = 'Current';
          else dHunters[i][12] = 'Old';
          
        } else {
          // The hunter is not found in the MM object, he is lost.  Set his status to "Lost"
          dHunters[i][12] = 'Lost';
        }
      }
      // Have now completed the loop over the dHunters subset.  Rather than refresh the entire db each time this runs, 
      // only the changed rows will be updated.
      saveMyDb_(wb,dHunters,[2+LastRan-0,1,dHunters.length,dHunters[0].length]);
      LastRan = LastRan-0 + BatchSize-0; // Increment LastRan for next batch's usage
      PropertiesService.getScriptProperties().setProperties({'LastRan':LastRan, 'AvgTime':(((new Date().getTime()) - starttime)/1000+PropertiesService.getScriptProperties().getProperty('AvgTime')*1)/2});
      Logger.log('Batch time of ' + ((new Date().getTime()) - btime)/1000 + ' sec')
    }
    Logger.log('Completed up to ' + LastRan + ' hunters, script time ' + ((new Date().getTime()) - starttime)/1000 + ' sec');
    lock.releaseLock();
  }
}
function UpdateStale() {
  // This function is used to update the Lost Hunters page more frequently than UpdateScoreboard can run
  var lock = LockService.getPublicLock();
  lock.waitLock(5000);
  if ( lock.hasLock() ) {
    var wb = SpreadsheetApp.openById(mhccSSkey);
    var dbSheet = wb.getSheetByName('SheetDb');
    var db = getMyDb_(wb,4); // Db, sorted by LastSeen (ascending)
    var lostSheet = wb.getSheetByName('"Lost" Hunters');
    var StaleArray = [];
    var LostArray = [];
    for (var i = 0;i<db.length;i++ ) {
      // Loop over the sorted db and construct Lost and Stale arrays
      switch ( db[i][12] ) {
        case 'Old':
          StaleArray.push([db[i][0], Utilities.formatDate(new Date(db[i][3]), 'EST', 'yyyy-MM-dd'), db[i][2], db[i][2].replace('apps.facebook.com/mousehunt','www.mousehuntgame.com')]);
          break;
        case 'Lost':
          LostArray.push(['=hyperlink("' + db[i][2] + '","' + db[i][0] + '")']);
          break;
      }
    }
    // Write the new Stale Hunters to the sheet
    lostSheet.getRange(3,3,Math.max(lostSheet.getLastRow()-2,1),4).setValue('');  // Remove old Stale hunters
    if (StaleArray.length > 0) {
      lostSheet.getRange(3,3,StaleArray.length,StaleArray[0].length).setValues(StaleArray); // Add new Stale hunters
    }
    lostSheet.getRange(2, 1, lostSheet.getLastRow()-1).setValue('');            // Clean out any previously 'Lost' hunters
    if (LostArray.length > 0 ) {
      lostSheet.getRange(2, 1, LostArray.length).setFormulas(LostArray);        // Add new 'Lost' hunters
    } 
    lock.releaseLock();
  }
}
function UpdateScoreboard() {
  // This function is used to write the collected data from SheetDb into a format that is visually appealing and functional 
  var start = new Date().getTime();
  var wb = SpreadsheetApp.openById(mhccSSkey);
  
  // Check for stale & 'lost' hunters
  UpdateStale();
  
  // Build scoreboard
  // Get the crown-count sorted memberlist : {column:10,ascending:false},{column:9,ascending:false},{column:8,ascending:false},{column:7,ascending:false}]);
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
