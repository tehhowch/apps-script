/**
 *  This spreadsheet uses Google Apps Script, Spreadsheets API, and the "experimental" FusionTables API to maintain a record of
 *  all MHCC members that opted into crown tracking. Via the UpdateDatabase script and the external Horntracker.com website, the
 *  crowns of all members can be updated in an automated fashion. These members are processed in chunks of up to 127 at a time
 *  (higher batch sizes overload the maximum URL length), with an unspecified number of batches processed per execution.  The
 *  script will update as many batches as it can without exceeding a specified execution time, in order to avoid triggering the
 *  Maximum Execution Time Exceeded error (at 300 seconds).
 *
 * Tracking Progress of Updates
 *
 *  As the unspecified and unresolved issue with a range-specific paste rendered the previous method unworkable, the move to host
 *  updates on FusionTables effectively hides the progress of updates from those with access to this workbook. SheetDb now will
 *  only ever hold the same data as that used to construct the currently-visibile Scoreboard page, but sorted alphabetically.
 *  Whoever has set up the triggered events will likely receive daily emails about failed executions of the script, due to timeouts
 *  from Horntracker or Service Errors from Google (and maybe daily/hourly Quota overruns from FusionTables). If you wish to know
 *  the current status of the update, you can view the LastRan parameter via File -> Project Properties -> Project Properties
 *
 * Forcing a Scoreboard Update
 *
 *  If you must update the scoreboard immediately, manually run the UpdateScoreboard function via "Run" -> "UpdateScoreboard" here,
 *  or use the 'Administration' tab on from the spreadsheet. Doing so will commit the current state of the member list to the
 *  scoreboard sheet. This will also reset the LastRan parameter, effectively restarting the update cycle. It may also trigger the
 *  FusionTable maintenance script, which trims out old or duplicated records from the crown database.
 *
 * Forcing a Restart (Getting the database to restart crown updates instead of continuing)
 *
 *  This is easiest done by forcing a scoreboard update, but can also be achieved by editing the LastRan parameter. Click "File" ->
 *  "Project Properties" -> "Project Properties", and you should now see a table of fields and values.  Click the current value for
 *  LastRan (e.g. 2364) and replace it with 0.  Click "Save" to commit your change.
 */
var mhccSSkey = '1P8UDv4j2lPM0hAKw4EbBT_GtvlOgFYeARV16NzWA6pc';
/**
 * function onOpen()      Sets up the admin's menu from the spreadsheet interface
 */
function onOpen(){
  SpreadsheetApp.getActiveSpreadsheet().addMenu('Administration', [{name:"Add Members",functionName:"addFusionMember"},
                                                                   {name:"Delete Members",functionName:"delFusionMember"},
                                                                   {name:"Refresh Scoreboard",functionName:"UpdateScoreboard"},
                                                                   {name:"Perform Crown Update",functionName:"UpdateDatabase"},
                                                                   {name:"Perform RecordCount Maintenance",functionName:"doRecordsMaintenance"}]);
}
/**
 * function getMyDb_          Returns the enter data contents of the worksheet SheetDb to the calling code
 *                            as a rectangular array. Does not supply header information.
 * @param  {Workbook} wb      The workbook containing the worksheet named SheetDb
 * @param  {Object} sortObj   An integer column number or an Object[][] of sort objects
 * @return {Array}            An M by N rectangular array which can be used with Range.setValues() methods
 */
function getMyDb_(wb,sortObj) {
  var SS = wb.getSheetByName('SheetDb');
  var db = SS.getRange(2, 1, SS.getLastRow()-1, SS.getLastColumn()).sort(sortObj).getValues();
  return db;
}
/**
 * function saveMyDb_         Uses the Range.setValues() method to write a rectangular array to the worksheet
 *
 * @param  {Workbook} wb      The workbook containing the worksheet named SheetDb
 * @param  {Array} db         The rectangular array of data to write to SheetDb
 * @return {Boolean}          Returns true if the data was written successfully, and false if a database lock was not acquired.
 */
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
/**
 * function UpdateDatabase    This is the main function which governs the process of updating crowns and writing the
 *                            scoreboard data. Batches of 127 are the maximum, due to the maximum length of a URL. The
 *  maximum execution time is 300 seconds, after which Google's servers will kill the script mid-execution. For this
 *  reason, coupled with the day-to-day performance variability of HornTracker, batches are started only if the execution
 *  time has not exceeded 180 seconds.
 */
function UpdateDatabase() {
  var BatchSize = 127;
  var wb = SpreadsheetApp.openById(mhccSSkey);
  var db = getMyDb_(wb,1);                                                // alphabetized records with Rank and CrownChangeDate available
  var dbKeys = getDbIndex_(db);                                           // map UIDs to their index in the db for fast 1:1 accessing
  var props = PropertiesService.getScriptProperties().getProperties();
  var nMembers = props.nMembers;                                          // Database records count (may be larger than the written db on SheetDb)
  var LastRan = props.LastRan*1;                                          // The last successfully updated member crown data record
  var sheet = wb.getSheetByName('Members');
  // Read in the MHCC tiers as a 13x3 array. If the MHCC tiers are moved, this getRange target MUST be updated!
  var aRankTitle = sheet.getRange(3, 8, 13, 3).getValues();
  if ( LastRan >= nMembers ) {
    UpdateScoreboard();                                                   // Perform scoreboard update check / progress reset
    PropertiesService.getScriptProperties().setProperty('LastRan', 0);    // Point the script back to the start
  } else {
      // Grab a subset of the alphabetized member record
      var lock = LockService.getPublicLock();
      lock.waitLock(30000);
      if ( lock.hasLock() ) {
        var starttime = new Date().getTime();
        Logger.log('Started with '+LastRan+' completed member updates');
        var remMembers = getUserBatch_(LastRan,nMembers*1);               // get everyone that hasn't been processed yet
        var mem2update = [];                                              // remMembers is Array of [Name, UID]
        // Loop over remaining members in sets of BatchSize. Stop looping when out of members or >180s of runtime.
        while ( ((new Date().getTime()) - starttime)/1000 < 180 && (LastRan < nMembers)) {
          var batchHunters = remMembers.slice(LastRan,LastRan-0+BatchSize-0);  // Create new array from a section of the [Name,UID] array
          var sIDstring = batchHunters[0][1];                 // Initialize the URL parameter string
          for (var i=1;i<batchHunters.length;i++ ) {
            if ( batchHunters[i][1] != '' ) {
              sIDstring += ","+batchHunters[i][1].toString();  // Concatenate all the remaining non-null IDs
            } else {
              Logger.log(batchHunters[i][0]+" has no UID");
            }
          }
          // Have built the ID string, now query HT's MostMice.php
          var MM = UrlFetchApp.fetch('http://horntracker.com/backend/mostmice.php?function=hunters&hunters='+sIDstring).getContentText();
          MM = JSON.parse(MM); // Separate line for debug purposes
          // Loop over our member subset batchHunters and parse the corresponding MM entry
          Logger.log(Object.keys(MM.hunters).length+' returned hunters out of '+batchHunters.length)
          for (var i=0;i<batchHunters.length;i++) {
            var j = 'ht_'+batchHunters[i][1];
            var dbRow = dbKeys[batchHunters[i][1]];           // store this members row in the large scoreboard dataset
            if ( typeof MM.hunters[j] != 'undefined' ) {
              // The hunter's ID was found in the MostMice object, and the update can be performed
              var nB = 0, nS = 0, nG = 0;
              for ( var k in MM.hunters[j].mice ) {
                // Assign crowns by summing over all mice
                if ( MM.hunters[j].mice[k] >= 500 ) nG++;
                else if ( MM.hunters[j].mice[k] >= 100 ) nS++;
                else if ( MM.hunters[j].mice[k] >= 10 ) nB++;
              }
              // Adding columns onto our originally BatchSize X 2 array
              batchHunters[i][2] = Date.parse((MM.hunters[j].lst).replace(/-/g,"/"));
              // The previous crown data is stored in the most recent scoreboard update, in our db variable
              if ( db[dbRow][7] != nG || db[dbRow][6] != nS || db[dbRow][5] != nB ) batchHunters[i][3] = new Date().getTime();
              batchHunters[i][4] = new Date().getTime();    // Time of this update, the 'touched' value
              batchHunters[i][5] = nB                       // Bronze
              batchHunters[i][6] = nS                       // Silver
              batchHunters[i][7] = nG                       // Gold
              batchHunters[i][8] = nG-0 + nS-0;             // MHCC Crowns
              batchHunters[i][9] = db[dbRow][9]             // The member's rank among all members, as of the previous update.
              // Determine the MHCC rank & squirrel of this hunter
              for ( var k = 0; k<aRankTitle.length; k++ ) {
                if ( batchHunters[i][8] >= aRankTitle[k][0] ) {
                  // Crown count meets/exceeds required crowns for this level
                  batchHunters[i][10] = aRankTitle[k][2];    // Set the Squirrel value
                  break;
                }
              }
            }
          }
          mem2update.push(batchHunters);                // Stage this batch's data for a single write call
          LastRan = LastRan-0 + BatchSize-0;            // Increment LastRan for next batch's usage
        }
        ftBatchWrite(mem2update);                       // maximum API efficiency is insert calls with mod 500.
        PropertiesService.getScriptProperties().setProperty('LastRan',LastRan.toString());
        Logger.log('Through '+LastRan+' members, elapsed='+((new Date().getTime())-starttime)/1000+' sec');
        lock.releaseLock();
      }
  }
}
/**
 * function UpdateStale:          Writes to the secondary page that serves as a "Help Wanted" ad for getting
 *                                updates for oft-unvisited members
 * @param {Integer} lostTime      the number of milliseconds fter which a member is considered "in dire need
 *                                of revisiting"
 */
function UpdateStale_(lostTime) {
  var lock = LockService.getPublicLock();
  lock.waitLock(30000);
  if ( lock.hasLock() ) {
    var wb = SpreadsheetApp.openById(mhccSSkey);
    var dbSheet = wb.getSheetByName('SheetDb');
    var db = getMyDb_(wb,4);                 // Sorts the most recently written snapshot of crown data by the date of last MostMice inspection, in ascending order
    var lostSheet = wb.getSheetByName('"Lost" Hunters');
    var StaleArray = [];
    var starttime = new Date().getTime();
    for (var i = 0;i<db.length;i++ ) {
        if ( starttime - db[i][3] > lostTime ) {
            StaleArray.push([db[i][0],
                             Utilities.formatDate(new Date(db[i][3]), 'EST', 'yyyy-MM-dd'),
                             "https://apps.facebook.com/mousehunt/profile.php?snuid="+db[i][1],
                             "https://www.mousehuntgame.com/profile.php?snuid="+db[i][1]
                            ]);
        }
    }
    // Write the new Stale Hunters to the sheet
    lostSheet.getRange(3,3,Math.max(lostSheet.getLastRow()-2,1),4).setValue('');  // Remove old Stale hunters
    if (StaleArray.length > 0) {                                                  // Add new Stale hunters
      lostSheet.getRange(3,3,StaleArray.length,4).setValues(StaleArray);
    }
    lock.releaseLock();
  }
}
/**
 * function UpdateScoreboard:     Write the most recent snapshot of each member's crowns to the Scoreboard page
 *                                Update the spreadsheet snapshot of crown data on SheetDb, and update the number of members
 */
function UpdateScoreboard() {
  UpdateStale(20*86400*1000);                   // If a member hasn't been seen in the last 20 days, then request a high-priority update
  var start = new Date();
  var wb = SpreadsheetApp.openById(mhccSSkey);
  var nMembers = FusionTables.Query.sql("SELECT * FROM "+utbl).rows.length;
  PropertiesService.getScriptProperties().setProperty("nMembers",nMembers.toString());
  // To build the scoreboard....
  // 1) Request the most recent snapshot of all members
  var db = getLatestRows_(nMembers);
  // 2) Store it on SheetDb
  saveMyDb_(wb,db);
  // 3) Sort it by MHCC crowns, then LastCrown, then LastSeen: The first to have a particular total should rank above someone who got there at a later time.
  var AllHunters = getMyDb_(wb,[{column:9,ascending:false},{column:4,ascending:true},{column:3,ascending:true}]);
  var Scoreboard = [];
  var i = 1;
  // 4) Build the array with this format:   i UpdateDate CrownChangeDate Squirrel MHCCCrowns Name Profile
  while ( i <= AllHunters.length ) {
    Scoreboard.push([i,                                                                             // Rank
                     Utilities.formatDate(new Date(AllHunters[i-1][3]), 'EST', 'yyyy-MM-dd'),       // Last Seen
                     Utilities.formatDate(new Date(AllHunters[i-1][4]), 'EST', 'yyyy-MM-dd'),       // Last Crown
                     AllHunters[i-1][9],                                                            // Squirrel
                     AllHunters[i-1][8],                                                            // #MHCC Crowns
                     AllHunters[i-1][0],                                                            // Name
                     "https://apps.facebook.com/mousehunt/profile.php?snuid="+AllHunters[i-1][1]    // Profile Link (fb)
                    ])
    if ( i%150 == 0 ) Scoreboard.push(['Rank','Last Seen','Last Crown','Squirrel Rank','G+S Crowns','Hunter','Profile Link'] )
  }
  // 5) Write it to the spreadsheet
  var sheet = wb.getSheetByName('Scoreboard');
  sheet.getRange(2, 1, sheet.getLastRow(), Scoreboard[0].length).setValue('');                      // Clear out old scoreboard data
  sheet.getRange(2, 1, Scoreboard.length, Scoreboard[0].length).setValues(Scoreboard);              // Write new data
  wb.getSheetByName('Members').getRange('I23').setValue((start-wb.getSheetByName('Members').getRange('H23').getValue())/(24*60*60*1000));
  wb.getSheetByName('Members').getRange('H23').setValue(start);                                     // Write scoreboard update time
  SpreadsheetApp.flush();                                                                           // Force full write before returning
  Logger.log((new Date().getTime() - start.getTime())/1000 + ' sec for scoreboard operations');
}
