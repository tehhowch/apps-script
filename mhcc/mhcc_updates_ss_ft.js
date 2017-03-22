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
 *  the current status of the update, you can view the lastRan parameter via File -> Project Properties -> Project Properties
 *
 * Forcing a Scoreboard Update
 *
 *  If you must update the scoreboard immediately, manually run the UpdateScoreboard function via "Run" -> "UpdateScoreboard" here,
 *  or use the 'Administration' tab on from the spreadsheet. Doing so will commit the current state of the member list to the
 *  scoreboard sheet. This will also reset the lastRan parameter, effectively restarting the update cycle. It may also trigger the
 *  FusionTable maintenance script, which trims out old or duplicated records from the crown database.
 *
 * Forcing a Restart (Getting the database to restart crown updates instead of continuing)
 *
 *  This is easiest done by forcing a scoreboard update, but can also be achieved by editing the lastRan parameter. Click "File" ->
 *  "Project Properties" -> "Project Properties", and you should now see a table of fields and values.  Click the current value for
 *  lastRan (e.g. 2364) and replace it with 0.  Click "Save" to commit your change.
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
  var lock = LockService.getScriptLock();
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
  var batchSize = 127;
  var wb = SpreadsheetApp.openById(mhccSSkey);
  var db = getMyDb_(wb,1);                                                // alphabetized records with Rank and CrownChangeDate available
  var dbKeys = getDbIndex_(db);                                           // map UIDs to their index in the db for fast 1:1 accessing
  var props = PropertiesService.getScriptProperties().getProperties();
  var numMembers = props.numMembers;                                      // Database records count (may be larger than the written db on SheetDb)
  var lastRan = props.lastRan*1;                                          // The last successfully updated member crown data record
  var sheet = wb.getSheetByName('Members');
  // Read in the MHCC tiers as a 13x3 array. If the MHCC tiers are moved, this getRange target MUST be updated!
  var aRankTitle = sheet.getRange(3, 8, 13, 3).getValues();
  if ( lastRan >= numMembers ) {
    UpdateScoreboard();                                                   // Perform scoreboard update check / progress reset
    PropertiesService.getScriptProperties().setProperty('lastRan', 0);    // Point the script back to the start
  } else {
      // Grab a subset of the alphabetized member record
      var lock = LockService.getScriptLock();
      lock.waitLock(30000);
      if ( lock.hasLock() ) {
        var startTime = new Date().getTime();
        Logger.log('Started with '+lastRan+' completed member updates');
        var remMembers = getUserBatch_(0,numMembers*1);                   // get everyone
        var mem2Update = [];                                              // remMembers is Array of [Name, UID]
        // Loop over remaining members in sets of batchSize. Stop looping when out of members or >180s of runtime.
        while ( ((new Date().getTime() - startTime)/1000 < 180) && (lastRan < numMembers) ) {
          var batchHunters = remMembers.slice(lastRan,lastRan-0+batchSize-0);  // Create new array from a section of the [Name,UID] array
          var sIDstring = batchHunters[0][1];                 // Initialize the URL parameter string
          for (var i=1;i<batchHunters.length;i++ ) {
            if ( batchHunters[i][1] != '' ) {
              sIDstring += ","+batchHunters[i][1].toString();  // Concatenate all the remaining non-null IDs
            } else {
              throw new Error(batchHunters[i][0].toString()+' has no UID');
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
              // Adding columns onto our originally batchSize X 2 array
              batchHunters[i][2] = Date.parse((MM.hunters[j].lst).replace(/-/g,"/"));
              // The previous crown data is stored in the most recent scoreboard update, in our db variable
              if ( db[dbRow][7] != nG || db[dbRow][6] != nS || db[dbRow][5] != nB ) {
                batchHunters[i][3] = new Date().getTime();
              } else {
                batchHunters[i][3] = db[dbRow][3];          // Crown Change Date
              }
              batchHunters[i][4] = new Date().getTime();    // Time of this update, the 'touched' value
              batchHunters[i][5] = nB                       // Bronze
              batchHunters[i][6] = nS                       // Silver
              batchHunters[i][7] = nG                       // Gold
              batchHunters[i][8] = nG-0 + nS-0;             // MHCC Crowns
              batchHunters[i][9] = db[dbRow][9]             // The member's rank among all members
              // Determine the MHCC rank & squirrel of this hunter
              for ( var k = 0; k<aRankTitle.length; k++ ) {
                if ( batchHunters[i][8] >= aRankTitle[k][0] ) {
                  // Crown count meets/exceeds required crowns for this level
                  batchHunters[i][10] = aRankTitle[k][2];    // Set the Squirrel value
                  break;
                }
              }
              batchHunters[i][11] = db[dbRow][4]            // When the member's rank was generated
            }
          }
          mem2Update = [].concat(mem2Update,batchHunters); // Stage this batch's data for a single write call
          lastRan = lastRan-0 + batchSize-0;               // Increment lastRan for next batch's usage
        }
        ftBatchWrite_(mem2Update);                         // maximum API efficiency is with minimum write calls.
        PropertiesService.getScriptProperties().setProperty('lastRan',lastRan.toString());
        Logger.log('Through '+lastRan+' members, elapsed='+((new Date().getTime())-startTime)/1000+' sec');
        lock.releaseLock();
      }
  }
}
/**
 * function UpdateStale:          Writes to the secondary page that serves as a "Help Wanted" ad for getting
 *                                updates for oft-unvisited members
 * @param {Object} wb             The MHCC spreadsheet instance
 * @param {Integer} lostTime      the number of milliseconds after which a member is considered "in dire need
 *                                of revisiting"
 */
function UpdateStale_(wb,lostTime) {
  var lock = LockService.getScriptLock();
  lock.waitLock(30000);
  if ( lock.hasLock() ) {
    // Retrieve the most recently written crown snapshots ordered by the date of last MostMice inspection, in ascending order
    var db = getMyDb_(wb,3);
    var lostSheet = wb.getSheetByName('"Lost" Hunters');
    var staleArray = [];
    var startTime = new Date().getTime();
    for (var i = 0;i<db.length;i++ ) {
        if ( startTime - db[i][3] > lostTime ) {
            staleArray.push([db[i][0],
                             Utilities.formatDate(new Date(db[i][3]), 'EST', 'yyyy-MM-dd'),
                             "https://apps.facebook.com/mousehunt/profile.php?snuid="+db[i][1],
                             "https://www.mousehuntgame.com/profile.php?snuid="+db[i][1]
                            ]);
        } else {
            // snapshots were ordered oldest->newest so once a row is no longer old, all remaining
            // rows will also not be old.
            break;
        }
    }
    // Remove pre-existing "Stale" reports
    lostSheet.getRange(3,3,Math.max(lostSheet.getLastRow()-2,1),4).setValue('');
    if (staleArray.length > 0) {
      // Add new Stale hunters (if possible)
      lostSheet.getRange(3,3,staleArray.length,4).setValues(staleArray);
    }
    lock.releaseLock();
  }
}
/**
 * function UpdateScoreboard:     Write the most recent snapshots of each member's crowns to the Scoreboard page
 *                                Update the spreadsheet snapshots of crown data on SheetDb, and update the number of members
 */
function UpdateScoreboard() {
  var startTime = new Date();
  var wb = SpreadsheetApp.openById(mhccSSkey);
  var numMembers = FusionTables.Query.sql("SELECT * FROM "+utbl).rows.length;
  PropertiesService.getScriptProperties().setProperty("numMembers",numMembers.toString());
  // To build the scoreboard....
  // 1) Request the most recent snapshots of all members
  var db = getLatestRows_(numMembers);
  // 2) Store it on SheetDb
  var didSave = saveMyDb_(wb,db);
  if ( didSave ) {
    UpdateStale_(wb, 20*86400*1000);                   // If a member hasn't been seen in the last 20 days, then request a high-priority update
    // 3) Sort it by MHCC crowns, then LastCrown, then LastSeen:
    // The first to have a particular crown total should rank above someone who attained it at a later time.
    var allHunters = getMyDb_(wb,[{column:9,ascending:false},{column:4,ascending:true},{column:3,ascending:true}]);
    var scoreboardArr = [], i = 1;
    // 4) Build the array with this format:   Rank UpdateDate CrownChangeDate Squirrel MHCCCrowns Name Profile
    while ( i <= allHunters.length ) {
      scoreboardArr.push([i,                                                                          // Rank
                       Utilities.formatDate(new Date(allHunters[i-1][3]), 'EST', 'yyyy-MM-dd'),       // Last Seen
                       Utilities.formatDate(new Date(allHunters[i-1][4]), 'EST', 'yyyy-MM-dd'),       // Last Crown
                       allHunters[i-1][10],                                                            // Squirrel
                       allHunters[i-1][8],                                                            // #MHCC Crowns
                       allHunters[i-1][0],                                                            // Name
                       "https://apps.facebook.com/mousehunt/profile.php?snuid="+allHunters[i-1][1]    // Profile Link (fb)
                      ])
      if ( i%150 == 0 ) scoreboardArr.push(['Rank','Last Seen','Last Crown','Squirrel Rank','G+S Crowns','Hunter','Profile Link'] )
      // Store the counter as the hunters' rank, then increment the counter
      allHunters[i-1][9]=i++;
    }
    // 5) Write it to the spreadsheet
    var sheet = wb.getSheetByName('Scoreboard');
    sheet.getRange(2, 1, sheet.getLastRow(), scoreboardArr[0].length).setValue('');
    sheet.getRange(2, 1, scoreboardArr.length, scoreboardArr[0].length).setValues(scoreboardArr);
    // Provide estimate of the next new scoreboard posting and the time this one was posted
    wb.getSheetByName('Members').getRange('I23').setValue((startTime-wb.getSheetByName('Members').getRange('H23').getValue())/(24*60*60*1000));
    wb.getSheetByName('Members').getRange('H23').setValue(startTime);
    // Overwrite the latest db version with the version that has the proper ranks
    saveMyDb_(wb,allHunters);

  } else {
      throw new Error('Unable to save snapshots retrieved from crown database');
  }
  SpreadsheetApp.flush();
  Logger.log((new Date().getTime() - startTime.getTime())/1000 + ' sec for all scoreboard operations');
}
