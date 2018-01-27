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
var mhccSSkey = '1P8UDv4j2lPM0hAKw4EbBT_GtvlOgFYeARV16NzWA6pc';
var crownDBnumColumns = 12;
/**
 * function onOpen()      Sets up the admin's menu from the spreadsheet interface.
 */
function onOpen()
{
  SpreadsheetApp.getActiveSpreadsheet().addMenu('Administration', [{name:"Add Members", functionName:"addFusionMember"},
                                                                   {name:"Delete Members", functionName:"delFusionMember"},
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
  if (db == null)
    return 1;
  var lock = LockService.getScriptLock();
  lock.tryLock(30000);
  if (lock.hasLock())
  {
    // Have a lock on the db, now save.
    var SS = wb.getSheetByName('SheetDb');
    // If the new db is smaller, the old db must be cleared first.
    if (db.length < SS.getLastRow() - 1)
      SS.getRange(2, 1, SS.getLastRow(), SS.getLastColumn()).setValue('');
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
  var startTime = new Date().getTime();
  var batchSize = 127;
  var wb = SpreadsheetApp.openById(mhccSSkey);
  var db = getMyDb_(wb, 1);
  var props = PropertiesService.getScriptProperties().getProperties();
  // Database records count (may be larger than the written db on SheetDb)
  var numMembers = props.numMembers;
  // The last successfully updated member crown data record indicates where to begin updates.
  var lastRan = props.lastRan * 1;
  // CrownChange calculation and Rank storing requires a map between a UID and its SheetDb position.
  var dbKeys = getDbIndexMap_(db, numMembers);

  // Read in the MHCC tiers as a 13x3 array.
  // If the MHCC tiers are moved, this getRange target MUST be updated!
  var sheet = wb.getSheetByName('Members');
  var aRankTitle = sheet.getRange(3, 8, 13, 3).getValues();
  // After each Scoreboard operation, a backup of the FusionTable database should be created.
  if (lastRan >= numMembers)
  {
    UpdateScoreboard();
    PropertiesService.getScriptProperties().setProperty('lastRan', 0);
    if (Math.random() < 0.4)
      doWebhookPost_(props['mhDiscord']);
    doBackupTable_();
  }
  else
  {
    var lock = LockService.getScriptLock();
    lock.waitLock(30000);
    if (lock.hasLock())
    {
      // allMembers is an array of [Name, UID]
      var allMembers = getUserBatch_(0, numMembers * 1);
      var mem2Update = [], skippedRows = [];
      // Update remaining members in sets of batchSize. Stop early if past "safe" runtime.
    hunterBatchLoop:
      while (((new Date().getTime() - startTime) / 1000 < 150) && (lastRan < numMembers))
      {
        var batchHunters = allMembers.slice(lastRan, lastRan - 0 + batchSize - 0);
        var urlIDs = [];
        for (var i = 0; i < batchHunters.length; ++i)
        {
          if (batchHunters[i][1] != '')
            urlIDs.push(batchHunters[i][1].toString());
          else
            throw new Error(batchHunters[i][0].toString() + ' has no UID');
        }
        // Attempt to retrieve a JSON object from HornTracker
        try
        {
          var htResponse = UrlFetchApp.fetch('http://horntracker.com/backend/mostmice.php?function=hunters&hunters=' + urlIDs.join(','));
          if (htResponse == 'undefined')
            throw new Error('Undefined Response');
          else if (htResponse.getResponseCode() != 200)
            throw new Error('HornTracker Unavailable');
          else if (htResponse.getContentText().toLowerCase().indexOf('connect to mysql') > -1)
            throw new Error('Unexpected HornTracker Error');
          else if (htResponse.getContentText().length == 0)
            throw new Error('Empty Content Text');
          var MM = JSON.parse(htResponse.getContentText());
        }
        catch (e)
        {
          // Stop trying to get new data.
          switch (e.message)
          {
            case "HornTracker Unavailable":
            case "Unexpected HornTracker Error":
            case "Empty Content Text":
            case "Undefined Response":
              break hunterBatchLoop;
            break;
            default:
              if (e.message.toLowerCase().indexOf('unexpected error: h') > -1)
                break hunterBatchLoop;
              else if (e.message.toLowerCase().indexOf('timeout: h') > -1)
                break hunterBatchLoop;
              else
              {
                // Unknown new error: total abort
                console.error(e);
                throw new Error('HT GET - errmsg: "' + e.message + '"');
              }
          }
        }
        // Loop over our member subset batchHunters and parse the corresponding MM entry.
        for (var i = 0; i < batchHunters.length; ++i)
        {
          var j = 'ht_' + batchHunters[i][1];
          var dbRow = dbKeys[batchHunters[i][1]];           // store this members row in the large scoreboard dataset
          if (typeof dbRow == 'undefined')
          {
            // Should have found a row, but didn't. Explicitly request this member's update.
            var record = getMostRecentRecord_(batchHunters[i][1]);
            if (record.length == crownDBnumColumns)
            {
              // Add to SheetDb.
              wb.getSheetByName('SheetDb').appendRow(record);
              // Reindex rows.
              dbKeys = getDbIndexMap_(db, numMembers);
              dbRow = dbKeys[batchHunters[i][1]];
              // Skip this hunter if their data is still missing.
              if (typeof dbRow == 'undefined')
              {
                // Splice out this row as it will not have the proper number of columns.
                skippedRows.push(batchHunters.splice(i, 1)[0]);
                // Decrement the index to maintain a valid iterator.
                --i;
                continue;
              }
            }
            // Move on to the next hunter in batchHunters due to malformed record data.
            else
            {
              // Splice out this row as it does not have the proper number of columns.
              skippedRows.push(batchHunters.splice(i, 1)[0]);
              // Decrement the index to maintain a valid iterator.
              --i;
              continue;
            }
          }
          if (typeof MM.hunters[j] != 'undefined')
          {
            // The hunter's ID was found in the MostMice object, and the update can be performed.
            var nB = 0, nS = 0, nG = 0;
            // Assign crowns by summing over all mice.
            for (var k in MM.hunters[j].mice)
            {
              if (MM.hunters[j].mice[k] >= 500)
                nG++;
              else if (MM.hunters[j].mice[k] >= 100)
                nS++;
              else if (MM.hunters[j].mice[k] >= 10)
                nB++;
            }
            // Construct the new record by adding columns onto the original batchSize array.
            batchHunters[i][2] = Date.parse((MM.hunters[j].lst).replace(/-/g,"/"));

            // The previous crown data is stored in the most recent scoreboard update, in our db variable.
            if (db[dbRow][7] != nG || db[dbRow][6] != nS || db[dbRow][5] != nB)
              batchHunters[i][3] = batchHunters[i][2];
            // Re-use their Crown Change Date as they have no new crowns.
            else
              batchHunters[i][3] = db[dbRow][3];

            // Time of this update, the 'touched' value (must be unique!)
            batchHunters[i][4] = (i == 0) ? new Date().getTime() : (batchHunters[i - 1][4] - 0 + 1 - 0);
            batchHunters[i][5] = nB                      // Bronze
            batchHunters[i][6] = nS                      // Silver
            batchHunters[i][7] = nG                      // Gold
            batchHunters[i][8] = nG-0 + nS-0;            // MHCC Crowns
            batchHunters[i][9] = db[dbRow][9]            // The member's rank among all members
            // Determine the MHCC rank & Squirrel of this hunter.
            for (var k = 0; k < aRankTitle.length; ++k)
              if (batchHunters[i][8] >= aRankTitle[k][0])
              {
                // Crown count meets/exceeds required crowns for this level.
                batchHunters[i][10] = aRankTitle[k][2];
                break;
              }
            // Store the time when this hunter's rank was determined.
            batchHunters[i][11] = db[dbRow][11];  
          }
          else
          {
            // Splice out this row as it will not have the proper number of columns.
            skippedRows.push(batchHunters.splice(i, 1)[0]);
            // Decrement the index to maintain a valid iterator.
            --i;
          }
        }
        mem2Update = [].concat(mem2Update, batchHunters);       // Stage this batch's data for a single write call
        lastRan = lastRan-0 + batchSize-0;                      // Increment lastRan for next batch's usage
      }
      if (mem2Update.length > 0)
      {
        ftBatchWrite_(mem2Update);
        PropertiesService.getScriptProperties().setProperty('lastRan', lastRan.toString());
      }
      if (skippedRows.length > 0)
        console.info({message:'Skipped ' + skippedRows.length + ' members', initialData:skippedRows});
      console.log('Through ' + lastRan + ' members, elapsed=' + ((new Date().getTime()) - startTime) / 1000 + ' sec');
      lock.releaseLock();
    }
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
    var staleArray = [], startTime = new Date().getTime();
    for (var i = 0; i < db.length; ++i)
    {
      if (startTime - db[i][2] > lostTime)
      {
        staleArray.push([db[i][0],
                          Utilities.formatDate(new Date(db[i][2]), 'EST', 'yyyy-MM-dd'),
                          "https://apps.facebook.com/mousehunt/profile.php?snuid=" + db[i][1],
                          "https://www.mousehuntgame.com/profile.php?snuid=" + db[i][1]
                        ]);
      }
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
  var wb = SpreadsheetApp.openById(mhccSSkey);
  var numMembers = FusionTables.Query.sqlGet("SELECT * FROM " + utbl).rows.length;
  PropertiesService.getScriptProperties().setProperty("numMembers", numMembers.toString());
  // To build the scoreboard....
  // 1) Request the most recent snapshots of all members
  var db = getLatestRows_(numMembers);
  // 2) Store it on SheetDb
  var didSave = saveMyDb_(wb, db);
  if (didSave)
  {
    // If a member hasn't been seen in the last 20 days, then request a high-priority update
    UpdateStale_(wb, 20 * 86400 * 1000);

    // 3) Sort it by MHCC crowns, then LastCrown, then LastSeen. This means the first to have a
    // particular crown total should rank above someone who (was seen) attaining it at a later time.
    var allHunters = getMyDb_(wb, [{column:9, ascending:false}, {column:4, ascending:true}, {column:3, ascending:true}]);
    var scoreboardArr = [], rank = 1;
    var plotLink = 'https://script.google.com/macros/s/AKfycbwCT-oFMrVWR92BHqpbfPFs_RV_RJPQNV5pHnZSw6yO2CoYRI8/exec?uid=';
    // 4) Build the array with this format:   Rank UpdateDate CrownChangeDate Squirrel MHCCCrowns Name Profile
    while (rank <= allHunters.length)
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
      if (rank % 150 == 0)
        scoreboardArr.push(['Rank', 'Last Seen', 'Last Crown', 'Squirrel Rank', 'G+S Crowns', 'Hunter', 'Profile Link', 'MHG']);

      // Store the time this rank was generated.
      allHunters[rank - 1][11] = startTime.getTime();
      // Store the counter as the hunters' rank, then increment the counter.
      allHunters[rank - 1][9] = rank++;
    }
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
  console.info((new Date().getTime() - startTime.getTime()) / 1000 + ' sec for all scoreboard operations');
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
