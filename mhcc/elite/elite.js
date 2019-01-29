/**
 *  This spreadsheet uses Google Apps Script and the Spreadsheets API functionality to maintain a record of all Elite MHCC members placed on the
 *  'Members' sheet. Via the UpdateDatabase script and the external Horntracker.com website, the crowns of all members can be updated in an
 *  automated fashion. These members are processed in chunks of up to 127 at a time (higher batch sizes overload the maximum URL length), with
 *  an unspecified number of batches processed per execution. The script will update as many batches as it can without exceeding a specified execution
 *  time (default 30 seconds), in order to avoid triggering the "Maximum Execution Time Exceeded" error (at 300 seconds).
 *
 *
 * Adding Members
 *
 *  Place the member name and their profile link in the next row on the 'Members' worksheet. They will be automatically added on the next script execution
 *
 * Deleting Members
 *
 *  First delete the relevant row on the 'Members' worksheet.
 *  Second, delete the relevant row on the 'SheetDb' worksheet.
 *  Optionally, delete the relevant row on the Scoreboard worksheet. (This would be done automatically on next update)
 *
 * Tracking Progress
 *
 *  Whoever has set up the triggered events will likely receive daily emails about failed executions of the script, due likely to timeouts from Horntracker
 *  (during its maintenance period, or high load). If you wish to know the current status of the update, you can unhide the SheetDb worksheet, and scroll down
 *  the LastTouched column, which is the last time the UpdateDatabase script manipulated data in that row (measured in milliseconds). You can alternately view
 *  the LastRan parameter via File -> Project Properties -> Project Properties
 *
 * Forcing a Scoreboard Update
 *
 *  If you must update the scoreboard immediately, you can manually run the UpdateScoreboard function via "Run -> UpdateScoreboard". Doing so will commit
 *  the current state of the member list to the scoreboard sheet, and may generate a significant number of "Lost" hunters for the sole reason that they hadn't
 *  been updated yet this cycle. This will not reset the current progress of the database update script.
 *
 * Forcing a Restart (Getting the database to restart crown updates instead of continuing)
 *
 *  If the requirements for a title are changed, the true status for each member will not be reflected until the scoreboard has updated twice. This is
 *  because any hunters processed before the change are not re-processed upon changing of the titles / number of mice. To restart the database update
 *  process from square 1, you must edit the LastRan parameter. Click "File -> Project Properties… -> Project Properties", and you should now see
 *  a table of fields and values. Click the current value for LastRan (e.g. 2364) and replace it with 0. Click "Save" to commit your change. You should
 *  verify that your change was accepted, and this can be done by waiting 15 minutes. View the LastRan parameter after 10 minutes, and if it is greater
 *  than 300 then your first edit was performed while the script was running, and promptly overwritten when the script finished. Repeat this cycle of
 *  waiting 15 minutes and checking the LastRan value until it is apparent that the script has accepted the new value.
 *
 * Function list
 *
 *  AddMemberToDB:  Two inputs, called from UpdateDatabase whenever the number of member rows on 'Members' is greater than the number of members in the
 *                  hidden database.  It will scan over the entire member list in order to determine which members are new, and will add them.
 *
 *  UpdateDatabase:  This is the main function.  It should be called by a time-based trigger, running either every 5 or 10 minutes.  Refer to inline comments
 *                   for details of how the script runs; in general it retrieves a subset of MHCC members, asks Horntracker for their mouse catchess, and then
 *                   adds up catches, assigns Elite points, determines whether or not the number of current crowns is different than the number of
 *                   stored crowns, and touches the record so that it is known whether or not the record is up-to-date.  These changes are then saved back to
 *                   the database.  When new member-info rows are detected, this script will partner up with AddMemberToDB to add these members to the fold.
 *
 *  UpdateScoreboard:  This function manages the updates for the Scoreboard. After generating the scoreboard, it will then write the current rank of each member
 *                     to the SheetDb page.
 */

var dbSheetName = 'SheetDb';
var SS = SpreadsheetApp.getActive();

/**
 * function getMyDb_          Returns the entire data contents of the worksheet SheetDb to the calling
 *                            code as a rectangular array. Does not supply header information.
 * @param  {GoogleAppsScript.Spreadsheet.Spreadsheet} wb      The workbook containing the worksheet named SheetDb
 * @param  {number|Array <{column: number, ascending: boolean}>} [sortObj]   An integer column number or an Object[][] of sort objects
 * @return {Array[]}          An M by N rectangular array which can be used with Range.setValues() methods
 */
function getMyDb_(wb, sortObj)
{
  const SS = wb.getSheetByName(dbSheetName);
  try
  {
    var r = SS.getRange(2, 1, SS.getLastRow() - 1, SS.getLastColumn());
    if (sortObj)
      r.sort(sortObj);
    return r.getValues();
  }
  catch (e)
  {
    console.warn({ "message": e.message, "error": e, "dbRange": r, "sorter": sortObj });
    return [];
  }
}



function saveMyDb_(db, range)
{
  if (!db || !db.length || !db[0].length)
    return false;
  const lock = LockService.getScriptLock();
  lock.tryLock(30000);
  if (lock.hasLock())
  {
    // Have a lock on the db, now save
    const sheet = SS.getSheetByName(dbSheetName);
    if (!range)
    {
      // No position input -> full db write -> no sorting needed
      if (db.length < sheet.getLastRow() - 1)
      {
        // new db is smaller than old db, so clear it out
        sheet.getRange(2, 1, sheet.getLastRow(), sheet.getLastColumn()).clearContent();
        SpreadsheetApp.flush();
      }
      sheet.getRange(2, 1, db.length, db[0].length).setValues(db).sort(1);
    }
    else
    {
      // supplied position to save to, e.g. minidb save -> alphabetical sort required before saving
      sheet.getRange(2, 1, sheet.getLastRow(), sheet.getLastColumn()).sort(1);
      sheet.getRange(range[0], range[1], range[2], range[3]).setValues(db);
    }
    SpreadsheetApp.flush(); // Commit changes
    lock.releaseLock();
    return true
  }
  return false
}


/**
 * Add any members found only on "Members" to the database.
 * @param {Array <string[]>} dbList The existing members from SheetDb
 * @returns {boolean} Whether any members were added to the database.
 */
function AddMemberToDB_(dbList)
{
  if (!dbList)
    return false;
  const knownMembers = dbList.reduce(function (acc, entry, i) {
    var dbID = entry[1].toString();
    acc[dbID] = { "index": i, "name": entry[0] };
    return acc;
  }, {});

  // Get an alphabetically sorted list of members
  // MemberRange = [[Name, Profile Link],[Name2, ProfLink2],...]
  const memSS = SS.getSheetByName('Members');
  const memList = memSS.getRange(2, 1, memSS.getLastRow() - 1, 2).sort(1)
    .getValues()
    .map(function (row) {
      var mplink = row[1];
      var mpID = mplink.slice(mplink.search("=") + 1).toString();
      return {'name': row[0], 'uid': mpID, 'link': mplink };
    })
    .filter(function (member) {
      return !knownMembers.hasOwnProperty(member.uid);
    });
  // If all the members are known, there's nothing to do.
  if (!memList.length)
    return false;

  var nextRank = dbList.length;
  const newDbEntries = memList.map(function (newMember) {
    return [
      newMember.name,
      newMember.uid,
      'https://www.mousehuntgame.com/profile.php?snuid=' + newMember.uid,
      0, // LastSeen
      0, // LastCrown
      0, // LastTouched
      0, // Bronze
      0, // Silver
      0, // Gold
      0, // Points
      ++nextRank,
      '', // Comment
      'Old'  // Status
    ];
  });
  Array.prototype.push.apply(dbList, newDbEntries);
  saveMyDb_(dbList);
  return true;
}



function UpdateDatabase()
{
  // This function is used to update the database's values, and runs frequently on small sets of data
  var BatchSize = 150;//127;                                                               // Number of records to process on each execution
  var LastRan = 1 * PropertiesService.getScriptProperties().getProperty('LastRan');                           // Determine the last successfully processed record
  var db = [];
  db = getMyDb_(SS, 1);             // Get the db, sort on col 1 (name)
  var nMembers = db.length               // Database records count
  var sheet = SS.getSheetByName('Members');
  var nRows = sheet.getLastRow() - 1;                                                     // Spreadsheet records count

  var aScoring = SS.getSheetByName('Scoring').getRange(2, 1, 3, 2).getValues();   // (row column numberrows numbercolumns)
  var Minimums = SS.getRangeByName('Minimums').getValues();
  // New Member check
  if (nMembers < nRows)
  {
    var rs = AddMemberToDB_(db);
    return rs;
  }

  // Perform scoreboard update check / progress reset
  if (LastRan >= nMembers)
  {
    UpdateScoreboard();
    PropertiesService.getScriptProperties().setProperty('LastRan', 0);        // Point the script back to the start
    return 0;
  }

  // Grab a subset of the alphabetized member record
  var lock = LockService.getScriptLock();
  lock.waitLock(30000);
  if (lock.hasLock())
  {
    var starttime = new Date().getTime();
    Logger.log('Started with ' + LastRan + ' completed rows')// Perform time-remaining check (operate for up to 10% of allowable time)
    while (((new Date().getTime()) - starttime) / 1000 < 120 && LastRan < nMembers)
    {
      // Set up loop beginning at index LastRan (0-valued) and proceeding for BatchSize records.  Use start/stop times to determine if multiple
      // batches can be run without reaching 50% usage
      var btime = new Date().getTime();
      var dHunters = db.slice(LastRan, LastRan - 0 + BatchSize - 0);  // Create a new array from LastRan to LastRan + BatchSize.
      var sIDstring = dHunters[0][1].toString();            // Get the first ID for the string
      var i = 0;
      for (i = 1; i < dHunters.length; i++)
      {
        if (dHunters[i][1] != '')
        {
          sIDstring = sIDstring + ',' + dHunters[i][1];  // Concatenate all the remaining non-null IDs
        }
      }
      // Have built the ID string, now query HT's MostMice.php
      var MM = UrlFetchApp.fetch('http://horntracker.com/backend/mostmice.php?function=hunters&hunters=' + sIDstring).getContentText();
      if (MM.length <= 10)
        break;
      MM = JSON.parse(MM); // Separate line for debug purposes
      // Cannot requery the returned batch to ensure exact matching IDs, so have to requery the returned MM object
      // by looping over our db subset dHunters
      Logger.log(Object.keys(MM.hunters).length + ' returned hunters out of ' + BatchSize)
      for (i = 0; i < dHunters.length; i++)
      {
        var j = 'ht_' + dHunters[i][1];
        if (typeof MM.hunters[j] != 'undefined')
        {
          // The hunter's ID was found in the MostMice object, he is not "lost"
          // Thus, the update can be performed
          var nB = 0;
          var nS = 0;
          var nG = 0;
          for (var k in MM.hunters[j].mice)
          {
            // Assign crowns by summing over all mice
            if (MM.hunters[j].mice[k] >= 500) nG = nG + 1;
            else if (MM.hunters[j].mice[k] >= 100) nS = nS + 1;
            else if (MM.hunters[j].mice[k] >= 10) nB = nB + 1;
          }
          dHunters[i][3] = Date.parse((MM.hunters[j].lst).replace(/-/g, "/"));
          if (dHunters[i][8] !== nG || dHunters[i][7] !== nS || dHunters[i][6] !== nB)
            dHunters[i][4] = new Date().getTime();
          dHunters[i][5] = new Date().getTime();  // Time of last update, the 'touched' value
          dHunters[i][6] = nB // Bronze
          dHunters[i][7] = nS // Silver
          dHunters[i][8] = nG // Gold
          if (nG < Minimums[0][0])
          {
            dHunters[i][11] = 'Need ' + (Minimums[0][0] - nG) + ' more Gold';
            dHunters[i][9] = nG;
          }
          else if (nG + nS < Minimums[1][0])
          {
            dHunters[i][11] = 'Need ' + (Minimums[1][0] - nG - nS) + ' more Silver';
            dHunters[i][9] = nG * 2;
          }
          else if (nG + nS + nB < Minimums[2][0])
          {
            dHunters[i][11] = 'Need ' + (Minimums[2][0] - nG - nS - nB) + ' more Bronze';
            dHunters[i][9] = nG * 3 + nS;
          }
          else
          {
            dHunters[i][9] = nG * aScoring[0][1] + nS * aScoring[1][1] + nB * aScoring[2][1]; // Points
            dHunters[i][11] = '';
          }
          if (dHunters[i][3] >= (new Date().getTime() - 2000000000))
            dHunters[i][12] = 'Current';
          else
            dHunters[i][12] = 'Old';

        }
        else
        {
          // The hunter is not found in the MM object; (s)he is lost/excluded from MM.
          dHunters[i][12] = 'Manual';
          nB = dHunters[i][6] // Bronze
          nS = dHunters[i][7] // Silver
          nG = dHunters[i][8] // Gold
          if (nG < Minimums[0][0])
          {
            dHunters[i][11] = 'Need ' + (Minimums[0][0] - nG) + ' more Gold';
            dHunters[i][9] = nG;
          }
          else if (nG + nS < Minimums[1][0])
          {
            dHunters[i][11] = 'Need ' + (Minimums[1][0] - nG - nS) + ' more Gold & Silver';
            dHunters[i][9] = nG * 2;
          }
          else if (nG + nS + nB < Minimums[2][0])
          {
            dHunters[i][11] = 'Need ' + (Minimums[2][0] - nG - nS - nB) + ' more crowns';
            dHunters[i][9] = nG * 3 + nS;
          }
          else
          {
            dHunters[i][9] = nG * aScoring[0][1] + nS * aScoring[1][1] + nB * aScoring[2][1]; // Points
            dHunters[i][11] = '';
          }
        }
      }
      // Have now completed the loop over the dHunters subset.  Rather than refresh the entire db each time this runs,
      // only the changed rows will be updated.
      saveMyDb_(dHunters, [2 + LastRan - 0, 1, dHunters.length, dHunters[0].length]);
      LastRan = LastRan - 0 + BatchSize - 0; // Increment LastRan for next batch's usage
      PropertiesService.getScriptProperties().setProperties({ 'LastRan': LastRan, 'AvgTime': (((new Date().getTime()) - starttime) / 1000 + PropertiesService.getScriptProperties().getProperty('AvgTime') * 1) / 2 });
      Logger.log('Batch time of ' + ((new Date().getTime()) - btime) / 1000 + ' sec')
    }
    Logger.log('Completed up to ' + LastRan + ' hunters, script time ' + ((new Date().getTime()) - starttime) / 1000 + ' sec');
    lock.releaseLock();
  }
}



function UpdateScoreboard()
{
  // This function is used to update the spreadsheet's values, and runs after a complete db sweep

  // Get the crown-count sorted memberlist
  var AllHunters = getMyDb_(SS, [{ column: 10, ascending: false }, { column: 9, ascending: false }, { column: 8, ascending: false }, { column: 7, ascending: false }]);
  var Scoreboard = [];
  var i = 1;
  // Scoreboard format:   i UpdateDate CrownChangeDate Squirrel MHCCCrowns Name Profile
  while (i <= AllHunters.length)
  {
    Scoreboard.push([i,
      AllHunters[i - 1][0],  // Name
      AllHunters[i - 1][2],   // Profile Link (fb)
      "",  // Filler for the hyperlink formula
      AllHunters[i - 1][8],                                        // #Gold Crowns
      AllHunters[i - 1][7] + AllHunters[i - 1][8],                     // #G+Silver Crowns
      AllHunters[i - 1][6] + AllHunters[i - 1][7] + AllHunters[i - 1][8],  // #Total Crowns
      AllHunters[i - 1][9],  // #Points
      AllHunters[i - 1][11], // Comments
      Utilities.formatDate(new Date(AllHunters[i - 1][3]), 'EST', 'yyyy-MM-dd'), // Last Seen
      Utilities.formatDate(new Date(AllHunters[i - 1][4]), 'EST', 'yyyy-MM-dd') // Last Crown
    ])
    if (i % 550 === 0)
      Scoreboard.push(['Rank', 'Name', 'Profile', 'Hunter', 'Squirrel', 'Gold', 'Silver', 'Bronze', 'Points', 'Last Seen', 'Last Crown'])
    AllHunters[i - 1][10] = i++;  // Store the hunter's rank in the db listing
  }
  saveMyDb_(AllHunters);    // Store & alphabetize the new ranks
  // Clear out old data
  var SS = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Scoreboard');
  SS.getRange(6, 1, SS.getLastRow(), Scoreboard[0].length).setValue('');

  // Write new data
  SS.getRange(6, 1, Scoreboard.length, Scoreboard[0].length).setValues(Scoreboard);//.sort([{column: 5, ascending:false},{column: 3, ascending: true}, {column: 2, ascending:true}]);
  SS.getRange(6, 4, Scoreboard.length, 1).setFormulaR1C1('=HYPERLINK(R[0]C[-1],R[0]C[-2])');

  // Force full write before returning
  SpreadsheetApp.flush();
  SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Members').getRange('K1').setValue(new Date());
}
