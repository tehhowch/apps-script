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



/**
 * Update the database's values, and runs frequently on small sets of data
 */
function UpdateDatabase()
{
  const BatchSize = 150;//127;                                                               // Number of records to process on each execution
  const store = PropertiesService.getScriptProperties();
  var LastRan = 1 * store.getProperty('LastRan');                           // Determine the last successfully processed record
  const db = getMyDb_(SS, 1);             // Get the db, sort on col 1 (name)
  var memberCount = db.length               // Database records count
  const sheet = SS.getSheetByName('Members');
  var nRows = sheet.getLastRow() - 1;                                                     // Spreadsheet records count

  var aScoring = SS.getSheetByName('Scoring').getSheetValues(2, 1, 3, 2);
  var minimum = SS.getRangeByName('Minimums').getValues().reduce(function (acc, row, i) {
    var key = i === 0 ? "gold" : (i === 1 ? "silver" : (i === 2 ? "bronze" : ""));
    if (key)
      acc[key] = row[0];
    return acc;
  }, {});
  // New Member check
  if (memberCount < nRows)
    return AddMemberToDB_(db);

  // Perform scoreboard update check / progress reset
  if (LastRan >= memberCount)
  {
    UpdateScoreboard();
    store.setProperty('LastRan', 0);        // Point the script back to the start
    return 0;
  }

  // Grab a subset of the alphabetized member record
  const lock = LockService.getScriptLock();
  if (lock.tryLock(30000))
  {
    const start = new Date().getTime();
    // Perform time-remaining check (operate for up to 120 s (maximum 360)).

    do {
      // Set up loop beginning at index LastRan (0-valued) and proceeding for BatchSize records.
      var dHunters = db.slice(LastRan, LastRan - 0 + BatchSize - 0);  // Create a new array from LastRan to LastRan + BatchSize.
      var idString = dHunters.map(function (member) { return member[1]; })
        .filter(function (id) { return !!id; }).join(",");

      // Have built the ID string, now query HT's MostMice.php
      var MM = UrlFetchApp.fetch('http://horntracker.com/backend/mostmice.php?function=hunters&hunters=' + idString).getContentText();
      if (MM.length <= 10)
        break;
      var mmData = JSON.parse(MM).hunters;
      // Overwrite data in the sliced array, and rewrite the range subset to the database.
      dHunters.forEach(function (member) {
        var dataId = 'ht_' + member[1];
        var hunterData = mmData[dataId];
        if (hunterData)
        {
          var bronze = 0, silver = 0, gold = 0;
          for (var k in hunterData.mice)
          {
            // Assign crowns by summing over all mice
            if (hunterData.mice[k] >= 500) ++gold;
            else if (hunterData.mice[k] >= 100) ++silver;
            else if (hunterData.mice[k] >= 10) ++bronze;
          }
          // Set the "Last Seen" for this record.
          member[3] = Date.parse(hunterData.lst.replace(/-/g, "/"));
          // Set the "Last Crown" for this record
          if (member[8] !== gold || member[7] !== silver || member[6] !== bronze)
            member[4] = new Date().getTime();
          member[5] = new Date().getTime();
          member[6] = bronze;
          member[7] = silver;
          member[8] = gold;
          // Report if this record is using recent data or not.
          member[12] = (member[3] >= start - 20 * 864000 * 1000) ? "Current" : "Old";
        }
        else
        {
          // Manually entered data. All that might change are the needed amounts.
          gold = member[8];
          silver = member[7];
          bronze = member[6];
          member[12] = "Manual";
        }
        // Check if any comments are needed.
        var comment = "";
        var points = 0;
        if (gold < minimum.gold)
        {
          comment = 'Need ' + (minimum.gold - gold) + ' more Gold';
          points = gold;
        }
        else if (gold + silver < minimum.silver)
        {
          comment = 'Need ' + (minimum.silver - gold - silver) + ' more Silver';
          points = gold * 2;
        }
        else if (gold + silver + bronze < minimum.bronze)
        {
          comment = 'Need ' + (minimum.bronze - gold - silver - bronze) + ' more Bronze';
          points = gold * 3 + silver;
        }
        else
          points = gold * aScoring[0][1] + silver * aScoring[1][1] + bronze * aScoring[2][1];
        member[9] = points;
        member[11] = comment;
      });
      // Have now completed the loop over the dHunters subset.  Rather than refresh the entire db each time this runs,
      // only the changed rows will be updated.
      saveMyDb_(dHunters, [2 + LastRan - 0, 1, dHunters.length, dHunters[0].length]);
      LastRan += BatchSize - 0; // Increment LastRan for next batch's usage
    } while (new Date() - start < 120000 && LastRan < memberCount)
    Logger.log('Completed up to ' + LastRan + ' hunters, script time ' + ((new Date() - start) / 1000) + ' sec');
    store.setProperty('LastRan', LastRan.toString());
  }
  lock.releaseLock();
}



/**
 * Update the spreadsheet's displayed values (after a complete update cycle).
 */
function UpdateScoreboard()
{
  // Get the points-sorted memberlist. (Points, then Gold, then Silver, then Bronze)
  const AllHunters = getMyDb_(SS, [{ column: 10, ascending: false }, { column: 9, ascending: false }, { column: 8, ascending: false }, { column: 7, ascending: false }]);
  const newData = AllHunters.map(function (member, i) {
    member[10] = i + 1;
    return [
      member[10], // Rank
      member[0], // Name
      member[2], // FB Profile Link
      '=HYPERLINK("' + member[2] + '", "' + member[0] + '")',
      member[8], // Gold
      member[7] + member[8], // G + S
      member[6] + member[7] + member[8], // G + S + B
      member[9], // Points
      member[11], // Comments
      Utilities.formatDate(new Date(member[3]), "EST", "yyyy-MM-dd"), // Last Seen
      Utilities.formatDate(new Date(member[4]), "EST", "yyyy-MM-dd") // Last Crown
    ];
  });
  // Store & alphabetize the new ranks.
  saveMyDb_(AllHunters);

  // Clear out old data.
  const sheet = SS.getSheetByName('Scoreboard');
  sheet.getRange(6, 1, sheet.getLastRow(), newData[0].length).setValue('');
  SpreadsheetApp.flush();

  // Write new data.
  sheet.getRange(6, 1, newData.length, newData[0].length).setValues(newData);
  sheet.getRange(6, 4, newData.length, 1).setFormulaR1C1('=HYPERLINK(R[0]C[-1],R[0]C[-2])');
  // Timestamp the update.
  SS.getRange('Members!K1').setValue(new Date());
}
