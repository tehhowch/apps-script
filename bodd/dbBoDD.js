/*

function                Purpose

AddMemberToDB           Called by UpdateDatabase whenever the number of rows on 'Members' is more than the
                        internal count of members, e.g. new members have been added to the group.  If the
                        member being evaluated already exists, nothing happens, but if the member does not
                        exist, he is added.

UpdateDatabase          Called by a time-based trigger, this function is in charge of everything.  Makes use
                        of the script property LastRan to provide inter-execution consistency.

UpdateStale             Called by the UpdateScoreboard function or a time-based trigger, this function
                        provides access to a means of updating the publicly-viewable links of members that
                        need a data refresh

UpdateScoreboard        Called by UpdateDatabase, this function will iterate over the internal database and
                        create the various scoreboards Roll of Honour, Alphabetical, and Underlings.  This
                        function can also be called manually.

ReverseMemberFind       Called by a time-based trigger, this function will iterate over the Alphabetical scoreboard
                        and determine if a member listed there is not listed on the 'Members' worksheet.  Any such
                        hunters are placed into the "Gone, but not yet" region on the 'Members' sheet as a reminder
                        to either perform the ManualMemberRemoval script (if the member is to be deleted), or to
                        restore the accidentally deleted row (if the member is supposed to exist)

*/
function getMyDb_(sortObj) {
  const sheet = SpreadsheetApp.getActive().getSheetByName('SheetDb');
  const db = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn())
      .sort(sortObj)
      .getValues();
  return db;
}

function saveMyDb_(db, range) {
  if (!db || !Array.isArray(db) || !db.length || !Array.isArray(db[0]))
    return false;

  const lock = LockService.getScriptLock();
  if (lock.tryLock(30000)) {
    // Have a lock on the db, now save
    const sheet = SpreadsheetApp.getActive().getSheetByName('SheetDb');
    if (!range) {
      // No position input -> full db write -> no sorting needed
      if (db.length < sheet.getLastRow() - 1) {
        // new db is smaller than old db, so clear it out
        sheet.getRange(2, 1, sheet.getLastRow(), sheet.getLastColumn()).clearContent();
        SpreadsheetApp.flush();
      }
      sheet.getRange(2, 1, db.length, db[0].length)
        .setValues(db)
        .sort(1);
    } else {
      // supplied position to save to, e.g. minidb save -> alphabetical sort required before saving
      sheet.getRange(2, 1, sheet.getLastRow(), sheet.getLastColumn()).sort(1);
      sheet.getRange(range[0], range[1], range[2], range[3]).setValues(db);
    }
    SpreadsheetApp.flush(); // Commit changes
    lock.releaseLock();
    return true;
  }
  lock.releaseLock();
  return false;
}

/**
 *
 * @param {Array[]} dbList The existing array of member records
 */
function AddMemberToDB_(dbList) {
  // This function compares the list of members on the Members page to the received list of members (from the SheetDb page).  Any members missing are added.
  if (!dbList)
    return false;

  // MemberRange = [[Name, JoinDate, Profile Link],[Name2, JoinDate2, ProfLink2],...]
  const memberSheet = SpreadsheetApp.getActive().getSheetByName('Members');
  // Get an alphabetically sorted list of members
  const memList = memberSheet.getRange(2, 1, memberSheet.getLastRow() - 1, 3)
      .sort(1)
      .getValues();
  if (!memList.length)
    return false;

  // Construct a name - id mapping of known members.
  const dbMembers = dbList.reduce(function (acc, dbMember) {
    var dbId = dbMember[1].toString();
    if (acc[dbId] !== undefined) {
      console.warn("Member '" + dbMember[0] + "' with id='" + dbId + "' already exists as '" + acc[dbId] + "'");
    } else {
      acc[dbId] = dbMember[0];
    }
    return acc;
  }, {});

  // Filter the "Member" list to only those not in the database.
  const toAdd = memList.filter(function (member) {
    var link = member[2];
    var id = link.slice(link.search("=") + 1).toString();
    return dbMembers[id] === undefined;
  });
  const newRank = dbList.length + 1;
  toAdd.forEach(function (member) {
    var link = member[2];
    var id = link.slice(link.search("=") + 1).toString();
    dbList.push([
      member[0],
      id,
      "https://www.mousehuntgame.com/profile.php?snuid=" + id,
      0, // LastSeen
      0, // LastChange
      0, // LastTouched
      0, // Whelps
      0, // Wardens
      0, // Dragons
      "Entrant", // Title
      newRank,
      "Old" // Record Status
    ]);
  });

  saveMyDb_(dbList);  // write the new db
  return true;
}

function UpdateDatabase() {
  // This function is used to update the database's values, and runs frequently on small sets of data
  var BatchSize = 127;                                                               // Number of records to process on each execution
  var LastRan = 1 * PropertiesService.getScriptProperties().getProperty('LastRan');                           // Determine the last successfully processed record
  var dbSS = SpreadsheetApp.getActive().getSheetByName('SheetDb');
  var db = [];
  db = getMyDb_(1);             // Get the db, sort on col 1 (name)
  var nMembers = db.length;               // Database records count
  var SS = SpreadsheetApp.getActive().getSheetByName('Members');
  var nRows = SS.getLastRow() - 1;                                                     // Spreadsheet records count
  // Read in the tiers as a 21x3 array
  // If the tiers are moved, this getRange MUST be updated!
  var aRankTitle = SpreadsheetApp.getActive().getSheetByName('Ranks').getRange(2, 1, 21, 3).getValues();   // (row column numberrows numbercolumns)
  // New Member check
  if (nMembers < nRows) {
    var rs = AddMemberToDB_(db);
    return rs;
  }

  // Perform scoreboard update check / progress reset
  if (LastRan >= nMembers) {
    UpdateScoreboard();
    PropertiesService.getScriptProperties().setProperty('LastRan', 0);        // Point the script back to the start
    return 0;
  }

  // Grab a subset of the alphabetized member record
  const lock = LockService.getScriptLock();
  if (lock.tryLock(30000)) {
    var starttime = new Date().getTime();
    Logger.log('Started with ' + LastRan + ' completed rows');// Perform time-remaining check (operate for up to 10% of allowable time)
    while (((new Date().getTime()) - starttime) / 1000 < 140 && LastRan < nMembers) {
      // Set up loop beginning at index LastRan (0-valued) and proceeding for BatchSize records.  Use start/stop times to determine if multiple
      // batches can be run without reaching 50% usage
      var btime = new Date().getTime();
      var dHunters = db.slice(LastRan, LastRan - 0 + BatchSize - 0);  // Create a new array from LastRan to LastRan + BatchSize.
      var sIDstring = dHunters[0][1].toString();            // Get the first ID for the string
      var i = 0;
      for (i = 1; i < dHunters.length; i++) {
        if (dHunters[i][1] != '') {
          sIDstring = sIDstring + ',' + dHunters[i][1];  //Concatenate all the remaining non-null IDs
        }
      }
      // Have built the ID string, now query HT's MostMice.php
      var MM = UrlFetchApp.fetch('http://horntracker.com/backend/mostmice.php?function=hunters&hunters=' + sIDstring).getContentText();
      if (MM.indexOf('maintenance') >= 0) return 1;
      MM = JSON.parse(MM); // Separate line for debug purposes
      // Cannot requery the returned batch to ensure exact matching IDs, so have to requery the returned MM object
      // by looping over our db subset dHunters
      Logger.log(Object.keys(MM.hunters).length + ' returned hunters out of ' + BatchSize);
      for (i = 0; i < dHunters.length; i++) {
        var j = 'ht_' + dHunters[i][1];
        try {
          // The hunter's ID was found in the MostMice object, he is not "lost"
          // Thus, the update can be performed

          var nWh = MM.hunters[j].mice['Whelpling'] || 0;
          var nDW = MM.hunters[j].mice['Draconic Warden'] || 0;
          var nD = MM.hunters[j].mice['Dragon'] || 0;

          dHunters[i][3] = Date.parse((MM.hunters[j].lst).replace(/-/g, "/"));
          if (!(dHunters[i][4] > 0)) {
            dHunters[i][4] = Date.parse((MM.hunters[j].lst).replace(/-/g, "/"));
          } else {
            if (dHunters[i][8] != nD || dHunters[i][7] != nDW || dHunters[i][6] != nWh) dHunters[i][4] = new Date().getTime();
          }
          dHunters[i][5] = new Date().getTime();  // Time of last update, the 'touched' value
          dHunters[i][6] = nWh; // Whelplings
          dHunters[i][7] = nDW; // Wardens
          dHunters[i][8] = nD; // Dragons
          // Determine the BoDD of this hunter
          for (var k = 0; k < aRankTitle.length; k++) {
            if (nD <= aRankTitle[k][2]) {
              // Dragon count does not exceed required crowns for this level
              dHunters[i][9] = aRankTitle[k][0];
              break;
            }
          }
          if (dHunters[i][3] >= (new Date().getTime() - 2000000000)) dHunters[i][11] = 'Current';
          else dHunters[i][11] = 'Old';

        }
        catch (e) {
          // The hunter is not found in the MM object, he is lost.  Set his status to "Lost"
          dHunters[i][11] = 'Lost';
          Logger.log(dHunters[i][0] + ' is lost');
        }
      }
      // Have now completed the loop over the dHunters subset.  Rather than refresh the entire db each time this runs,
      // only the changed rows will be updated.
      saveMyDb_(dHunters, [2 + LastRan - 0, 1, dHunters.length, dHunters[0].length]);
      LastRan = LastRan - 0 + BatchSize - 0; // Increment LastRan for next batch's usage
      PropertiesService.getScriptProperties().setProperties({ 'LastRan': LastRan, 'AvgTime': (((new Date().getTime()) - starttime) / 1000 + PropertiesService.getScriptProperties().getProperty('AvgTime') * 1) / 2 });
      Logger.log('Batch time of ' + ((new Date().getTime()) - btime) / 1000 + ' sec');
    }
    Logger.log('Completed up to ' + LastRan + ' hunters, script time ' + ((new Date().getTime()) - starttime) / 1000 + ' sec');
  }
  lock.releaseLock();
}

/**
 * Flag hunters with out-of-date data, or no API data at all.
 */
function UpdateStale() {
  const lock = LockService.getScriptLock();
  if (lock.tryLock(5000)) {
    const db = getMyDb_(4); // Db, sorted by seen (ascending)
    const lSS = SpreadsheetApp.getActive().getSheetByName('RefreshLinks');
    const StaleArray = [];
    const LostArray = [];
    db.forEach(function (member) {
      if (member[11] === "Old") {
        StaleArray.push([
          member[0],
          Utilities.formatDate(new Date(member[3]), "EST", "yyyy-MM-dd"),
          member[2],
          member[2].replace("apps.facebook.com/mousehunt", "www.mousehuntgame.com")
        ]);
      } else if (member[11] === "Lost") {
        LostArray.push(['=HYPERLINK("' + member[2] + '","' + member[0] + '")']);
      }
    });

    // Write the new Stale Hunters to the sheet
    lSS.getRange(3, 3, Math.max(lSS.getLastRow() - 2, 1), 4).setValue('');  // Remove old Stale hunters
    if (StaleArray.length > 0) {
      lSS.getRange(3, 3, StaleArray.length, StaleArray[0].length).setValues(StaleArray); // Add new Stale hunters
    }
    lSS.getRange(2, 1, lSS.getLastRow() - 1).setValue('');            // Clean out any previously 'Lost' hunters
    if (LostArray.length > 0) {
      lSS.getRange(2, 1, LostArray.length).setFormulas(LostArray);      // Add new 'Lost' hunters
    }
  }
  lock.releaseLock();
}

/**
 * Refresh the various scoreboards with the latest database values.
 */
function UpdateScoreboard() {
  // Check for stale & lost hunters
  UpdateStale();

  // Build the scoreboards.
  const Scoreboard = [];
  const WardenBoard = [];
  const WhelpBoard = [];

  // Get the crown-count sorted memberlist
  const orderedHunters = getMyDb_([{ column: 9, ascending: false }, { column: 8, ascending: false }, { column: 7, ascending: false }]);
  orderedHunters.forEach(function (member, i) {
    var name = member[0].toString();
    var rank = i + 1;
    var dragons = parseInt(member[8], 10);
    var wardens = parseInt(member[7], 10);
    var whelps = parseInt(member[6], 10);
    Scoreboard.push([
      name, rank, dragons, member[9],
      Utilities.formatDate(new Date(member[4]), "EST", "yyyy-MM-dd"), // LastChange
      Utilities.formatDate(new Date(member[3]), "EST", "yyyy-MM-dd"), // LastSeen
      member[2] // Profile Link
    ]);
    WardenBoard.push([name, wardens]);
    WhelpBoard.push([name, whelps]);
    // Store the member's overall ranking.
    member[10] = rank;
  });
  // Write the new ranks to the database.
  saveMyDb_(orderedHunters);

  // Write the new scoreboards
  const wb = SpreadsheetApp.getActive();
  // Clear out old data
  const rollSheet = wb.getSheetByName('Roll of Honour');
  rollSheet.getRange(2, 1, rollSheet.getLastRow(), Scoreboard[0].length).setValue('');
  // Write new data
  rollSheet.getRange(2, 1, Scoreboard.length, Scoreboard[0].length).setValues(Scoreboard);

  const alpha = wb.getSheetByName('Alphabetical');
  alpha.getRange(2, 1, alpha.getLastRow(), Scoreboard[0].length).setValue('');
  alpha.getRange(2, 1, Scoreboard.length, Scoreboard[0].length)
      .setValues(Scoreboard)
      .sort({ column: 1, ascending: true });

  const others = wb.getSheetByName('Underlings');
  others.getRange(2, 2, others.getLastRow(), WardenBoard[0].length - 0 + WhelpBoard[0].length - 0).setValue('');
  others.getRange(2, 2, WardenBoard.length, WardenBoard[0].length)
      .setValues(WardenBoard)
      .sort({ column: 3, ascending: false });
  others.getRange(2, 4, WhelpBoard.length, WhelpBoard[0].length)
      .setValues(WhelpBoard)
      .sort({ column: 5, ascending: false });

  // Force full write before returning
  SpreadsheetApp.flush();
  wb.getSheetByName('Members').getRange('H1').setValue(new Date());
}

/**
 * Determine which members are on the scoreboard, but not listed as Members
 * (i.e. those with no Member sheet row, but not deleted from the database.)
 */
function ReverseMemberFind() {
  // Used to determine which members are on the scoreboard but not in the Member list
  // ( e.g. deleted row but not removed from the database via ManualMemberRemoval )
  const wb = SpreadsheetApp.getActive();
  const memberSheet = wb.getSheetByName('Members');
  const members = memberSheet.getRange(2, 1, memberSheet.getLastRow() - 1, 3)
      .getValues() // [[Name1, Join1, Link1],[Name2 ... ]]
      .reduce(function (acc, member) {
        var link = member[2];
        var id = link.slice(link.search("=") + 1).toString();
        if (id) {
          acc[id] = member[0];
        }
        return acc;
      }, {});

  const scoreboard = wb.getSheetByName("Alphabetical");
  const scoreboardList = scoreboard.getRange(2, 1, scoreboard.getLastRow() - 1, 7).getValues();  // [[Name1]...[Link1],[Name2]...[Link2]]
  const GoneNotYet = scoreboardList.filter(function (sbMember) {
    var link = sbMember[6];
    var id = link.slice(link.search("=") + 1).toString();
    return members[id] === undefined;
  }).map(function (memberToReport) {
    return [memberToReport[0].toString(), memberToReport[6].toString()];
  });

  memberSheet.getRange(43, 6, 100, 2).setValue('');
  if (GoneNotYet.length) {
    SS.getRange(43, 6, GoneNotYet.length, GoneNotYet[0].length).setValues(GoneNotYet);
  }
}

// Runs on form submit, checks their ID against all IDs in the DB
function IsDupeMember(e) {
  console.log(e);
  const sheet = SpreadsheetApp.getActive().getSheetByName('JoinRequests');
  const URL = sheet.getRange(sheet.getLastRow(), 8).getValue().toString();
  const newId = URL.slice(URL.search("=") + 1);

  // Create a mapping object of the existing member IDs.
  const db = getMyDb_(1);
  if (!db.length)
    throw new Error("No database data found.");

  const searchObj = db.reduce(function (acc, member) {
    acc[member[1]] = member[0];
    return acc;
  }, {});

  sheet.getRange(sheet.getLastRow(), 9).setValue(searchObj.hasOwnProperty(newId) ? "Duplicate" : "Eligible ID");
}
