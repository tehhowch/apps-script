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
  const props = PropertiesService.getScriptProperties();
  var LastRan = parseInt(props.getProperty('LastRan'), 10); // Determine the last successfully processed record
  const BatchSize = 127; // Number of records to process on each execution
  const db = getMyDb_(1); // Get the alphabetized db.
  const nMembers = db.length; // Database records count.
  const wb = SpreadsheetApp.getActive();
  const SS = wb.getSheetByName('Members');
  // Read in the tiers as a 21x3 array (If the tiers are moved, this getRange MUST be updated!)
  const aRankTitle = wb.getSheetByName('Ranks').getRange(2, 1, 21, 3).getValues();

  if (nMembers < SS.getLastRow() - 1) {
    // Add new members.
    return AddMemberToDB_(db);
  } else if (LastRan >= nMembers) {
    // Perform scoreboard update.
    UpdateScoreboard();
    props.setProperty('LastRan', 0);
    return true;
  }

  const lock = LockService.getScriptLock();
  if (lock.tryLock(30000)) {
    const start = new Date().getTime();
    do {
      var batch = db.slice(LastRan, LastRan + BatchSize);
      var idString = batch.map(function (member) { return member[1]; }).filter(function (id) { return !!id; }).join(",");
      var response = UrlFetchApp.fetch("http://horntracker.com/backend/mostmice.php?function=hunters&hunters=" + idString).getContentText();
      if (response.indexOf("maintenance") !== -1 || response.indexOf("Unexpected") === 0)
        return false;
      var mmData = JSON.parse(response).hunters;
      batch.forEach(function (member) {
        var htId = "ht_" + member[1];
        var data = mmData[htId];
        if (!data) {
          // Hunter was queried for, but not found -> Lost
          member[11] = "Lost";
        } else {
          var whelps = data.mice["Whelpling"] || 0;
          var wardens = data.mice["Draconic Warden"] || 0;
          var dragons = data.mice["Dragon"] || 0;
          // Update the LastSeen timestamp.
          member[3] = Date.parse(data.lst.replace(/-/g, "/"));
          // Update the LastChange if needed.
          if (member[4] === 0 || (whelps !== member[6] || wardens !== member[7] || dragons !== member[8])) {
            member[4] = member[3];
          }
          member[5] = new Date().getTime();
          member[6] = whelps;
          member[7] = wardens;
          member[8] = dragons;
          // Determine the member's title.
          for (var k = 0; k < aRankTitle.length; ++k) {
            if (dragons <= aRankTitle[k][2]) {
              member[9] = aRankTitle[k][0];
              break;
            }
          }
          member[11] = (member[3] < new Date().getTime() - 20 * 86400 * 1000) ? "Old" : "Current";
        }
      });

      saveMyDb_(batch, [2 + LastRan, 1, batch.length, batch[0].length]);
      LastRan += BatchSize;
      props.setProperty("LastRan", LastRan);
    } while (LastRan < nMembers && (new Date().getTime() - start) < 140000);
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
