// @ts-check
/**
 * This spreadsheet uses Google Apps Script, the Spreadsheet Service, and Google
 * Fusion Tables to maintain a record of all Elite MHCC members. All crown data
 * is updated by scripts associated with the MHCC Scoreboard - this workbook is
 * a "listener" for a specific subset of members, with its own methodology for
 * ranking its member subset.
 *
 * Adding / Deleting Members
 *    To add or delete members, use the custom menu to open the administration
 * panel, and select the appropriate prompts. You will be asked for the desired
 * display name, and the member's MH Profile URL. When deleting a member, the
 * given name must match the internal name exactly, as an extra step.
 *   - All members must be members of MHCC in order to be added as Elite MHCC members
 *   - Deleting a member from Elite MHCC will **not** delete the member from MHCC
 *
 * Updating Display Name
 *    To change a member's display name, follow the procedure to add a member,
 * and specify the desired display name. Confirm the change when prompted.
 *
 *
 * Setting up triggers
 *    The only function that should be triggered is the "UpdateScoreboard" function
 */
// @OnlyCurrentDoc
/**
 * Create an administrative menu for spreadsheet editors.
 */
function onOpen()
{
  const ui = SpreadsheetApp.getUi();
  const menu = ui.createMenu("Elite Admin");
  menu.addItem("Update Member Info", "getSidebar");
  menu.addItem("Update Scoreboard", "UpdateScoreboard");
  menu.addSeparator();
  const subMenu = ui.createMenu("Auth");
  subMenu.addItem("Create Scoreboard Trigger", "createTrigger");
  subMenu.addItem("Delete My Triggers", "removeTriggers");
  subMenu.addItem("Revoke My Authorization", "revokeAuth");
  menu.addSubMenu(subMenu);
  menu.addToUi();
}

/**
 * Create a scoreboard update trigger for the given document
 */
function createTrigger()
{
  const wb = SpreadsheetApp.getActive();
  const fnToTrigger = "UpdateScoreboard";
  const allTriggers = ScriptApp.getProjectTriggers().map(function (t) { return t.getHandlerFunction(); });
  const propTriggers = PropertiesService.getScriptProperties().getProperty("triggers");
  const knownTriggers = (propTriggers ? JSON.parse(propTriggers) : []);
  if (allTriggers.filter(function (fn) { return fn === fnToTrigger; }).length ||
      knownTriggers.filter(function (fn) { return fn === fnToTrigger; }).length)
    wb.toast("Trigger already configured, thanks.");
  else
  {
    var t = ScriptApp.newTrigger(fnToTrigger).timeBased().everyHours(4).create();
    knownTriggers.push(t.getHandlerFunction());
    PropertiesService.getScriptProperties().setProperty("triggers", JSON.stringify(knownTriggers));
    wb.toast("Trigger configured successfully, thanks.");
  }
}

/**
 * Remove the invoking user's triggers for this Script Project.
 */
function removeTriggers()
{
  const wb = SpreadsheetApp.getActive();
  const userTriggers = ScriptApp.getUserTriggers(wb);
  const propTriggers = PropertiesService.getScriptProperties().getProperty("triggers");
  const knownTriggers = (propTriggers ? JSON.parse(propTriggers) : []);
  console.warn({ message: "User triggers deleted", count: userTriggers.length,
      functions: userTriggers.map(function (t) { return t.getHandlerFunction(); }), userKey: Session.getTemporaryActiveUserKey() });
  userTriggers.forEach(function (t) {
    var index = knownTriggers.indexOf(t.getHandlerFunction());
    if (index !== -1)
      knownTriggers.splice(index);
    ScriptApp.deleteTrigger(t);
  });
  PropertiesService.getScriptProperties().setProperty("triggers", JSON.stringify(knownTriggers));
  wb.toast("Deleted " + userTriggers.length + " project triggers.");
}

/**
 * Remove the invoking user's authorization for the script.
 */
function revokeAuth()
{
  const wb = SpreadsheetApp.getActive();
  console.warn({ message: "User authorization revoked", userKey: Session.getTemporaryActiveUserKey() });
  wb.toast("Script authorization revoked. You will need to reauthorize this project (by attempting to run a function).");
  ScriptApp.invalidateAuth();
}

/**
 * Update the spreadsheet's displayed values (after a complete update cycle).
 */
function UpdateScoreboard()
{
  const startTime = new Date().getTime();
  function CanUpdateScoreboard()
  {
    var latestMHCC = bq_getLatestRankTime_('Core');
    var latestElite = bq_getLatestRankTime_('Elite');
    return latestElite < latestMHCC;
  }
  if (!CanUpdateScoreboard())
    return;

  const wb = SpreadsheetApp.getActive();
  const scoring = wb.getRangeByName('Scoring').getValues()
    .reduce(function (acc, row, i) {
      var key = i === 0 ? "gold" : (i === 1 ? "silver" : (i === 2 ? "bronze" : ""));
      if (key)
        acc[key] = row[0];
      return acc;
    }, { "gold": 0, "silver": 0, "bronze": 0 });
  const minimumCounts = wb.getRangeByName('Minimums').getValues()
    .reduce(function (acc, row, i) {
      var key = i === 0 ? "gold" : (i === 1 ? "gs" : (i === 2 ? "total" : ""));
      if (key)
        acc[key] = row[0];
      return acc;
    }, { "gold": 0, "gs": 0, "total": 0 });

  // Get the points- & crown-sorted memberlist.
  const newData = getLatestEliteScoreboardRows_(scoring, minimumCounts);

  // Clear out old data.
  const sheet = wb.getSheetByName('Scoreboard');
  sheet.getRange(6, 1, sheet.getLastRow(), newData[0].length).clearContent();
  SpreadsheetApp.flush();

  // Write new data.
  sheet.getRange(6, 1, newData.length, newData[0].length).setValues(newData);

  console.log({message: "Scoreboard updated (" + newData.length + " rows)", elapsed: new Date().getTime() - startTime});
}

/**
 * Compute the latest scoreboard rows using the most up-to-date information from the MHCC database.
 * Submits the computed rows to the Elite Rank DB for archiving.
 * @param {{bronze: number, silver: number, gold: number}} scoreFor The number of points earned for a crown of each type
 * @param {{gold: number, gs: number, total: number}} minimum The minimum crown counts needed of each type.
 * @returns {Array[]} The ordered scoreboard rows, for spreadsheet serialization.
 */
function getLatestEliteScoreboardRows_(scoreFor, minimum)
{
  // Create an array of object data from the Fusion Tables row data.
  const recordData = bq_getLatestMHCCRows_().map(function (mhcc)
  {
    // MHCC: [Name, UID, LastSeen, LastCrown, LastTouched, Bronze, Silver, Gold, MHCC, Squirrel]
    var data = {
      name: mhcc[0].toString(),
      uid: mhcc[1].toString(),
      link: ("https://www.mousehuntgame.com/profile.php?snuid=" + mhcc[1]),
      seen: parseInt(mhcc[2], 10),
      lastCrown: parseInt(mhcc[3], 10),
      bronze: parseInt(mhcc[5], 10),
      silver: parseInt(mhcc[6], 10),
      gold: parseInt(mhcc[7], 10),
      gs: 0,
      total: 0,
      points: 0,
      comment: '',
    };
    data.gs = data.gold + data.silver;
    data.total = data.gs + data.bronze;

    if (data.gold < minimum.gold) {
      data.comment = "Need " + (minimum.gold - data.gold) + " more Gold";
      data.points = data.gold;
    }
    else if (data.gs < minimum.gs) {
      data.comment = "Need " + (minimum.gs - data.gs) + " more Silver";
      data.points = data.gold * 2;
    }
    else if (data.total < minimum.total) {
      data.comment = "Need " + (minimum.total - data.total) + " more Bronze";
      data.points = data.gold * 3 + data.silver;
    }
    else
      data.points = data.gold * scoreFor.gold + data.silver * scoreFor.silver + data.bronze * scoreFor.bronze;

    return data;
  });
  if (!recordData.length) throw new Error("Aborting Scoreboard update due to no retrieved MHCC records");

  // Sort record objects by points, descending.
  recordData.sort(function (a, b)
  {
    var pointDiff = b.points - a.points;
    if (pointDiff) return pointDiff;

    var goldDiff = b.gold - a.gold;
    if (goldDiff) return goldDiff;

    var gsDiff = b.gs - a.gs;
    return (gsDiff ? gsDiff : b.total - a.total);
  });

  // Create & format the scoreboard records.
  // TODO: Add hyperlink to Point/Rank history
  const records = recordData.map(function (data)
  {
    return [
      0,
      data.name,
      data.link,
      '=HYPERLINK("' + data.link + '", "' + data.name.replace(/"/g, '""') + '")',
      data.gold,
      data.gs,
      data.total,
      data.points,
      data.comment,
      Utilities.formatDate(new Date(data.seen), "EST", "yyyy-MM-dd"),
      Utilities.formatDate(new Date(data.lastCrown), "EST", "yyyy-MM-dd")
    ];
  });

  // Assign the member ranking.
  var rank = 0;
  records.forEach(function (record) { record[0] = ++rank; });

  try {
    // Create the Rank DB submissions using the ranked records, and the non-formatted
    // record data.
    const rankTime = new Date().getTime();
    const recordsObj = recordData.reduce(function (obj, data)
    {
      obj[data.uid] = data;
      return obj;
    }, {});
    const submissions = records.map(function (record)
    {
      // [Name, UID, LastSeen, RankTime, Rank, Points, Comment]
      var uid = record[2].slice(record[2].search("=") + 1).toString();
      var obj = recordsObj[uid];
      return [
        obj.name,
        uid,
        obj.seen,
        rankTime,
        record[0],
        obj.points,
        obj.comment
      ];
    });
    bq_addRankSnapshots_(submissions);
  }
  catch (err) {
    console.warn({ message: "Unable to write to Elite Ranks table",
        error: {msg: err.message, stack: err.stack.split("\n")} });
  }
  return records;
}
