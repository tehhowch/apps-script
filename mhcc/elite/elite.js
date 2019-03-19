// @ts-check
// TODO: Update
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
 *  UpdateScoreboard:  This function manages the updates for the Scoreboard. After generating the scoreboard, it will then write the current rank of each member
 *                     to the SheetDb page.
 */
// @OnlyCurrentDoc
var wb = SpreadsheetApp.getActive();

/**
 * Create an administrative menu for spreadsheet editors.
 */
function onOpen()
{
  const menu = SpreadsheetApp.getUi().createMenu("Elite Admin");
  menu.addItem("Add / Remove Members", "getSidebar");
  menu.addItem("Update Scoreboard", "UpdateScoreboard");
  menu.addItem("Revoke Authorization", "revokeAuth");
  menu.addItem("Delete My Triggers", "removeTriggers");
  menu.addToUi();
}

/**
 * Remove the invoking user's triggers for this Script.
 */
function removeTriggers()
{
  const userTriggers = ScriptApp.getUserTriggers(wb);
  userTriggers.forEach(function (t) {
    ScriptApp.deleteTrigger(t);
  });
  console.warn({message: "User triggers deleted", count: userTriggers.length, functions: userTriggers.map(function (t) { return t.getHandlerFunction(); }), userKey: Session.getTemporaryActiveUserKey() });
  wb.toast("Deleted " + userTriggers.length + " project triggers.");
}

/**
 * Remove the invoking user's authorization for the script.
 */
function revokeAuth()
{
  console.warn({message: "User authorization revoked", userKey: Session.getTemporaryActiveUserKey() });
  wb.toast("Script authorization revoked. You will need to reauthorize this project.");
  ScriptApp.invalidateAuth();
}

/**
 * Update the spreadsheet's displayed values (after a complete update cycle).
 */
function UpdateScoreboard()
{
  const aScoring = wb.getSheetByName('Scoring').getSheetValues(2, 1, 3, 2);
  const scoring = "TODO";
  const minimumCounts = wb.getRangeByName('Minimums').getValues().reduce(function (acc, row, i) {
    var key = i === 0 ? "gold" : (i === 1 ? "silver" : (i === 2 ? "bronze" : ""));
    if (key)
      acc[key] = row[0];
    return acc;
  }, { 'gold': 0, 'silver': 0, 'bronze': 0 });

  // Get the points- & crown-sorted memberlist.
  const newData = getLatestEliteScoreboardRows_(scoring, minimumCounts);

  // Clear out old data.
  const sheet = wb.getSheetByName('Scoreboard');
  sheet.getRange(6, 1, sheet.getLastRow(), newData[0].length).clearContent();
  SpreadsheetApp.flush();

  // Write new data.
  sheet.getRange(6, 1, newData.length, newData[0].length).setValues(newData);
  // Timestamp the update.
  wb.getRange('Members!K1').setValue(new Date());
}
