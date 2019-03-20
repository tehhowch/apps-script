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
var wb = SpreadsheetApp.getActive();

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
  subMenu.addItem("Delete My Triggers", "removeTriggers");
  subMenu.addItem("Revoke My Authorization", "revokeAuth");
  menu.addSubMenu(subMenu);
  menu.addToUi();
}

/**
 * Remove the invoking user's triggers for this Script Project.
 */
function removeTriggers()
{
  const userTriggers = ScriptApp.getUserTriggers(wb);
  console.warn({ message: "User triggers deleted", count: userTriggers.length,
      functions: userTriggers.map(function (t) { return t.getHandlerFunction(); }), userKey: Session.getTemporaryActiveUserKey() });
  userTriggers.forEach(function (t) { ScriptApp.deleteTrigger(t); });
  wb.toast("Deleted " + userTriggers.length + " project triggers.");
}

/**
 * Remove the invoking user's authorization for the script.
 */
function revokeAuth()
{
  console.warn({ message: "User authorization revoked", userKey: Session.getTemporaryActiveUserKey() });
  wb.toast("Script authorization revoked. You will need to reauthorize this project (by attempting to run a function).");
  ScriptApp.invalidateAuth();
}

/**
 * Update the spreadsheet's displayed values (after a complete update cycle).
 */
function UpdateScoreboard()
{
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
}
