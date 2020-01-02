//@ts-check
/**
 * Apps Script code to handle the webapp.
 */

/**
 * @typedef {Object} UserHistory
 * @property {string} memberName The display name associated with this history data.
 * @property {string[][]} crownHeader Array of column titles for the crown history data.
 * @property {number[][]} crownData The crown history data to be plotted.
 * @property {string[][]} rankHeader Array of column titles for the rank history data.
 * @property {number[][]} rankData The rank history data to be plotted.
 */

/**
 * Runs when the script's app link is clicked
 * Reference: https://developers.google.com/apps-script/guides/web#request_parameters
 * @param {GoogleAppsScript.Events.DoGet} e Object containing various properties. Each link will have at least one parameter labeled "uid", stored in e.parameter
 * @returns {GoogleAppsScript.HTML.HtmlOutput}  Returns a webpage described by assembling the various .html files
 */
function doGet(e)
{
  var pg = HtmlService.createTemplateFromFile("webapp\\page");
  // Require that multiple uids were explicitly joined, i.e. "&uid=1,2", rather than "&uid=1&uid=2".
  pg.webAppUID = e.parameter.uid;
  return pg.evaluate().setTitle("MHCC Crown History").setFaviconUrl("https://i.imgur.com/QMghA1l.png");
}

/**
 * Get the crown and rank data from BigQuery for use by the webapp's plotters.
 * Due to limitations on transferable data, date values are sent as millisecond timestamps
 * @param {string} uids The UID of the member(s) to query crown and rank history for.
 * @returns {string} JSON-stringified data object.
 */
function loadUserData(uids)
{
  /**
   * Nested function which handles parsing the data for a given user into the properly-formatted webapp code.
   * @param {Object <string, UserHistory>} outputToModify Data which will be stringified and sent to the webapp for display.
   * @param {UserData} userData Data received from the database which needs to be formatted for the webapp.
   */
  function _addUserData_(outputToModify, userData)
  {
    if (!userData.user) throw new Error("Missing username for whom to parse data.");

    outputToModify[userData.user] = {
      memberName: userData.user,

      crownHeader: [["Date Seen", "# Bronze", "# Silver", "# Gold", "Total"]],
      crownData: userData.crown.dataset.map(function (value) {
        return [value[1] * 1, value[2] * 1, value[3] * 1, value[4] * 1, (value[5] * 1 + value[2] * 1)];
      }),

      rankHeader: [["Date Ranked", "Rank", "# MHCC Crowns", "Date Seen"]],
      rankData: userData.rank.dataset.map(function (value) {
        return [value[4] * 1, value[3] * 1, value[2] * 1, value[1] * 1];
      }),
    };
    // Bind a reference to it.
    const userOutput = outputToModify[userData.user];

    if (userOutput.crownHeader[0].length !== userOutput.crownData[0].length)
      throw new Error("Crown History is not rectangular");
    if (userOutput.rankHeader[0].length !== userOutput.rankData[0].length)
      throw new Error("Rank History is not rectangular");

    // Ensure the crown data is sorted ascending by LastSeen.
    userOutput.crownData.sort(function (a, b) { return a[0] - b[0]; });

    // Ensure the rank data is sorted ascending by RankTime.
    userOutput.rankData.sort(function (a, b) { return a[0] - b[0]; });
  }

  if (!uids || uids === "undefined")
    throw new Error("No UID provided/loaded");

  if (uids.split(",").length > 5)
    throw new Error("UI for comparing that many members at once is not available.");

  // Obtain data for the UID(s).
  /** @type {Object <string, UserHistory>} */
  const dataForWebapp = {};
  const history = getUserHistory_(uids, true);
  history.forEach(function (user) { _addUserData_(dataForWebapp, user); });

  if (!Object.keys(dataForWebapp).length)
    throw new Error("No data to be sent to webapp");
  if (Object.keys(dataForWebapp).length !== uids.split(",").length)
    console.warn({ "message": "At least 1 requested dataset is unavailable", "uids": uids.split(","), "data": dataForWebapp });

  // Due to object complexity, send as a string instead of a raw object.
  return JSON.stringify(dataForWebapp);
}
