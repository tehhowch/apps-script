/**
 * Apps Script code to handle the webapp.
 */

/**
 * @typedef {Object} PostData
 * @property {number} length Length of the POST body (same as `contentLength`)
 * @property {string} type MIME type of the POST body.
 * @property {any}  contents The content text of the POST body.
 */
/**
 * Reference: https://developers.google.com/apps-script/guides/web#request_parameters
 *
 * @typedef {Object} WebApp_HTTP_Object
 * @property {Object <string, string>} parameter An object of key/value pairs that correspond to the request parameters. Only the first value
 * is returned for parameters that have multiple values. (e.g. `{"name": "alice", "n": "1"}`)
 * @property {number} contentLength The length of the request body for POST requests, or -1 for GET requests
 * @property {string} queryString The value of the query string portion of the URL, or null if no query string is specified. (e.g. `name=alice&n=1&n=2`)
 * @property {Object <string, string[]>} parameters An object similar to `parameter`, but with an array of values for each key (e.g. `{"name": ["alice"], "n": ["1", "2"]}`)
 * @property {PostData} postData Information about a POST request.
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
 *
 * @param {WebApp_HTTP_Object} e Object containing various properties. Each link will have at least one parameter labeled "uid", stored in e.parameter
 * @returns {HtmlOutput}  Returns a webpage described by assembling the various .html files
 */
function doGet(e)
{
  var pg = HtmlService.createTemplateFromFile("webapp\\page");
  // Require that multiple uids were explicitly joined, i.e. "&uid=1,2", rather than "&uid=1&uid=2".
  pg.webAppUID = e.parameter.uid;
  return pg.evaluate().setTitle("MHCC Crown History").setFaviconUrl("https://i.imgur.com/QMghA1l.png");
}
/**
 * Get the crown and rank data from FusionTables for use by the webapp's plotters.
 * Due to limitations on transferable data, date values are sent as millisecond timestamps
 *
 * @param {string} uids The UID of the member(s) to query crown and rank history for.
 * @returns {string} JSON-stringified data object.
 */
function loadUserData(uids)
{
  /**
   * Nested function which handles parsing the data for a given user into the properly-formatted webapp code.
   *
   * @param {Object <string, UserHistory>} outputToModify Data which will be stringified and sent to the webapp for display.
   * @param {UserData} userData Data received from the FusionTable which needs to be formatted for the webapp.
   */
  function _addUserData_(outputToModify, userData)
  {
    if (!userData.user) throw new Error("Missing username for whom to parse data.");
    // Create a property for this user in the parent's collection object
    outputToModify[userData.user] = {};
    // Bind a reference to it.
    const userOutput = outputToModify[userData.user];

    /**
     * Format the FusionTables data for consumption by the webapp.
     * userData: {
     *   "user": The user's display name
     *   "crown": {
     *      "headers": [Member, LastSeen, Bronze, Silver, Gold, MHCC]
     *      "dataset": [][] ordered by LastSeen ascending
     *   },
     *   "rank": {
     *      "headers": [Member, LastSeen, MHCC Crowns, Rank, RankTime]
     *      "dataset": [][] ordered by RankTime ascending
     *   }
     * }
     */
    userOutput.memberName = userData.user;

    userOutput.crownHeader = [["Date Seen", "# Bronze", "# Silver", "# Gold", "Total"]];
    var crownData = userData.crown.dataset.map(function (value) {
      return [value[1] * 1, value[2] * 1, value[3] * 1, value[4] * 1, (value[5] * 1 + value[2] * 1)];
    });
    // Ensure the data is sorted ascending by LastSeen.
    userOutput.crownData = crownData.sort(function (a, b) { return a[0] - b[0]; });

    userOutput.rankHeader = [["Date Ranked", "Rank", "# MHCC Crowns", "Date Seen"]];
    var rankData = userData.rank.dataset.map(function (value) {
      return [value[4] * 1, value[3] * 1, value[2] * 1, value[1] * 1];
    });
    // Ensure the data is sorted ascending by RankTime.
    userOutput.rankData = rankData.sort(function (a, b) { return a[0] - b[0]; });

    if (userOutput.crownHeader[0].length !== userOutput.crownData[0].length)
      throw new Error("Crown History is not rectangular");
    if (userOutput.rankHeader[0].length !== userOutput.rankData[0].length)
      throw new Error("Rank History is not rectangular");
  }

  if (!uids || uids === "" || uids === "undefined")
    throw new Error("No UID provided/loaded");

  if (uids.split(",").length > 5)
    throw new Error("UI for comparing that many members at once is not available.");

  // Obtain data for the UID(s).
  /** @type {Object <string, UserHistory>} */
  const dataForWebapp = {},
      history = getUserHistory_(uids, true);
  history.forEach(function (user) { _addUserData_(dataForWebapp, user); });

  if (!Object.keys(dataForWebapp).length)
    throw new Error("No data to be sent to webapp");
  if (Object.keys(dataForWebapp).length !== uids.split(",").length)
    console.warn({ "message": "At least 1 requested dataset is unavailable", "uids": uids.split(","), "data": dataForWebapp });

  // Due to object complexity, send as a string instead of a raw object.
  return JSON.stringify(dataForWebapp);
}
