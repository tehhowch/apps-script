/**
 * Apps Script code to handle the webapp.
 * devlink (v):
 * publink (v):
 */

/**
 * Runs when the script's app link is clicked
 * 
 * @param {{parameter: {String:String}, contextPath:String, contentLength:Number, queryString:null, parameters:{String:String[]}}} e
 *                    Object containing various properties. Each link will have at least one parameter labeled "uid", stored in e.parameter
 *                    Reference: https://developers.google.com/apps-script/guides/web#request_parameters
 * @return {HtmlOutput}  Returns a webpage described by assembling the various .html files
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
 * @param {String} uids      The UID of the member to query
 */
function loadUserData(uids)
{
  /**
   * Nested function which handles parsing the data for a given user into the properly-formatted webapp code.
   * 
   * @param {{String:{}}} outputToModify
   * @param {{name:String, crown:{}, rank:{}}} userData
   */
  function _addUserData_(outputToModify, userData)
  {
    var user = userData.name;
    if (!user) throw new Error("Missing username for whom to parse data.");
    // Create a property for this user in the parent's collection object
    outputToModify[String(user)] = {};
    // Bind a reference to it.
    const userOutput = outputToModify[String(user)];

    /**
     * Format the FusionTables data for consumption by the webapp.
     * userData: {
     *   "name": The user's display name
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
    userOutput.memberName = userData.name;

    userOutput.crownHeader = [["Date Seen", "# Bronze", "# Silver", "# Gold", "Total"]];
    var crownData = userData.crown.dataset.map(function (value) {
      return [value[1] * 1, value[2] * 1, value[3] * 1, value[4] * 1, (value[5] * 1 + value[2] * 1)];
    });
    // Ensure the data is sorted ascending by LastSeen.
    userOutput.crownData = crownData.sort();

    userOutput.rankHeader = [["Date Ranked", "Rank", "# MHCC Crowns", "Date Seen"]];
    var rankData = userData.rank.dataset.map(function (value) {
      return [value[4] * 1, value[3] * 1, value[2] * 1, value[1] * 1];
    });
    // Ensure the data is sorted ascending by RankTime.
    userOutput.rankData = rankData.sort();

    if (userOutput.crownHeader[0].length !== userOutput.crownData[0].length)
      throw new Error("Crown History is not rectangular");
    if (userOutput.rankHeader[0].length !== userOutput.rankData[0].length)
      throw new Error("Rank History is not rectangular");
  }

  if (!uids || uids === "" || uids === "undefined")
    throw new Error("No UID provided/loaded");
  
  if (uids.split(",").length > 2)
    throw new Error("UI for comparing more than 2 members at once is not available.");

  // Obtain data for the UID(s).
  var history = getUserHistory_(uids, true),
      dataForWebapp = {};
  history.forEach(function (user) { _addUserData_(dataForWebapp, user); });

  if (!Object.keys(dataForWebapp).length)
    throw new Error("No data to be sent to webapp");
  if (Object.keys(dataForWebapp).length !== uids.split(",").length)
    console.warn({ message: "At least 1 requested dataset is unavailable", uids: uids.split(","), data: dataForWebapp });

  return dataForWebapp;
}
