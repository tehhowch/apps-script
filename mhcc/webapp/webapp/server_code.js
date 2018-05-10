/*
 * Apps Script code to handle the webapp.
 * devlink (v):
 * publink (v):
 */

/*
 * function doGet     Runs when the script's app link is clicked
 * @param {Object} e  Object containing various properties. Each link will have at least one parameter labeled uid
 *                    which is stored in e.parameter
 *                    {parameter={}, contextPath=, contentLength=-1, queryString=null, parameters={[]}}
 * @return {webpage}  Returns a webpage described by assembling the various .html files
 */
function doGet(e)
{
  var pg = HtmlService.createTemplateFromFile("webapp\\page");
  pg.webAppUID = e.parameter.uid;
  return pg.evaluate().setTitle("MHCC Crown History").setFaviconUrl("https://i.imgur.com/QMghA1l.png");
}
/*
 * function loadUserData    gets the crown and rank data from FusionTables for use by the webapp's plotters
 * @param {String} uid      The UID of the member to query
 */
function loadUserData(uid)
{
  if (!uid || uid === "" || uid === "undefined")
    throw new Error("No UID provided/loaded");
  
  switch (uid.split(",").length)
  {
    case 1:
      var history = getUserHistory_(uid, true);
      /* history = {"user":MemberName,
      *            "headers":[Member, LastSeen, Bronze, Silver, Gold, MHCC, Rank, RankTime]
      *            "dataset":[][] ordered by LastSeen ascending
      *           }*/
      // Split this object and format it for the webapp
      var output = {};
      output.memberName = history.user;
      
      var crownData = history.dataset.map(function(value, index) {
        return [value[1] * 1, value[2] * 1, value[3] * 1, value[4] * 1, (value[5] * 1 + value[2] * 1)];
      });
      output.crownHeader = [["Date Seen", "# Bronze", "# Silver", "# Gold", "Total"]];
      output.crownData = crownData.sort();
      if (output.crownHeader[0].length !== output.crownData[0].length)
        throw new Error("Crown History is not rectangular");
      
      var rankData = history.dataset.map(function(value, index) {
        return [value[7] * 1, value[6] * 1, value[5] * 1];
      });
      output.rankHeader = [["Date Ranked", "Rank", "# MHCC Crowns"]];
      output.rankData = rankData.sort();
      if (output.rankHeader[0].length !== output.rankData[0].length)
        throw new Error("Rank History is not rectangular");
      
      output.length = crownData.length;
      break;
    default:
      throw new Error("Multi-member compare is not yet written");
      break;
  }
  return output;
}
