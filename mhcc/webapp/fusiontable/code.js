var ftid = "";
var rankTableId = "";

/**
 * @typedef {Object} PlotData
 * @property {string[]} headers Headers for the column data.
 * @property {(string|number)[][]} dataset Column data to plot.
 */
/**
 * @typedef {Object} UserData
 * @property {string} uid MHCC identifier that refers to this user.
 * @property {string} user Display name used for this user.
 * @property {PlotData} crown History of the user's crown counts.
 * @property {PlotData} rank History of the user's rank within MHCC.
 */
/**
 * Server-side function which queries the various FusionTables to acquire the input users' crown and rank history.
 * 
 * @param  {string} uids The (comma-separated) UID string with specific members for whom the data snapshots are returned.
 * @param  {boolean} [blGroup] Optional parameter controlling GROUP BY or "return all" behavior. Generally true.
 * @return {UserData[]}  An array of user data objects, each containing at minimum "user", "crown", and "rank".
 */
function getUserHistory_(uids, blGroup)
{
  if (!uids)
    throw new Error('No UID provided');
  uids = String(uids);

  const labels = ["crown", "rank"];
  const selects = [
    "SELECT UID, Member, LastSeen, Bronze, Silver, Gold, MHCC FROM " + ftid,
    "SELECT UID, Member, LastSeen, 'MHCC Crowns', Rank, RankTime FROM " + rankTableId
  ];
  const where = "WHERE UID IN (" + uids + ")";
  const groups = [
    "GROUP BY UID, Member, LastSeen, Bronze, Silver, Gold, MHCC",
    "GROUP BY UID, Member, LastSeen, 'MHCC Crowns', Rank, RankTime"
  ];
  const orders = [
    "ORDER BY UID ASC, LastSeen ASC",
    "ORDER BY UID ASC, RankTime ASC"
  ];
  
  const sqls = [];
  if (blGroup && selects.length === groups.length && groups.length === orders.length)
  {
    for (var i = 0, numClauses = selects.length; i < numClauses; ++i)
      sqls.push([selects[i], where, groups[i], orders[i]].join(" "));
  }
  else
    selects.forEach(function (selectClause, i) { sqls.push([selectClause, where, orders[i]].join(" ")); });

  /** @type {Object <string, PlotData>} */
  const queryData = {};
  sqls.forEach(function (sql, index) {
    var resp = FusionTables.Query.sqlGet(sql, { quotaUser: uids });
    if (!resp || !resp.rows || !resp.rows.length || !resp.columns)
      throw new Error("No data for input=" + uids + " in the " + labels[index] + " datasource.");

    queryData[labels[index]] = { "headers": resp.columns, "dataset": resp.rows };
  });

  // Organize the query data by its associated member.
  var members = uids.split(","), output = [];
  members.forEach(function (id) {
    /** @type {UserData} */
    var memberOutput = { "uid": id };
    labels.forEach(function (l) {
      var memberData = queryData[l].dataset.filter(function (row) { return String(row[0]) === id; });
      memberOutput[l] = {
        // Do not include the member's UID in the column data sent to the webapp.
        "headers": queryData[l].headers.slice(1),
        "dataset": memberData.map(function (row) { return row.slice(1); })
      };
    });
    // Read the user's name from the last entry in the "crown" dataset (could query the Members FusionTable).
    memberOutput.user = memberOutput["crown"].dataset.slice(-1)[0][0];
    output.push(memberOutput);
  });
  
  return output;
}
