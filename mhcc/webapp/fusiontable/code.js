var ftid = "";
var rankTableId = "";

/**
 * Query the crown data snapshots for the given users to return crown counts and rank data.
 * 
 * @param  {String} uids                         The (comma-separated) UID string with specific members for whom the data snapshots are returned.
 * @param  {Boolean} blGroup                     Optional parameter controlling GROUP BY or "return all" behavior. Generally true.
 * @return {[{user:String, crown:{}, rank:{}}]}  An array of user data objects, each containing at minimum "user", "crown", and "rank".
 */
function getUserHistory_(uids, blGroup)
{
  if (!uids)
    throw new Error('No UID provided');
  uids = String(uids);

  var labels = ["crown", "rank"];
  var selects = [
    "SELECT UID, Member, LastSeen, Bronze, Silver, Gold, MHCC FROM " + ftid,
    "SELECT UID, Member, LastSeen, MHCC Crowns, Rank, RankTime FROM " + rankTableId
  ];
  var where = "WHERE UID IN (" + uids + ")";
  var groups = [
    "GROUP BY UID, Member, LastSeen, Bronze, Silver, Gold, MHCC",
    "GROUP BY UID, Member, LastSeen, MHCC Crowns, Rank, RankTime"
  ];
  var orders = ["ORDER BY UID ASC, LastSeen ASC", "ORDER BY UID ASC, RankTime ASC"];
  
  var sqls = [];
  if (blGroup && selects.length === groups.length && groups.length === orders.length)
  {
    for (var i = 0, numClauses = selects.length; i < numClauses; ++i)
      sqls.push([selects[i], where, groups[i], orders[i]].join(" "));
  }
  else
    selects.forEach(function (selectClause, i) { sqls.push([selectClause, where, orders[i]].join(" ")); });

  var queryData = {};
  sqls.forEach(function (sql, index) {
    var resp = FusionTables.Query.sqlGet(sql, { quotaUser: uids });
    if (!resp || !resp.rows || !resp.rows.length || !resp.columns)
      throw new Error("No data for input=" + uids + " in the " + labels[index] + " datasource.");

    queryData[labels[index]] = { "headers": resp.columns, "dataset": resp.rows };
  });
  // Organize the query data by its associated member.
  var members = uids.split(","), output = [];
  members.forEach(function (id) {
    var memberOutput = { "uid": id };
    labels.forEach(function (l) {
      var memberData = queryData[l].dataset.filter(function (row) { return row[0] === id; });
      memberOutput[l] = {
        "headers": queryData[l].headers,
        // Do not include the member's UID in the column data sent to the webapp.
        "dataset": memberData.map(function (row) { return row.slice(1); })
      };
    });
    // Read the user's name from the last entry in the "crown" dataset (could query the Members FusionTable).
    memberOutput.user = memberOutput["crown"].dataset.slice(-1)[0][0];
    output.push(memberOutput);
  });
  
  return output;
}
