var ftid = "";
var alt_table = "";

/**
 * function getUserHistory_   Queries the crown data snapshots for the given user and returns crown
 *                            counts and rank data as a function of HT MostMice's LastSeen property.
 * @param  {String} UID       The UIDs of specific members for which the crown data snapshots are
 *                            returned. If multiple members are queried, use comma separators.
 * @param  {Boolean} blGroup  Optional parameter controlling GROUP BY or "return all" behavior.
 * @return {Object}           An object containing "user" (member's name), "headers", and "dataset".
 */
function getUserHistory_(UID, blGroup)
{
  var sql = 'SELECT Member, LastSeen, Bronze, Silver, Gold, MHCC, Rank, ';
  if (!UID)
    throw new Error('No UID provided');
  
  if (blGroup === true)
    sql += "MINIMUM(RankTime) FROM " + ftid + " WHERE UID IN (" + UID.toString() + ") GROUP BY Member, LastSeen, Bronze, Silver, Gold, MHCC, Rank";
  else
    sql += "RankTime FROM " + ftid + " WHERE UID IN (" + UID.toString() + ")";

  sql += " ORDER BY LastSeen ASC";
  var resp = FusionTables.Query.sqlGet(sql, {quotaUser: UID});
  if (!resp || !resp.rows || !resp.rows.length)
    throw new Error('No data for UID=' + UID);

  // Use the most recent name for the user, since they may have changed it.
  return {"user": resp.rows[resp.rows.length - 1][0], "headers": resp.columns, "dataset": resp.rows};
}
