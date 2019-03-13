//@ts-check

/**
 * Returns a 2D array of the members' names and identifiers, from `start` until `limit`, or EOF.
 * @param  {number} [start] First retrieved index (relative to an alphabetical sort on members).
 * @param  {number} [limit] The maximum number of pairs to return.
 * @returns {Array <string>[]} The specified set of rows from the associated table
 */
function getUserBatch_(start, limit) {
  var sql = 'select * from ' + eliteUserTable + ' order by Member asc';
  if (start)
    sql += ' offset ' + start;
  if (limit)
    sql += ' limit ' + limit;
  return FusionTables.Query.sqlGet(sql).rows;
}

/**
 * Returns the record associated with the most recently touched snapshot for each current member.
 * Returns a single record per member, so long as every record has a different LastTouched value.
 * First finds each UID's maximum LastTouched value, then gets the associated row (after verifying
 * the member is current).
 * A query with "UID IN ( ... ) AND LastTouched IN ( ... )" would relax but not eliminate the
 * "unique LastTouched" requirement.
 * @return {Array[]}         N-by-crownDBnumColumns Array for UpdateScoreboard to parse.
 */
function getLatestRows_()
{
  // Query for only those members that are still current.
  /** @type {Object <string, string>} An association between a UID and the member's name */
  const valids = getUserBatch_(0).reduce(function (acc, pair) {
    if (acc.hasOwnProperty(pair[1]))
      console.warn(pair[1] + " is both '" + pair[0] + "' and '" + acc[pair[1]] + "'");
    else
      acc[pair[1]] = pair[0];
    return acc;
  }, {});
  const uids = Object.keys(valids);

  // Query for each member's most recently modified record. LastTouched is assumed (& required to be) unique.
  /** @type Array <[string,number]> */
  const mostRecent = [];
  const ltSQL = "select UID, MAXIMUM(LastTouched) from " + mhccCrownTable + " group by UID where UID IN (";
  do {
    var queried = [];
    do {
      queried.push(uids.pop());
    } while (uids.length && (ltSQL + queried.join(",") + ")").length < 8000);
    var mRbatch = FusionTables.Query.sqlGet(ltSQL + queried.join(",") + ")").rows;
    if (mRbatch)
      Array.prototype.push.apply(mostRecent, mRbatch);
  } while (uids.length);

  // Query the full record for the target table, using the uniqueness of the LastTouched
  // value to avoid needing to also restrict with `WHERE UID in (...)` (i.e. we use fewer queries).
  const snapshots = [];
  // The maximum SQL request length for the API is ~8100 characters (via POST), e.g. ~571 17-digit time values.
  const totalQueries = 1 + Math.ceil(mostRecent.length / 571);
  const baseSQL = "SELECT * FROM " + mhccCrownTable + " WHERE LastTouched IN (";
  const tail = ") ORDER BY Member ASC";

  do {
    var batchStart = new Date().getTime();
    // Assemble the query.
    var sql = "", lastTouched = [];
    do {
      lastTouched.push(mostRecent.pop()[1]);
      sql = baseSQL + lastTouched.join(",") + tail;
    } while (mostRecent.length > 0 && sql.length < 8000);

    // Execute the query & store the resulting rows.
    try { Array.prototype.push.apply(snapshots, FusionTables.Query.sqlGet(sql).rows); }
    catch (err)
    {
      console.error({ "message": "Error while fetching whole rows", "sql": sql,
          "error": err, "remainingUIDs": mostRecent.map(function (mem) { return mem[1]; }) });
      return [];
    }

    // Sleep to avoid exceeding FusionTable ratelimits (30 / min, 5 / sec).
    var elapsed = new Date().getTime() - batchStart;
    if (totalQueries > 29)
      Utilities.sleep(2001 - elapsed);
    else if (elapsed < 200)
      Utilities.sleep(201 - elapsed);
  } while (mostRecent.length > 0);

  // Log any current members who weren't included in the fetched data.
  if (snapshots.length !== Object.keys(valids).length)
  {
    // Prune `valids` to only those members without obtained data. (UID is in index 1).
    snapshots.forEach(function (record) { delete valids[record[1]]; });
    console.log({ "message": "Some members lack MHCC Crown DB records", "data": valids });
  }
  return snapshots;
}
