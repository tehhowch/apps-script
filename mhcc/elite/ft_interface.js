//@ts-check

/**
 * Returns a 2D array of the members' names and identifiers, from `start` until `limit`, or EOF.
 * @param  {number} [start] First retrieved index (relative to an alphabetical sort on members).
 * @param  {number} [limit] The maximum number of pairs to return.
 * @returns {Array <string>[]} The specified set of rows from the associated table
 */
function getUserBatch_(start, limit)
{
  var sql = 'select * from ' + eliteUserTable + ' order by Member asc';
  if (start)
    sql += ' offset ' + start;
  if (limit)
    sql += ' limit ' + limit;
  return FusionTables.Query.sqlGet(sql).rows;
}

/**
 * Obtain the most recent ranking time for the given Fusion Table.
 * @param {string} tableId The ___ Rank Fusion Table to query
 */
function getLatestRankTime_(tableId)
{
  const sql = 'select maximum(RankTime) from ' + tableId;
  var resp;
  try
  {
    resp = FusionTables.Query.sqlGet(sql);
  }
  catch (err)
  {
    console.warn({message: "RankTime query failed", sql: sql, error: err, stack: err.stack});
  }
  return (resp && resp.rows && resp.rows.length) ? resp.rows[0][0] : 0;
}

/**
 * Returns the MHCC record associated with the most recently touched snapshot for each current member.
 * Returns a single record per member, so long as every record has a different LastTouched value.
 * First finds each member UID's maximum `LastTouched` value, then gets the associated row.
 * A query with "UID IN ( ... ) AND LastTouched IN ( ... )" would relax but not eliminate the
 * "unique LastTouched" requirement.
 * @return {Array[]} N-by-crownDBnumColumns Array for scoreboard parsing.
 */
function getLatestMHCCRows_()
{
  // Query for only those members that are still current.
  /** @type {Object <string, string>} An association between a UID and the member's name */
  const valids = getUserBatch_(0).reduce(function (acc, pair)
  {
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
  const ltSQL = "select UID, MAXIMUM(LastTouched) from " + mhccCrownTable + " where UID IN (";
  do {
    var queried = [];
    do {
      queried.push(uids.pop());
    } while (uids.length && (ltSQL + queried.join(",") + ")").length < 8000);
    var mRbatch = FusionTables.Query.sqlGet(ltSQL + queried.join(",") + ") group by UID").rows;
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

  // Replace MHCC group names with the members' preferred Elite MHCC names.
  snapshots.forEach(function (record) { record[0] = valids[record[1]]; });

  // Log any current members who weren't included in the fetched data.
  if (snapshots.length !== Object.keys(valids).length)
  {
    // Prune `valids` to only those members without obtained data. (UID is in index 1).
    snapshots.forEach(function (record) { delete valids[record[1]]; });
    console.warn({ "message": "Some members lack MHCC Crown DB records", "data": valids });
  }
  return snapshots;
}

/**
 * Compute the latest scoreboard rows using the most up-to-date information from the MHCC database.
 * Submits the computed rows to the Elite Rank DB FusionTable for archiving.
 * @param {{bronze: number, silver: number, gold: number}} scoreFor The number of points earned for a crown of each type
 * @param {{gold: number, gs: number, total: number}} minimum The minimum crown counts needed of each type.
 * @returns {Array[]} The ordered scoreboard rows, for spreadsheet serialization.
 */
function getLatestEliteScoreboardRows_(scoreFor, minimum)
{
  // Create an array of object data from the Fusion Tables row data.
  const recordData = getLatestMHCCRows_().map(function (mhcc)
  {
    // MHCC: [Name, UID, LastSeen, LastCrown, LastTouched, Bronze, Silver, Gold, MHCC, Squirrel]
    var data = {
      name: mhcc[0],
      uid: mhcc[1].toString(),
      link: ("https://www.mousehuntgame.com/profile.php?snuid=" + mhcc[1]),
      seen: parseInt(mhcc[2], 10),
      lastCrown: parseInt(mhcc[3], 10),
      bronze: parseInt(mhcc[5], 10),
      silver: parseInt(mhcc[6], 10),
      gold: parseInt(mhcc[7], 10)
    };
    data.gs = data.gold + data.silver;
    data.total = data.gs + data.bronze;

    var comment = "", points = 0;
    if (data.gold < minimum.gold)
    {
      comment = "Need " + (minimum.gold - data.gold) + " more Gold";
      points = data.gold;
    }
    else if (data.gs < minimum.gs)
    {
      comment = "Need " + (minimum.gs - data.gs) + " more Silver";
      points = data.gold * 2;
    }
    else if (data.total < minimum.total)
    {
      comment = "Need " + (minimum.total - data.total) + " more Bronze";
      points = data.gold * 3 + data.silver;
    }
    else
      points = data.gold * scoreFor.gold + data.silver * scoreFor.silver + data.bronze * scoreFor.bronze;

    data.points = points;
    data.comment = comment;
    return data;
  });

  // Create & format the scoreboard records.
  // TODO: Add hyperlink to Point/Rank history
  const records = recordData.map(function (data) {
    return [
      0,
      data.name,
      data.link,
      '=HYPERLINK("' + data.link + '", "' + data.name.replace(/"/g, '""') + '")',
      data.gold,
      data.gs,
      data.total,
      data.points,
      data.comment,
      Utilities.formatDate(new Date(data.seen), "EST", "yyyy-MM-dd"),
      Utilities.formatDate(new Date(data.lastCrown), "EST", "yyyy-MM-dd")
    ];
  });
  if (!records.length)
  {
    console.log("Aborting Scoreboard update due to no retrieved MHCC records");
    return;
  }

  // Sort scoreboard records by points, descending.
  records.sort(function (a, b)
  {
    var pointDiff = b[7] - a[7];
    if (pointDiff)
      return pointDiff;
    var goldDiff = b[4] - a[4];
    if (goldDiff)
      return goldDiff;
    var gsDiff = b[5] - a[5];
    return (gsDiff ? gsDiff : b[6] - a[6]);
  });
  // Assign the member ranking.
  var rank = 0;
  records.forEach(function (record) { record[0] = ++rank; });

  try
  {
    // Create the Rank DB submissions using the ranked records, and the non-formatted
    // record data.
    const rankTime = new Date().getTime();
    const recordsObj = recordData.reduce(function (obj, data) {
      obj[data.uid] = data;
      return obj;
    }, {});
    const submissions = records.map(function (record) {
      // [Name, UID, LastSeen, RankTime, Rank, Points, Comment]
      var uid = record[2].slice(record[2].search("=") + 1).toString();
      var obj = recordsObj[uid];
      return [
        obj.name,
        uid,
        obj.seen,
        rankTime,
        record[0],
        obj.points,
        obj.comment
      ];
    });
    ftBatchWrite_(submissions, eliteRankTable);
  }
  catch (err)
  {
    console.warn({ message: "Unable to write to Elite Rank DB", id: eliteRankTable,
        error: {msg: err.mesage, stack: err.stack.split("\n")} });
  }
  return records;
}

/**
 * Construct a CSV representation of an array. Adds quoting and escaping as needed.
 * @param  {Array[]} myArr A 2D array to be converted into a CSV string
 * @return {string} A string representing the rows of the input array, joined by CRLF.
 */
function array2CSV_(myArr)
{
  return myArr.map(_row4CSV_).join("\r\n");

  /**
 * Ensure the given value is CSV-compatible, by escaping special characters and adding double-quotes if needed.
 * @param  {any} value An array element to be encapsulated into a CSV string.
 * @return {string} A string that will interpreted as a single element by a CSV reader.
 */
  function _val4CSV_(value)
  {
    const str = (typeof value === 'string') ? value : value.toString();
    if (str.indexOf(',') !== -1 || str.indexOf("\n") !== -1 || str.indexOf('"') !== -1)
      return '"' + str.replace(/"/g, '""') + '"';
    else
      return str;
  }
  /**
 * Construct a CSV representation of in the input array.
 * @param  {Array} row A 1-D array of elements which may be strings or other types which support toString().
 * @return {string} A string that will be interpreted as a single row by a CSV reader.
 */
  function _row4CSV_(row) { return row.map(_val4CSV_).join(","); }
}

/**
 * Convert the data array into a CSV blob and upload to FusionTables.
 * @param  {Array[]} newData  The 2D array of data that will be written to the database.
 * @param  {string}  tableId  The table to which the batch data should be written.
 * @param  {boolean} [strict=true] If the number of columns must match the table schema (default true).
 * @return {number} The number of rows that were added to the FusionTable database.
 */
function ftBatchWrite_(newData, tableId, strict)
{
  if (!tableId) return;
  const options = { isStrict: strict !== false };

  const dataAsCSV = array2CSV_(newData);
  var dataAsBlob;
  try { dataAsBlob = Utilities.newBlob(dataAsCSV, "application/octet-stream"); }
  catch (e)
  {
    e.message = "Unable to convert array into CSV format: " + e.message;
    console.error({ "message": e.message, "input": newData, "csv": dataAsCSV, "errstack": e.stack.split("\n") });
    throw e;
  }

  try { return parseInt(FusionTables.Table.importRows(tableId, dataAsBlob, options).numRowsReceived, 10); }
  catch (e)
  {
    e.message = "Unable to upload rows: " + e.message;
    throw e;
  }
}
