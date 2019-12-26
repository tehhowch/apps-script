//@ts-check
// Implementation details for the FusionTable interface
/**
 * Construct the desired queries to get the rowids for records with LastSeen values that fall
 * within the given datespan. If the query would exceed the allowed POST length (~8000 char)
 * then multiple queries will be returned.
 *
 * @param {Array <string>[]} members 2D array of Name | Link | UID for which to obtain Crown data
 * @param {Date} dateStart     Beginning Date object for when the member needed to have been seen.
 * @param {Date} dateEnd       Ending Date object for when the member needed to have been seen before.
 * @return {string[]}          Array of SQL queries that request member rows between the desired dates.
 */
function getRowidQueries_(members, dateStart, dateEnd)
{
  if (!members || !members.length || dateStart.getTime() === dateEnd.getTime())
  {
    console.warn({message: "Insufficient data for querying", data: {members: members, dateStart: dateStart, dateEnd: dateEnd}});
    return [];
  }

  const queries = [];
  const memUIDs = members.map(function (value) { return value[2]; });
  var SQL = "SELECT ROWID, UID, LastSeen, LastTouched FROM " + ftid + " WHERE LastSeen < " + dateEnd.getTime();
  SQL += " AND LastSeen >= " + dateStart.getTime() + " AND UID IN (";
  const sqlEnd = ") ORDER BY UID ASC, LastSeen ASC, LastTouched ASC";
  while (memUIDs.length)
  {
    queries.push(SQL);
    var sqlUIDs = [];
    var q = queries[queries.length - 1];
    do {
      sqlUIDs.push(memUIDs.pop());
    } while ((q + sqlUIDs.join(",") + sqlEnd).length < 8000 && memUIDs.length);

    queries[queries.length - 1] += sqlUIDs.join(",") + sqlEnd;
  }
  return queries;
}


/**
 * Extract the desired rowids from the day's data.
 *
 * @param {Array[]} queryData [[ROWID | Member ID (ascending) | LastSeen (ascending) | LastTouched (ascending)]]
 * @return {string[]} Array of rowid information from the given records
 */
function extractROWIDs_(queryData)
{
  if (!queryData || !queryData.length || queryData[0].length !== 4)
    return [];

  // Iterate rows and keep the first value for each new member for each LS
  // (i.e. the first record of a new LastSeen instance).
  const rowids = [];
  const seen = {};
  for (var row = 0, len = queryData.length; row < len; ++row)
  {
    try {
      // Check if the member and this LastSeen is known.
      var uid = queryData[row][1];
      var ls = new Date(queryData[row][2]);
      // If we have seen this particular LastSeen, we don't need to collect this next record.
      if (seen[uid] && seen[uid][ls])
        continue;
      else
      {
        // This is a new rowid that needs to be fetched.
        rowids.push(queryData[row][0]);
        // Update the tracking container.
        if (!seen[uid])
          seen[uid] = {};
        seen[uid][ls] = true;
      }
    }
    catch(e) { console.error({error: e, data: {row: row, data: queryData, seen: seen}}); }
  }
  return rowids;
}


/**
 * Convert the input array of rowids into queries for the desired data.
 * [['Member', 'Link', 'Date', 'Last Seen', 'Last Crown', 'Gold', 'Silver', 'Bronze']];
 * @param {string[]} rowids The rowids of records to be obtained
 * @return {string[]} The SQL queries to execute in order to obtain all desired rowids.
 */
function getRowQueries_(rowids)
{
  if (!rowids || !rowids.length || !rowids[0].length)
    return [];

  const queries = [];
  const SQL = "SELECT Member, UID, LastTouched, LastSeen, LastCrown, Gold, Silver, Bronze FROM " + ftid + " WHERE ROWID IN (";
  const sqlEnd = ") ORDER BY LastTouched ASC";
  do {
    queries.push(SQL);
    var sqlRowIDs = [];
    var q = queries[queries.length - 1];
    do {
      sqlRowIDs.push(rowids.pop());
    } while ((q + sqlRowIDs.join(",") + sqlEnd).length < 8000 && rowids.length);

    queries[queries.length - 1] += sqlRowIDs.join(",") + sqlEnd;
  } while(rowids.length);

  return queries;
}


/**
 * Execute the given queries, and return the aggregated row response.
 * @param {string[]} queries SQL GET queries to be executed by the FusionTables service
 * @return {Array[]} the aggregated data records
 */
function doSQLGET_(queries)
{
  if (!queries || !queries.length || !queries[0].length)
    return;

  const data = [];
  do {
    var sql = queries.pop();
    if (!sql.length)
      console.error("No query to perform");
    else
    {
      try
      {
        var response = FusionTables.Query.sqlGet(sql);
        if (response.rows)
          Array.prototype.push.apply(data, response.rows);
      }
      catch(e)
      {
        console.error({message: 'SQL get error from FusionTables', params: {error: e, query: sql, remaining: queries}});
        // Re-raise the error.
        throw e;
      }
    }
    // Obey API rate limits.
    if (queries.length)
      Utilities.sleep(500);
  } while (queries.length);

  console.log({data: data, length: data.length});
  return data;
}
