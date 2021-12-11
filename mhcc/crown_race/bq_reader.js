//@ts-check
/**
 * Interface for communicating with Google BigQuery resources associated with the MHCC project
 */

// #region query
/**
 * Workhorse function for performing a fully-formed StandardSQL query
 * @param {string} sql The query to execute on a table in this project
 * @returns {{ rows: Array<(string|number)[]>, columns: string[] }} Query results (rows) and labels (cols)
 */
function bq_querySync_(sql)
{
  let job = Bigquery.newJob();
  job = {
    configuration: {
      query: {
        query: sql,
        useLegacySql: false,
      }
    }
  };
  const queryJob = Bigquery.Jobs.insert(job, projectKey);
  let queryResult = Bigquery.Jobs.getQueryResults(projectKey, queryJob.jobReference.jobId);
  while (!queryResult.jobComplete)
  {
    queryResult = Bigquery.Jobs.getQueryResults(projectKey, queryJob.jobReference.jobId);
  }
  let pages = 1;
  console.log({
    message: 'Query Completed',
    cacheHit: queryResult.cacheHit,
    resultCount: queryResult.totalRows,
    queryBytes: queryResult.totalBytesProcessed,
    hasPages: !!queryResult.pageToken,
    firstResult: queryResult.rows ? queryResult.rows[0] : null
  });
  // Return the headers to the caller.
  const headers = queryResult.schema.fields.map(({ name }) => name);

  // Compute type-coercion functions from the column schema.
  const formatters = queryResult.schema.fields.map((colSchema) => {
    const type = colSchema.type.toLowerCase();
    if (type === 'float' || type === 'float64') {
      return { fn: function (value) { return parseFloat(value); } };
    } else if (type === 'numeric' || type === 'integer' || type === 'int64' || type === 'integer64') {
      return { fn: function (value) { return parseInt(value, 10); } };
    } else {
      return { fn: function (value) { return value; } };
    }
  });
  const results = [];
  Array.prototype.push.apply(results, queryResult.rows);
  while (queryResult.pageToken)
  {
    queryResult = Bigquery.Jobs.getQueryResults(projectKey, queryJob.jobReference.jobId, { pageToken: queryResult.pageToken });
    Array.prototype.push.apply(results, queryResult.rows);
    ++pages;
  }
  if (pages !== 1) console.log(`Queried results from ${pages} pages`);
  return {
    columns: headers,
    rows: results.map((row) => row.f.map((col, idx) => formatters[idx].fn(col.v))),
  };
}

/**
 * Obtains the first record for each member's LastSeen value that falls within the input date range [startDate, endDate)
 * returns `[UID, LastTouched, LastSeen, LastCrown, Gold, Silver, Bronze][]`
 * @param {string[]} uids The competitors for whom the rows should belong
 * @param {Date} startDate The Date before which no records should be returned
 * @param {Date} endDate The Date after which no records should be returned
 */
function bq_getRowsSeenInRange_(uids, startDate, endDate)
{
  if (!uids.length || !uids.every((uid) => typeof uid === 'string')) {
    console.warn({ message: 'Invalid UID input', uids });
    throw new Error('Invalid UIDs given for querying');
  }
  const tableId = [dataProject, 'Core', 'Crowns'].join('.');
  const sql = [
      'SELECT t.UID, t.LastTouched, t.LastSeen, t.LastCrown, t.Gold, t.Silver, t.Bronze FROM',
      ' `' + tableId + '` t JOIN (',
        'SELECT ti.UID, ti.LastSeen, MIN(ti.LastTouched) LastTouched FROM `' + tableId + '` ti',
        ' WHERE ti.UID IN ("' + uids.join('","') + '")',
        ' AND ti.LastSeen >= ' + startDate.getTime(),
        ' AND ti.LastSeen < ' + endDate.getTime(),
        ' GROUP BY ti.UID, ti.LastSeen',
      ') t2 USING (UID, LastSeen, LastTouched)',
      ' ORDER BY t.LastTouched ASC',
  ].join('');

  return bq_querySync_(sql).rows;
}
// #endregion
