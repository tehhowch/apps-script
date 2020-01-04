//@ts-check
/**
 * Interface for reading MHCC data from BigQuery, such as users, crowns, and ranks
 */

/**
 * TODO
 * - expose finding members within 5 ranks of the current UID's rank (to compare crowns & rank histories)
 * - support current fusiontable method's interface
 */
// #region webapp-invoked methods
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
// #endregion


// #region query
/**
 * Workhorse function for performing a fully-formed StandardSQL query
 * @param {string} sql The query to execute on a table in this project
 * @returns {{ rows: Array<(string|number)[]>, columns: string[] }} Query results (rows) and labels (cols)
 */
function bq_querySync_(sql)
{
  var job = Bigquery.newJob();
  job = {
    configuration: {
      query: {
        query: sql,
        useLegacySql: false,
      }
    }
  };
  const queryJob = Bigquery.Jobs.insert(job, projectKey);
  var queryResult = Bigquery.Jobs.getQueryResults(projectKey, queryJob.jobReference.jobId);
  while (!queryResult.jobComplete)
  {
    queryResult = Bigquery.Jobs.getQueryResults(projectKey, queryJob.jobReference.jobId);
  }
  var pages = 1;
  console.log({
    message: 'Query Completed',
    received: queryJob,
    cacheHit: queryResult.cacheHit,
    resultCount: queryResult.totalRows,
    queryBytes: queryResult.totalBytesProcessed,
    hasPages: !!queryResult.pageToken,
    firstResult: queryResult.rows ? queryResult.rows[0] : null
  });
  // Return the headers to the caller.
  const headers = [];
  Array.prototype.push.apply(headers, queryResult.schema.fields
    .map(function (schemaField) { return schemaField.name; }));

  // Compute type-coercion functions from the column schema.
  const formatters = queryResult.schema.fields.map(function (colSchema) {
    var type = colSchema.type.toLowerCase();
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
  if (pages !== 1) console.log({ message: 'Queried results from ' + pages + ' pages'});
  return {
    columns: headers,
    rows: results.map(function (row) { return row.f.map(
      function (col, idx) {
        return formatters[idx].fn(col.v);
      });
    }),
  };
}

/**
 * @param {'Core'|'Elite'} [datasetName] The dataset containing the target Members table (default "Core").
 */
function bq_getMembers_(datasetName)
{
  if (!datasetName) datasetName = 'Core';
  const tableId = [dataProject, datasetName, 'Members'].join('.');
  const sql = 'SELECT Member, UID FROM `' + tableId + '` ORDER BY Member ASC';
  return bq_querySync_(sql).rows;
}

/**
 * Obtain the latest modified rows from the given table. The table MUST have keys UID, LastTouched
 * MAYBE: pass in the 2nd required column, to allow use on other tables.
 * @param {string} dataset The dataProject's dataset to query
 * @param {string} table the table name to query in the given dataset
 */
function bq_getLatestRows_(dataset, table)
{
  // Use BQ to do the per-member UID-LastTouched limiting via an INNER JOIN:
  const tableId = [dataProject, dataset, table].join('.');
  // Require the order of returned columns to be the same as that needed to upload
  // to this table.
  const columns = bq_getTableColumns_(dataProject, dataset, table).join(', ');
  const sql = 'SELECT ' + columns + ' FROM `' + tableId + '` JOIN (SELECT UID, MAX(LastTouched) AS LastTouched FROM `' + tableId + '` GROUP BY UID ) USING (LastTouched, UID)'

  const latestRows = bq_querySync_(sql);
  if (!latestRows || !latestRows.rows.length) throw new Error('Unable to retrieve latest records for all users');
  return latestRows;
}

/**
 * Query the given table for the per-uid max values of the LastSeen, LastCrown, and LastTouched columns
 * @param {string} dataset
 * @param {string} table
 * @returns {[string, number, number, number][]}
 */
function bq_getLatestTimes_(dataset, table)
{
  const tableId = [dataProject, dataset, table].join('.');
  const sql = 'SELECT UID, MAX(LastSeen), MAX(LastCrown), MAX(LastTouched) FROM `' + tableId + '` GROUP BY UID';
  return bq_querySync_(sql).rows;
}

/**
 * Get the per-uid max values of LastSeen, LastCrown, and LastTouched columns for the given users only.
 * @param {string} dataset
 * @param {string} table
 * @param {string[]} uids
 */
function bq_getLatestBatch_(dataset, table, uids)
{
  const batchHash = uids.reduce(function (hash, uid) {
    hash[uid] = true;
    return hash;
  }, {});
  // To maximize query cache usage, just query everything and filter locally.
  const allMemberTimes = bq_getLatestTimes_(dataset, table);
  return allMemberTimes.filter(function (row) { return batchHash[row[0]]; });
}
// #endregion

// #region metadata
/**
 * @param {string} project Google Cloud Project in which the dataset and table reside
 * @param {string} dataset Dataset in which the table resides
 * @param {string} table Table name to request all column names for
 * @returns {string[]}
 */
function bq_getTableColumns_(project, dataset, table)
{
  const metadata = Bigquery.Tables.get(project, dataset, table);
  return metadata.schema.fields.map(function (colSchema) {
    return colSchema.name;
  });
}
// #endregion
