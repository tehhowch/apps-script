/**
 * Interface for communicating with Google BigQuery resources associated with the MHCC project
 */

// #region query
/**
 * Workhorse function for performing a fully-formed StandardSQL query
 * @param {string} sql The query to execute on a table in this project
 * @returns {{ rows: Array<(string|number)[]>, columns: string[] }} Query results (rows) and labels (cols)
 */
function bq_querySync_(sql, dataset, table)
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
  const queryJob = Bigquery.Jobs.insert(job, bqKey);
  var queryResult = Bigquery.Jobs.getQueryResults(bqKey, queryJob.jobReference.jobId);
  while (!queryResult.jobComplete)
  {
    queryResult = Bigquery.Jobs.getQueryResults(bqKey, queryJob.jobReference.jobId);
  }
  var pages = 1;
  console.log({
    message: 'Query Completed',
    submitted: job,
    received: queryJob,
    cacheHit: queryResult.cacheHit,
    resultCount: queryResult.totalRows,
    queryBytes: queryResult.totalBytesProcessed,
    hasPages: !!queryResult.pageToken,
    firstResult: queryResult.rows ? queryResult.rows[0] : null
  });
  const headers = [];
  Array.prototype.push.apply(headers, queryResult.schema.fields
    .map(function (schemaField) { return schemaField.name; }));
  const results = [];
  Array.prototype.push.apply(results, queryResult.rows);
  while (queryResult.pageToken)
  {
    queryResult = Bigquery.Jobs.getQueryResults(bqKey, queryJob.jobReference.jobId, { pageToken: queryResult.pageToken });
    Array.prototype.push.apply(results, queryResult.rows);
    ++pages;
  }
  if (pages !== 1) console.log({ message: 'Queried results from ' + pages + ' pages'});
  return {
    columns: headers,
    rows: results.map(function (row) { return row.f.map(function (col) { return col.v; })}),
  };
}

/**
 * @param {number} [start] The offset into the member list to retreive
 * @param {number} [limit] The maximum number of members to retrieve
 */
function bq_getMemberBatch_(start, limit)
{
  const sql = 'SELECT Member, UID FROM `' + bqKey + '.Core.Members` ORDER BY Member ASC';
  const members = bq_querySync_(sql, 'Core', 'Members').rows;
  if (start === undefined && limit === undefined)
    return members;

  start = Math.abs(parseInt(start || 0, 10));
  if (limit <= 0) limit = 1;
  limit = parseInt(limit || 100000, 10);
  return members.slice(start, start + limit);
}

/**
 * Read the MHCT Dataset table for extension-reported crown counts.
 * @param {string[]} uids UIDs to query
 */
function bq_readMHCTCrowns_(uids)
{
  const sql = 'SELECT * FROM `' + bqKey + '.MHCT.CrownCounts` WHERE snuid IN ("' + uids.join('","') + '")';
  return bq_querySync_(sql, 'MHCT', 'CrownCounts');
}

function bq_getLatestRows_(dataset, table)
{
  // Use BQ to do the per-member UID-LastTouched limiting via an INNER JOIN:
  const tableId = bqKey + '.' + dataset + '.' + table;
  const sql = 'SELECT * FROM `' + tableId + '` JOIN (SELECT UID, MAX(LastTouched) AS `LastTouched` FROM `' + tableId + '` GROUP BY UID ) USING (LastTouched, UID)'

  const latestRows = bq_querySync_(sql, dataset, table);
  if (!latestRows || !latestRows.rows.length)
  {
    console.error({ 'message': 'Unable to retrieve latest records for all users' });
    return { rows: [], columns: [] };
  }

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
  const tableId = bqKey + '.' + dataset + '.' + table;
  const sql = 'SELECT UID, MAX(LastSeen), MAX(LastCrown), MAX(LastTouched) FROM `' + tableId + '` GROUP BY UID';
  return bq_querySync_(sql, dataset, table).rows;
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
 * @param {string} dataset
 * @param {string} table
 * @returns {string[]}
 */
function bq_getTableColumns_(dataset, table)
{
  const metadata = Bigquery.Tables.get(bqKey, dataset, table);
  return metadata.schema.fields.map(function (colSchema) {
    return colSchema.name;
  });
}

function bq_getTableRowCount_(dataset, table)
{
  const metadata = Bigquery.Tables.get(bqKey, dataset, table);
  return parseInt(metadata.numRows, 10);
}
// #endregion


// #region upload
/**
 * Populates the BigQuery table associated with MHCC Members with the current rows of the respective FusionTable
 */
function bq_populateUserTable()
{
  const config = {
    projectId: bqKey,
    datasetId: 'Core',
    tableId: 'Members'
  };

  const insertJob = _insertTableData_(getUserBatch_(0, 100000), config);
  return insertJob;
}

function bq_addCrownSnapshots_(newCrownData)
{
  const config = {
    projectId: bqKey,
    datasetId: 'Core',
    tableId: 'Crowns',
  };

  const insertJob = _insertTableData_(newCrownData, config);
  return insertJob;
}

function bq_addRankSnapshots_(newRankData)
{
  const config = {
    projectId: bqKey,
    datasetId: 'Core',
    tableId: 'Ranks',
  };

  const insertJob = _insertTableData_(newRankData, config);
  return insertJob;
}

/**
 * Upload the given data into the given BigQuery table (table must exist).
 * Returns the associated LoadJob
 * @param {(string|number)[][]} data FusionTable data as a 2D javascript array
 * @param {{projectId: string, datasetId: string, tableId: string}} config Description of the target table
 */
function _insertTableData_(data, config)
{
  const dataAsCSV = array2CSV_(data);
  const dataAsBlob = Utilities.newBlob(dataAsCSV, "application/octet-stream");

  // ripped from https://developers.google.com/apps-script/advanced/bigquery
  var job = Bigquery.newJob()
  job = {
    configuration: {
      load: {
        destinationTable: config,
        // Never create this table via the job.
        createDisposition: 'CREATE_NEVER',
        // Overwrite this table's contents. (default = WRITE_APPEND)
        // writeDisposition: "WRITE_TRUNCATE"
      },
    }
  };
  job = Bigquery.Jobs.insert(job, config.projectId, dataAsBlob);
  console.log({message: "Created job " + job.id, jobData: job });
  return job;
}
// #endregion

// #region utility
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
// #endregion
