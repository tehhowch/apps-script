/**
 * Interface for communicating with Google BigQuery resources associated with the MHCC project
 */

// #region query
/**
 *
 * @param {string} sql The query to execute on a table in this project
 * @param {string} dataset the dataset name containing the table to query
 * @param {string} table the table to query against
 * @returns {GoogleAppsScript.Bigquery.Schema.TableRow[]} Query results, as Table Rows
 */
function bq_querySync_(sql, dataset, table)
{
  var job = Bigquery.newJob();
  var configuration = Bigquery.newJobConfigurationQuery()
  configuration.query = sql;
  job.configuration = configuration;
  var queryJob = Bigquery.Jobs.insert(job, bqKey);
  var results = [];
  var queryResult = Bigquery.Jobs.getQueryResults(bqKey, queryJob);
  while (!queryResult.jobComplete)
  {
    queryResult = Bigquery.Jobs.getQueryResults(bqKey, queryJob);
  }
  var pages = 1;
  console.log({
    message: 'Query Completed',
    sql: sql,
    cacheHit: queryResult.cacheHit,
    resultSize: queryResult.totalRows,
    querySize: queryResult.totalBytesProcessed,
    hasPages: !!queryResult.pageToken,
    firstResult: queryResult.rows ? queryResult.rows[0] : null
  });
  Array.prototype.push.apply(results, queryResult.rows);
  while (queryResult.pageToken)
  {
    queryResult = Bigquery.Jobs.getQueryResults(bqKey, queryJob, { pageToken: queryResult.pageToken });
    Array.prototype.push.apply(results, queryResult.rows);
    ++pages;
  }
  if (pages !== 1) console.log({ message: 'Queried results from ' + pages + ' pages'});
  return results;
}

function bq_getMemberBatch_(start, limit)
{
  const sql = 'SELECT Member, UID FROM `' + bqKey + '.Core.Members` ORDER BY Member ASC';
  const members = bq_querySync_(sql, 'Core', 'Members');
  if (start === undefined && limit === undefined)
    return members;

  start = abs(parseInt(start || 0, 10));
  if (limit <= 0) limit = 1;
  limit = parseInt(limit || 100000, 10);
  return members.slice(start, start + limit);
}

/**
 * @param {string} dataset
 * @param {string} table
 */
function bq_getLatestTouchTimes_(dataset, table) {
  var query = 'SELECT UID, MAX(LastTouched) FROM `' + bqKey + '.' + dataset + '.' + table + '` GROUP BY UID';
  return bq_querySync_(query, dataset, table);
}

function bq_getLatestRows_(dataset, table) {
  // Use BQ to do the per-member UID-LastTouched limiting via an INNER JOIN:
  const tableId = bqkey + '.' + dataset + '.' + table;
  const sql = 'SELECT * FROM `' + tableId + '` JOIN (SELECT UID, MAX(LastTouched) AS `LastTouched` FROM `' + tableId + '` GROUP BY UID ) USING (LastTouched, UID)'

  const latestRows = bq_querySync_(sql, dataset, table);
  if (!latestRows || !latestRows.length)
  {
    console.error({ 'message': 'Unable to retrieve latest records for all users' });
    return [];
  }

  return latestRows.map(function (row) {
    return row.f.map(function (col) {
      return col.v;
    });
  });
}
// #endregion


// #region upload
/**
 * Populates the BigQuery table associated with MHCC Members with the current rows of the respective FusionTable
 */
function populateUserTable() {
  var config = {
    projectId: bqKey,
    datasetId: 'Core',
    tableId: 'Members'
  };

  var insertJob = insertTableData_(getUserBatch_(0, 100000), config);
  return insertJob;
}

function addCrownSnapshots_(newCrownData) {
  var config = {
    projectId: bqKey,
    datasetId: 'Core',
    tableId: 'Crowns',
  };

  var insertJob = insertTableData_(newCrownData, config);
  return insertJob;
}

function addRankSnapshots_(newRankData) {
  var config = {
    projectId: bqKey,
    datasetId: 'Core',
    tableId: 'Ranks',
  };

  var insertJob = insertTableData_(newRankData, config);
  return insertJob;
}

/**
 * Upload the given data into the given BigQuery table (table must exist).
 * Returns the associated LoadJob
 * @param {(string|number)[][]} data FusionTable data as a 2D javascript array
 * @param {{projectId: string, datasetId: string, tableId: string}} config Description of the target table
 */
function insertTableData_(data, config) {
  var dataAsCSV = array2CSV_(data);
  var dataAsBlob = Utilities.newBlob(dataAsCSV, "application/octet-stream");

  // ripped from https://developers.google.com/apps-script/advanced/bigquery
  var job = Bigquery.newJob()
  job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: config.projectId,
          datasetId: config.datasetId,
          tableId: config.tableId
        },
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
