/**
 * Interface for communicating with Google BigQuery resources associated with the MHCC project
 */

// #region query
/**
 *
 * @param {string} sql The query to execute on a table in this project
 * @param {string} dataset the dataset name containing the table to query
 * @param {string} table the table to query against
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
    job: job,
    cacheHit: queryResult.cacheHit,
    resultSize: queryResult.totalRows,
    querySize: queryResult.totalBytesProcessed,
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
    rows: results.map(function (row) { return row.f.map(function (col) { return col.v; })}),
    columns: headers,
  };
}

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

function bq_getLatestBatch_(dataset, table, uids)
{
  const tableId = bqKey + '.' + dataset + '.' + table;
  const sql = 'SELECT UID, MAX(LastSeen), MAX(LastCrown), MAX(LastTouched) FROM `' + tableId + '`'
      + ' WHERE UID IN ("' + uids.join('","') + '") GROUP BY UID';
  return bq_querySync_(sql, dataset, table).rows;
}

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
// #endregion


// #region upload
/**
 * Populates the BigQuery table associated with MHCC Members with the current rows of the respective FusionTable
 */
function populateUserTable()
{
  const config = {
    projectId: bqKey,
    datasetId: 'Core',
    tableId: 'Members'
  };

  const insertJob = insertTableData_(getUserBatch_(0, 100000), config);
  return insertJob;
}

function addCrownSnapshots_(newCrownData)
{
  const config = {
    projectId: bqKey,
    datasetId: 'Core',
    tableId: 'Crowns',
  };

  const insertJob = insertTableData_(newCrownData, config);
  return insertJob;
}

function addRankSnapshots_(newRankData)
{
  const config = {
    projectId: bqKey,
    datasetId: 'Core',
    tableId: 'Ranks',
  };

  const insertJob = insertTableData_(newRankData, config);
  return insertJob;
}

/**
 * Upload the given data into the given BigQuery table (table must exist).
 * Returns the associated LoadJob
 * @param {(string|number)[][]} data FusionTable data as a 2D javascript array
 * @param {{projectId: string, datasetId: string, tableId: string}} config Description of the target table
 */
function insertTableData_(data, config)
{
  const dataAsCSV = array2CSV_(data);
  const dataAsBlob = Utilities.newBlob(dataAsCSV, "application/octet-stream");

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
