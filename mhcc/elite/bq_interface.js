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
 * @param {'Core'|'Elite'} [datasetName] The dataset containing the target Members table (default "Elite").
 * @param {number} [start] The offset into the member list to retreive
 * @param {number} [limit] The maximum number of members to retrieve
 */
function bq_getMemberBatch_(datasetName, start, limit)
{
  if (!datasetName) datasetName = 'Elite';
  const tableId = [dataProject, datasetName, 'Members'].join('.');
  const sql = 'SELECT Member, UID FROM `' + tableId + '` ORDER BY Member ASC';
  const members = bq_querySync_(sql).rows;
  if (start === undefined && limit === undefined)
    return members;

  start = Math.abs(parseInt(start || 0, 10));
  if (limit <= 0) limit = 1;
  limit = parseInt(limit || 100000, 10);
  return members.slice(start, start + limit);
}

/**
 * Return the MHCC Core Crown records for the current Elite members.
 */
function bq_getLatestMHCCRows_()
{
  const valids = bq_getMemberBatch_().reduce(function (hash, pair)
  {
    if (hash.hasOwnProperty(pair[1]))
      console.warn(pair[1] + ' is both "' + pair[0] + '" and "' + hash[pair[1]] + '"');
    else
      hash[pair[1]] = pair[0];
    return hash;
  }, {})

  const allData = bq_getLatestRows_('Core', 'Crowns');
  const snuidIndex = allData.columns.indexOf('UID');
  if (snuidIndex === -1) throw new Error('Missing "UID" column in Core.Crowns response');
  const eliteData = allData.rows.filter(function (row) { return valids[row[snuidIndex]]; });

  // Replace MHCC group names with the members' preferred Elite MHCC names
  const memberIndex = allData.columns.indexOf('Member');
  if (memberIndex === -1) throw new Error('Missing "Member" column in Core.Crowns response');
  eliteData.forEach(function (row) { row[memberIndex] = valids[row[snuidIndex]]; });

  // Log any current members who weren't included in the fetched data.
  if (eliteData.length !== Object.keys(valids).length) {
    // Prune `valids` to only those members without obtained data.
    eliteData.forEach(function (row) { delete valids[row[snuidIndex]]; });
    console.warn({ message: 'Some Elite members lack MHCC Crown DB records', data: valids });
  }
  return eliteData;
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

/**
 * @param {string} dataset Name of the dataset that contains the given table.
 * @param {string} [tableName] Table name containing a "RankTime" column (default "Ranks")
 */
function bq_getLatestRankTime_(dataset, tableName)
{
  tableName = tableName || 'Ranks';
  const tableId = [dataProject, dataset, tableName].join('.');
  const sql = 'SELECT MAX(RankTime) FROM `' + tableId + '`';
  const maxRankMillis = bq_querySync_(sql).rows[0][0];
  console.log({ maxRankMillis: maxRankMillis, type: typeof maxRankMillis, asDate: new Date(maxRankMillis) });
  return maxRankMillis;
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


// #region upload
/**
 * Upload the given Rank data to the Rank Records table.
 * @param {(string|number)[][]} newRankData array of rank data to upload
 */
function bq_addRankSnapshots_(newRankData)
{
  const config = {
    projectId: dataProject,
    datasetId: 'Elite',
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
  job = Bigquery.Jobs.insert(job, projectKey, dataAsBlob);
  console.log({
    message: "Created job " + job.id,
    jobData: job,
    inputRowCount: data.length,
    firstTenRows: data.slice(0, 10),
  });
  // TODO: add a check for job error, throw if errored.
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
