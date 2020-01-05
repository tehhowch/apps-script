//@ts-check
/**
 * Interface for reading MHCC data from BigQuery, such as users, crowns, and ranks
 */

/**
 * TODO
 * - support current fusiontable method's interface
 * - batch-submit queries (to improve responsiveness)
 * - split queries apart ("where UID = <val>" instead of "where UID IN (<vals)") to improve query caching
 * - validate UIDs against member list prior to submission
 * - allow displaying Elite ranking
 * - expose finding members within 5 ranks of the current UID's rank (to compare crowns & rank histories)
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

/**
 * Server-side function which queries the various tables to acquire the input users' crown and rank history.
 *
 * @param  {string} uids The (comma-separated) UID string with specific members for whom the data snapshots are returned.
 * @param  {boolean} [blGroup] Optional parameter controlling GROUP BY or "return all" behavior. Generally true.
 * @return {UserData[]}  An array of user data objects, each containing at minimum "user", "crown", and "rank" properties.
 */
function getUserHistory_(uids, blGroup)
{
  if (!uids) throw new Error('No UID provided');
  const queryData = _getBQData_(uids, blGroup);

  // Organize the query data by its associated member.
  const members = uids.split(",");
  const output = members.reduce(function (arr, id) {
    /** @type {UserData} */
    const memberOutput = {
      uid: id,
      crown: null,
      rank: null,
      user: '',
    };
    Object.keys(queryData).forEach(function (dataType) {
      const plotData = queryData[dataType]
      const memberData = plotData.dataset.filter(function (row) { return row[0] === id; });
      // Do not include the member's UID in the column data sent to the webapp.
      memberOutput[dataType] = {
        headers: plotData.headers.slice(1).map(function (colName) { return colName.replace(/_/g, ' '); }),
        dataset: memberData.map(function (row) { return row.slice(1); }),
      };
    });
    memberOutput.user = memberOutput.crown.dataset.slice(-1)[0][0];

    arr.push(memberOutput);
    return arr;
  }, []);
  return output;
}

// #endregion


// #region query
/**
 * Get the requested data from BigQuery.
 * @param {string} uids comma-separated UID string with specific members for whom the data snapshots are returned.
 * @param {boolean} [blGroup] Optional parameter controlling GROUP BY or 'return all' behavior. Generally true.
 */
function _getBQData_(uids, blGroup) {
  const queryConfig = [
    {
      label: 'crown', table: '`' + [dataProject, 'Core', 'Crowns'].join('.') + '`',
      columns: 'UID, Member, LastSeen, Bronze, Silver, Gold, MHCC',
      orderBy: 'UID ASC, LastSeen ASC',
    },
    {
      label: 'rank', table: '`' + [dataProject, 'Core', 'Ranks'].join('.') + '`',
      columns: 'UID, Member, LastSeen, MHCC_Crowns, Rank, RankTime',
      orderBy: 'UID ASC, RankTime ASC'
    },
  ];
  // Explicitly wrap UIDs in quotes for string parsing by BQ.
  const where = 'WHERE UID IN ("' + uids.replace(/"|'/g, '').split(',').join('","') + '")';

  const queries = queryConfig.map(function (config) {
    const terms = ['SELECT', config.columns, 'FROM', config.table, where];
    if (blGroup) terms.push('GROUP BY', config.columns);
    terms.push(config.orderBy);
    return { label: config.label, sql: terms.join(' ') };
  });

  /** @type {Object <string, PlotData> & { crown: PlotData, rank: PlotData }} */
  const queryData = { crown: null, rank: null };
  queries.forEach(function (config) {
    const resp = bq_querySync_(config.sql);
    queryData[config.label] = { "headers": resp.columns, "dataset": resp.rows };
  });
  return queryData
}

/**
 * Workhorse function for performing a fully-formed StandardSQL query
 * @param {string} sql The query to execute on a table in this project
 * @returns {{ rows: Array<(string|number)[]>, columns: string[] }} Query results (rows) and labels (cols)
 */
function bq_querySync_(sql)
{
  const queryJob = _bq_submitQuery_(sql);
  while (!_bq_hasQueryResults_(queryJob))
  {
    // Block until query is complete.
  }
  const queryResult = _bq_getQueryResults_(queryJob);

  console.log({
    message: 'Query Completed',
    received: queryJob,
    cacheHit: queryResult.cacheHit,
    resultCount: queryResult.totalRows,
    queryBytes: queryResult.totalBytesProcessed,
    firstResult: queryResult.rows ? queryResult.rows[0] : []
  });

  // Return the headers and formatted results to the caller.
  return {
    columns: queryResult.schema.fields.map(function (schemaField) { return schemaField.name; }),
    rows: bq_formatQueryRows_(queryResult.rows, queryResult.schema.fields),
  };
}

/**
 * @param {string} sql A #standardSQL query to submit for the configured project.
 */
function _bq_submitQuery_(sql)
{
  const job = Bigquery.newJob();
  job.configuration = {
    query: {
      query: sql,
      useLegacySql: false,
    }
  }
  return Bigquery.Jobs.insert(job, projectKey);
}

/**
 * Determines if job.getQueryResults is a valid API call (i.e. the job has started).
 * @param {GoogleAppsScript.Bigquery.Schema.Job} job
 */
function _bq_hasQueryResults_(job) {
  const jobStatus = Bigquery.Jobs.get(projectKey, job.jobReference.jobId, { fields: 'status/state' });
  const state = jobStatus.status.state.toLowerCase();
  return state === 'done' || state === 'success' || state === 'failure';
}

/**
 * Get query results for all pages from the given completed job.
 * @param {GoogleAppsScript.Bigquery.Schema.Job} job
 */
function _bq_getQueryResults_(job)
{
  const jobId = job.jobReference.jobId;
  var queryResult = Bigquery.Jobs.getQueryResults(projectKey, jobId);
  while (!queryResult.jobComplete)
  {
    console.error({ message: 'Should have been a completed job', queryResult: queryResult });
    queryResult = Bigquery.Jobs.getQueryResults(projectKey, jobId);
  }

  // Collect rows from any remaining pages into the first page's results.
  const firstResult = queryResult;
  while (queryResult.pageToken)
  {
    queryResult = Bigquery.Jobs.getQueryResults(projectKey, jobId, {
      pageToken: queryResult.pageToken,
      fields: 'rows,errors,pageToken',
    });
    Array.prototype.push.apply(firstResult.rows, queryResult.rows);
  }
  if (parseInt(firstResult.totalRows, 10) !== firstResult.rows.length) {
    console.error({ message: 'row mismatch', firstResult: firstResult, queryResult: queryResult });
  }
  return firstResult;
}

/**
 * Returns a copy of the BQ query TableRows, formatted according to the types present in the given schema field description.
 * @param {GoogleAppsScript.Bigquery.Schema.TableRow[]} rows BQ QueryResult rows to format
 * @param {GoogleAppsScript.Bigquery.Schema.TableFieldSchema[]} schemaFields Schema fields describing the result row format
 * @returns {(string | number)[][]}
 */
function bq_formatQueryRows_(rows, schemaFields)
{
  // Compute type-coercion functions from the column schema.
  const formatters = schemaFields.map(function (colSchema) {
    var type = colSchema.type.toLowerCase();
    if (type === 'float' || type === 'float64') {
      return { fn: function (value) { return parseFloat(value); } };
    } else if (type === 'numeric' || type === 'integer' || type === 'int64' || type === 'integer64') {
      return { fn: function (value) { return parseInt(value, 10); } };
    } else {
      return { fn: function (value) { return value; } };
    }
  });

  return rows.map(function (row) { return row.f.map(
    function (col, idx) {
      return formatters[idx].fn(col.v);
    });
  });
}

/**
 * Perform multiple queries in parallel, blocking until all have completed.
 * (These queries should be read-only to avoid races.)
 * @param {string[]} queries A set of queries to perform
 * @param {string[]} queryIds Labels for the queries to help distinguish results.
 */
function bq_querySetSync_(queries, queryIds)
{
  // Submit all jobs before checking any for completion.
  const queryJobs = queries.map(_bq_submitQuery_);
  while (!queryJobs.every(_bq_hasQueryResults_))
  {
    // Block until all queries have completed.
  }
  const queryResults = queryJobs.map(_bq_getQueryResults_);
  console.log({
    message: queryResults.length + ' queries completed',
    queryData: queryResults.map(function (qr) { return {
      cacheHit: qr.cacheHit,
      resultCount: qr.totalRows,
      queryBytes: qr.totalBytesProcessed,
      firstResult: qr.rows ? qr.rows[0] : [],
    }; }),
  });

  return queryResults.map(function (qr, idx) {
    return {
      queryId: (queryIds[idx] !== undefined) ? queryIds[idx] : queries[idx],
      columns: qr.schema.fields.map(function (sf) { return sf.name; }),
      rows: bq_formatQueryRows_(qr.rows, qr.schema.fields),
    };
  });
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
