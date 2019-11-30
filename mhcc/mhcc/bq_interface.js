/**
 * Interface for communicating with Google BigQuery resources associated with the MHCC project
 */

// #region read
/**
 * Return a list of all datasets associated with the given project.
 * @param {string} projectId BigQuery Project ID
 */
function getProjectData(projectId) {
  var data = Bigquery.Datasets.list(projectId || bqKey);
  return data.datasets;
}

/**
 * Return a list of all tables for all datasets in the given project.
 * @param {string} projectId BigQuery Project ID
 */
function getTables(projectId) {
  var datasets = getProjectData(projectId);
  return datasets.map(function (ds) {
    var dsRef = ds.datasetReference;
    var tables = Bigquery.Tables.list(dsRef.projectId, dsRef.datasetId);
    if (tables.tables && tables.tables.length) {
      console.log({message: "Project Tables: " + dsRef.datasetId, tables: tables.tables });
    }
    return tables.tables;
  });
}
// #endregion

// #region query
function bq_getLatestRows_() {
  var job = Bigquery.newJob();
  var configuration = Bigquery.newJobConfigurationQuery()
  // configuration
  // access the query results via getQueryResults
  // query caching means we can reliably perform calls to get the user batch and it will only
  // bill once a day unless new members were added.
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
