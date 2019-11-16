/**
 * Interface for communicating with Google BigQuery resources associated with the MHCC project
 */

(function () {
  function getUserBatch_() {
  }
  function getLatestRows_() { }
  function bqBatchWrite_() { }
  function getNewLastRanValue_() { }
  function getByteCount_() { }
  function array2CSV_() { }
  function getDbSize() { }
  function getDbSizeData_() { }
  function doBackupTable_() { }
  function getTotalRowCount_() { }
})(this);


function getProjectData(projectId) {
  var data = Bigquery.Datasets.list(projectId || bqKey);
  return data.datasets;
}

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

function populateUserTable() {
  var config = {
    projectId: bqKey,
    datasetId: "Members",
    tableId: "Members"
  };
  console.log({message: "initial table", table: Bigquery.Tables.get(config.projectId, config.datasetId, config.tableId) });
  var insertJob = insertTableData(undefined, config);
  console.log({
    message: "post-insert-job-create",
    table: Bigquery.Tables.get(config.projectId, config.datasetId, config.tableId),
    jobs: Bigquery.Jobs.list(config.projectId).jobs,
    insertJob: insertJob
  });
  return insertJob;
}

function insertTableData(data, config) {
  var dataAsCSV = array2CSV_(data || getUserBatch_(0, 100000));
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
        createDisposition: "CREATE_NEVER",
        // Always overwrite this table's contents.
        writeDisposition: "WRITE_TRUNCATE"
      },
    }
  };
  job = Bigquery.Jobs.insert(job, config.projectId, dataAsBlob);
  console.log({message: "Created job " + job.id, jobData: job });
  return job;
}
