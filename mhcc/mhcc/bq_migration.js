// FusionTable migration script

/**
 *
 * @param {string} str text to be made BQ-compatible (alphanumeric + underscores)
 */
function asBqName_(str) {
  return str.toString()
    .replace(/\s/g, "_")
    .replace(/-/g, "")
    .replace(/\./g, "")
    .replace(/:/g, "");
}


function getAllFusionTables() {
    var tables = FusionTables.Table.list().items;
    return tables.map(function (table) {
      // Convert into an array of slightly-simplified data.
      return {
        name: table.name,
        tableId: table.tableId,
        description: table.description,
        columns: table.columns.map(function (col) {
          return {
            name: col.name,
            columnId: col.columnId,
            description: col.description,
            type: col.type !== "NUMBER" ? col.type : (col.formatPattern === "NUMBER_INTEGER" ? "INT64" : "FLOAT64"),
            MODE: "REQUIRED"
          };
        })
      };
    });
}

function copyTables() {
  var tableData = getAllFusionTables();
  var ds = Bigquery.Datasets.insert({
    datasetReference: {
      datasetId: "FT_Autoimport_" + new Date().getTime(),
      projectId: bqKey,
    },
    description: "Automatic imports of known FusionTables",
  }, bqKey);
  var dsKey = ds.datasetReference.datasetId;
  return tableData.map(function createTableSchema(ftTableData) {
    var schema = Bigquery.newTableSchema();
    // https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tableschema
    schema.fields = ftTableData.columns.map(function (col) {
      var tfschema = Bigquery.newTableFieldSchema();
      tfschema.name = asBqName_(col.name);
      tfschema.type = col.type;
      tfschema.description = col.description;
      tfschema.mode = col.MODE || "REQUIRED";
      return tfschema;
    });
    return schema;
  }).map(function createTable(ts, idx) {
    var ft = tableData[idx];
    var table = Bigquery.Tables.insert({
        schema: ts,
        tableReference: {
          tableId: asBqName_(ft.name),
          datasetId: dsKey,
          projectId: bqKey
        },
        description: ft.description,
        friendlyName: ft.name
    }, bqKey, dsKey);
    // TODO: Load data into the table? This could be very time-consuming via Apps Script.
    console.log({ message: "Created empty BigQuery table '" + table.id + "' from FusionTable '" + ft.name + "' (" + ft.tableId + ")" });
    return { ft: ft.tableId, bq: table.id };
  });
}
