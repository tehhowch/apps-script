//@ts-check
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

/**
 * Convert the data array into a CSV blob and upload to FusionTables.
 * @param  {Array[]} newData  The 2D array of data that will be written to the database.
 * @param  {string}  tableId  The table to which the batch data should be written.
 * @param  {boolean} [strict=true] If the number of columns must match the table schema (default true).
 * @return {number} The number of rows that were added to the FusionTable database.
 */
function ftBatchWrite_(newData, tableId, strict)
{
  if (!tableId) return;
  const options = { isStrict: strict !== false };

  const dataAsCSV = array2CSV_(newData);
  var dataAsBlob;
  try { dataAsBlob = Utilities.newBlob(dataAsCSV, "application/octet-stream"); }
  catch (e)
  {
    e.message = "Unable to convert array into CSV format: " + e.message;
    console.error({ "message": e.message, "input": newData, "csv": dataAsCSV, "errstack": e.stack.split("\n") });
    throw e;
  }

  try { return parseInt(FusionTables.Table.importRows(tableId, dataAsBlob, options).numRowsReceived, 10); }
  catch (e)
  {
    e.message = "Unable to upload rows: " + e.message;
    throw e;
  }
}
