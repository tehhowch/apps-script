/**
* Google FusionTables to serve as the backend
* Structural changes: since updating rows is a 1-by-1 operation (yes, really >_> ), we
* will have multiple entries for each person. To prevent excessive table growth, we will store only
* 30 snapshots per user, removing any duplicates which occur when the script updates more often than
* the person's data is refreshed.
* Bonus: this means we can serve new data such as most crowns in the last 30 days.
*/

/**
 * Returns a 2D array of the members' names and identifiers, from `start` until `limit`, or EOF.
 * @param  {number} start First retrieved index (relative to an alphabetical sort on members).
 * @param  {number} limit The maximum number of pairs to return.
 * @return {Array <string>[]} [Name, UID]
 */
function getUserBatch_(start, limit)
{
  var sql = "SELECT Member, UID FROM " + utbl + " ORDER BY Member ASC OFFSET " + start + " LIMIT " + limit;
  return FusionTables.Query.sqlGet(sql).rows;
}

/**
 * function doBackupTable_      Ensure that a copy of the database exists prior to performing some
 *                              major update, such as attempting to remove all non-unique rows.
 *                              Returns the table id of the copy, if one was made.
 * @param {string} [tableId]    The FusionTable to operate on (default: MHCC Crowns).
 * @param {boolean} [deleteEarliest=true]  If true, the previous backup will be deleted after a successful backup.
 * @return {string} The FusionTable ID for the newly-created FusionTable.
 */
function doBackupTable_(tableId, deleteEarliest)
{
  /**
   * Access or initialize a backup object for the given user. The backup objects store the tableIds of copies for
   * a given input table, keyed to the time at which they were created.
   *
   * @param {any} store  A PropertiesService object (UserProperties or ScriptProperties)
   * @param {string} key The key which is used to access the object in the store.
   * @return {Object <string, Object <string, string>>} An object with keys of FusionTable IDs, yielding time-id associative objects.
   */
  function _getBackupObject_(store, key)
  {
    var value = store.getProperty(key);
    // If the value was there, it may or may not be stringified JSON.
    if (value && value[0] === "{" && value[1] === "\"")
    {
      var existing = JSON.parse(value);
      // Ensure the backup object has the requested table as a property.
      if (!existing[tableId])
        existing[tableId] = {};
      return existing;
    }
    var newObject = {};
    newObject[tableId] = {};
    return newObject;
  }

  if (!tableId) tableId = ftid;
  deleteEarliest = deleteEarliest !== false;

  const uStore = PropertiesService.getUserProperties(),
    store = PropertiesService.getScriptProperties(),
    userKey = "MHCC_MostCrownBackupIDs",
    scriptKey = "backupTableIDs";

  // Get the user and script backup objects, which will have at least the input tableId as a property.
  var userBackup = _getBackupObject_(uStore, userKey);
  var scriptBackup = _getBackupObject_(store, scriptKey);

  const copyOptions = { "copyPresentation": true, "fields": "tableId,name,description" };
  // We store the time a backup was made (ms epoch) as the key, and the tableId as the value.
  const now = new Date();
  const newSuffix = "_AsOf_" + [now.getUTCFullYear(), 1 + now.getUTCMonth(), now.getUTCDate(),
  now.getUTCHours(), now.getUTCMinutes()].join("-");

  // Get the minimal resource of the copied table.
  try { var backup = FusionTables.Table.copy(tableId, copyOptions); }
  catch (e) { console.error(e); return; }

  // Rename it and set the new description.
  backup.name = backup.name.slice(backup.name.indexOf("Copy of ") + "Copy of ".length).split(" ").join("_") + newSuffix;
  backup.description = "Automatic backup of table with id= '" + tableId + "'.";
  try { backup = FusionTables.Table.patch(backup, backup.tableId); }
  catch (e) { console.warn(e); }

  // Remove the oldest backup, if desired (and possible).
  try
  {
    if (deleteEarliest && Object.keys(userBackup[tableId]).length > 1)
    {
      var earliest = Object.keys(userBackup[tableId]).reduce(function (dt, next) { return Math.min(dt * 1, next * 1); });
      const key = String(earliest);
      const idToDelete = userBackup[tableId][key];
      if (idToDelete !== tableId)
      {
        FusionTables.Table.remove(idToDelete);
        delete userBackup[tableId][key];
        if (scriptBackup[tableId][key])
          delete scriptBackup[tableId][key];
      }
    }
  }
  catch (e) { console.warn({ "message": e.message, "error": e, "tableId": tableId, "user": userBackup, "script": scriptBackup, "earliest": earliest, "idToDelete": idToDelete }); }

  // Store the data about the backup.
  const newBackupKey = String(now.getTime());
  userBackup[tableId][newBackupKey] = backup.tableId;
  scriptBackup[tableId][newBackupKey] = backup.tableId;
  uStore.setProperty(userKey, JSON.stringify(userBackup));
  store.setProperty(scriptKey, JSON.stringify(scriptBackup));
  return backup.tableId;
}
