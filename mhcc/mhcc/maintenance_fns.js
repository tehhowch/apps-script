/**
 * function doBookending    This maintenance method will drastically reduce the database size if a
 *                          significant portion of the members are infrequently seen, while a much
 *                          smaller group is considerably more active (as is expected). Only records
 *                          which demarcate crown changes are kept (as opposed to just being seen).
 *                          For the same crown change date, two records are kept per rank seen, as
 *                          the rank can only worsen until the player gains a new crown change date.
 */
function doBookending_()
{
  var members = getUserBatch_(0, 100000), startTime = new Date().getTime();
  var rowids = identifyBookendRowids_(
    members.map(
      function (value, index) { return value[1] }
    )
  );
  var totalRowCount = getTotalRowCount_(ftid);
  Logger.log('Current non-bookend count: ' + (totalRowCount - rowids.length) + ' out of ' + totalRowCount + ' rows');
  if (rowids.length === totalRowCount)
    Logger.log('All records are bookends');
  else if (rowids.length > totalRowCount)
    throw new Error('More bookending records than records... How???');
  else if (rowids.length == 0)
    throw new Error('No rowids returned for bookending.');
  else if (doBackupTable_() === false)
    throw new Error("Couldn't back up existing table data");
  else
  {
    var records = retrieveWholeRecords_(rowids, ftid);
    if (records.length > 0)
      doReplace_(ftid, records);
    else
      throw new Error('No records returned from given bookend rowids');
  }
  Logger.log('doBookending: ' + ((new Date().getTime() - startTime) / 1000) + ' sec');
}
/**
 * function identifyBookendRowids_  For the given members, returns the ROWID array containing each
 *                                  members' records that describe
 *                                       1) every instance of a rank change
 *                                       2) every instance of a crown change
 *                                  If a user is newly seen, but doesn't get any new crowns, the
 *                                  record is ignored (keeping the earlier record having that same
 *                                  crown change date). 
 * @param {String[]} memUIDs        The members whose data will be kept.
 * @return {Array}                  The ROWIDs of their "bookending" records
 */
function identifyBookendRowids_(memUIDs)
{
  if (memUIDs.constructor !== Array)
    throw new TypeError('Expected input to be type Array');
  else if (memUIDs.length == 0 || typeof memUIDs[0] !== 'string')
    throw new TypeError('Expected input array to contain strings');
  
  var startTime = new Date().getTime(), resultArr = [];
  while (memUIDs.length > 0)
  {
    var baseSQL = 'SELECT UID, LastCrown, ROWID, LastSeen, Rank FROM ' + ftid + ' WHERE UID IN (';
    var tailSQL = '', sqlUIDs = [], batchTime = new Date().getTime(), lengthLimit = 8050 - baseSQL.length;
    while ((tailSQL.length <= lengthLimit) && (memUIDs.length > 0))
    {
      sqlUIDs.push(memUIDs.pop());
      tailSQL = sqlUIDs.join(",") + ") ORDER BY LastCrown ASC";
    }
    var resp = FusionTables.Query.sqlGet(baseSQL + tailSQL);
    if (typeof resp.rows == 'undefined')
      throw new Error('Unable to reach FusionTables');
    else
    {
      resultArr = [].concat(resultArr, resp.rows);
      var elapsedMillis = new Date().getTime() - batchTime;
      if (elapsedMillis < 500)
        Utilities.sleep(502 - elapsedMillis);
    }
  }
  // Categorize the database records.
  var lcMap = {};
  var row = 0, data, cur_uid, cur_lastCrown, cur_row_id, cur_lastSeen, cur_Rank;
  for (; row < resultArr.length; ++row)
  {
    data = resultArr[row];
    cur_uid = data[0];
    cur_lastCrown = data[1];
    cur_row_id = data[2];
    cur_lastSeen = data[3];
    cur_Rank = data[4];
    // Have we seen this member yet?
    if (typeof lcMap[cur_uid] === 'undefined')
      lcMap[cur_uid] = { 'lc': {} };
    // Have we seen this member's crown change date before?
    if (typeof lcMap[cur_uid].lc[cur_lastCrown] === 'undefined')
    {
      // Create a new object describing when it was first and last seen, and associated ranks.
      lcMap[cur_uid].lc[cur_lastCrown] = {
        "minLS": { "val": cur_lastSeen, "rowid": cur_row_id },
        "maxLS": { "val": cur_lastSeen, "rowid": cur_row_id },
        'minRank': { 'val': cur_Rank, 'rowid': cur_row_id },
        'maxRank': { 'val': cur_Rank, 'rowid': cur_row_id }
      };
    }
    else
    {
      // Yes, compare vs existing data
      if (cur_lastSeen < lcMap[cur_uid].lc[cur_lastCrown].minLS.val)
      {
        // This row has the same crown change date, but occurred before the stored min value. Replace the stored LastSeen
        lcMap[cur_uid].lc[cur_lastCrown].minLS = { "val": cur_lastSeen, "rowid": cur_row_id };
      }
      else if (cur_lastSeen > lcMap[cur_uid].lc[cur_lastCrown].maxLS.val)
      {
        // This row has the same crown change date, but occurred after the stored max value. Replace the stored LastSeen
        lcMap[cur_uid].lc[cur_lastCrown].maxLS = { "val": cur_lastSeen, "rowid": cur_row_id };
      }

      if (cur_Rank < lcMap[cur_uid].lc[cur_lastCrown].minRank.val)
      {
        // Same crown change date, but lower rank than the stored min rank. Updated stored rank.
        lcMap[cur_uid].lc[cur_lastCrown].minRank = { 'val': cur_Rank, 'rowid': cur_row_id };
      }
      else if (cur_Rank > lcMap[cur_uid].lc[cur_lastCrown].maxRank.val)
      {
        // Same crown change date, but higher rank than the stored max rank. Updated stored rank.
        lcMap[cur_uid].lc[cur_lastCrown].maxRank = { 'val': cur_Rank, 'rowid': cur_row_id };
      }
    }
  }
  // Push the needed rowids
  var keptRowids = [];
  for (var mem in lcMap)
    for (var changeDate in lcMap[mem].lc)
    {
      var lc = lcMap[mem].lc[changeDate];
      // Rowids are unique to each user, but each user's entry may have the same minimum and maximum
      // LastSeen or Rank for each LastCrown. Only the lc object needs to be checked for duplicates.
      // Restricting this check prevents nasty indexOf() use on a very large (>100,000) array.
      var toAdd = [lc.minLS.rowid];
      if (toAdd.indexOf(lc.maxLS.rowid) === -1)
        toAdd.push(lc.maxLS.rowid);
      if (toAdd.indexOf(lc.minRank.rowid) === -1)
        toAdd.push(lc.minRank.rowid);
      if (toAdd.indexOf(lc.maxRank.rowid) === -1)
        toAdd.push(lc.maxRank.rowid);

      // Add the user's unique rows for this LastCrown to the rowid list for fetching.
      while (toAdd.length != 0)
        keptRowids.push(toAdd.pop());
    }
  Logger.log((new Date().getTime() - startTime) / 1000 + ' sec to find bookend ROWIDs');
  return keptRowids.sort();   
}
/** Function that takes the 12-column Crowns table and exports rank data, operating on sets of users at a time.
 * 
 */
function populateRankFusionTable_()
{
  /**
   * Obtain the UIDs of those members that have data in the Rank DB FusionTable.
   * @return {String[]}
   */
  function _getRankTableUserIDs_()
  {
    var sql = "SELECT UID FROM " + rankTableId + " GROUP BY UID ORDER BY UID ASC";
    var resp = FusionTables.Query.sqlGet(sql, { quotaUser: "maintenance" });
    if (!resp || !resp.rows || !resp.columns)
      return [];
    return (resp.rows.map(function (row) { return String(row[0]); }));
  }
  /**
   * Query the Crown FusionTable to determine how many rows each member has, and return a UID-indexed object.
   * 
   * @return {{uid:Number}}
   */
  function _getCrownTableRowCounts_()
  {
    var sql = "SELECT UID, Count(LastTouched) FROM " + ftid + " GROUP BY UID";
    var resp = FusionTables.Query.sqlGet(sql, { quotaUser: "maintenance" });
    var output = {};
    resp.rows.forEach(function (row) { output[String(row[0])] = row[1] * 1; });
    return output;
  }
  /**
   * Return an array of the next set of UIDs to query for migratable records. Makes sure that both the query string length
   * and number of rows queried will be below the supplied arguments.
   * Mutates the input "remaining" array.
   * 
   * @param {String[][]} remaining MUTABLE 2D array of [Name, UID] of members that have yet to be migrated.
   * @param {{uid:Number}} rowKey  Lookup object which stores the number of rows a given UID adds to the overall query.
   * @param {Number} maxRows       The maximum number of rows that should return from a query (i.e. to not generate a "Response > 10 MB" 503 error).
   * @param {Number} maxUIDStrLength The maximum joined length of the queried UIDs (total query string length must be < ~8050 characters).
   * @return {String[]}
   */
  function _getNextUserSet_(remaining, rowKey, maxRows, maxUIDStrLength)
  {
    if (!remaining || !remaining.length)
      return [];
    if (!maxUIDStrLength) maxUIDStrLength = 7900;
    const unknown = remaining.filter(function (memUID) { return (!rowKey[memUID[1]] && rowKey[memUID[1]] !== 0); });
    if (unknown.length)
    {
      console.warn({ "no-row members": unknown });
      throw new Error("Some members not present in the row key");
    }

    // Require the "remaining" set is sorted descending by number of rows.
    remaining.sort(function (a, b) { return rowKey[b[1]] - rowKey[a[1]]; });

    const nextSet = [];
    var nextRows = 0, queryLength = 0;
    // Fill from the front (the largest number of rows) first.
    for (var m = 0; m < remaining.length; /* mutating */)
    {
      var memberRows = rowKey[remaining[m][1]];
      if (memberRows && (nextRows + memberRows < maxRows) && (queryLength + remaining[m][1].length < maxUIDStrLength))
      {
        var uid = String(remaining.splice(m, 1)[0][1]);
        nextSet.push(uid);
        nextRows += memberRows;
        queryLength = nextSet.join(",").length;
      }
      else if (memberRows === 0)
        remaining.splice(m, 1);
      else
        ++m;
      // Stop searching if we can't add another member.
      if (maxUIDStrLength - queryLength < 16)
        break;
    }
    return nextSet;
  }
  /**
   * Mutate the input records by determining the appropriate LastSeen and MHCC Crown values for each
   * member's unique RankTime values. If a RankTime is duplicated, only the first instance is kept.
   * 
   * @param {Array[]} records    2D array destined to be uploaded to the Rank DB FusionTable.
   * @param {{uid: {rankTimes: {String: Number}}}} dataMap Object which stores yet-unprocessed RankTimes, and the source row.
   * @param {Array[]} reference  2D array downloaded from the Crown DB FusionTable, which birthed the records array.
   * @return {void}
   */
  function _fillRankRecords_(records, dataMap, reference)
  {
    for (var r = 0; r < records.length; /* mutating */)
    {
      var record = records[r];
      var rt = record[3], UID = String(record[1]);
      // The RankTime will exist, unless we have used it already.
      if (dataMap[UID].rankTimes[rt])
      {
        var desiredIndex = dataMap[UID].rankTimes[rt].row - 1;
        if (desiredIndex >= 0 && String(reference[desiredIndex][1]) === UID)
        {
          var basisRow = reference[desiredIndex];
          // We can use this row's data.
          record[2] = basisRow[2];
          record[5] = basisRow[4];
          // Log the time shift (difference in LastTouched values) we've induced.
          timeShifts.push(reference[desiredIndex + 1][3] - basisRow[3]);
        }
        else { /* Keep blank values for LastSeen and MHCC Crowns */ }

        // We have used this RankTime value.
        delete dataMap[UID].rankTimes[rt];
        ++r;
      }
      // This is a duplicated value of RankTime for this member.
      else
        duplicates.push({ "uid": UID, "value": records.splice(r, 1) });
    }
  }

  const start = new Date();

  // Don't let Apps Script run this function concurrently.
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30000))
    return;

  // Query both tables, to determine who is yet to be operated on.
  const members = getUserBatch_(0, 10000),
        done = _getRankTableUserIDs_(),
        sizeData = getDbSizeData_();

  // Filter the member list by those who have yet to be processed.
  const toDo = members.filter(function (row) { return done.indexOf(String(row[1])) === -1; }),
        rowKey = _getCrownTableRowCounts_(),
        logs = [];

  // We only need the Member Name, UID, LastSeen, LastTouched, MHCC Crowns, Rank, and RankTime.
  const SELECT = ("SELECT Member, UID, LastSeen, LastTouched, MHCC, Rank, RankTime FROM " + ftid),
        ORDER = ") ORDER BY UID ASC, RankTime ASC, LastTouched ASC";

  do {
    var uids = _getNextUserSet_(toDo, rowKey, 60000, 8000 - SELECT.length - ORDER.length - 15);
    if (!uids || !uids.length) break;

    console.time("Batch execution: " + uids.length + " members.");
    var sql = SELECT + " WHERE UID IN (" + uids.join(",") + ORDER;
    logs.push(sql);
    // Download every record for these members from the MHCC Crowns DB, sorted ASC by RankTime & then LastTouched.
    // The LastTouched value should be greater than the RankTime value for every record.
    var crownRecords = FusionTables.Query.sqlGet(sql, { quotaUser: "maintenance" }).rows;
    // Map between a given RankTime and the row it came from so that the previous row can be queried for
    // LastSeen & MHCC Crown data, and we can remove duplicated RankTime values.
    var rankMapping = {};
    crownRecords.forEach(function (record, index) {
      var UID = String(record[1]);
      if (!rankMapping[UID]) rankMapping[UID] = { "rankTimes": {} };

      // If this is a new RankTime, store it (& the row).
      if (!rankMapping[UID].rankTimes[record[6]]) rankMapping[UID].rankTimes[record[6]] = { "row": index };
    });

    // Create the new Rank DB records from the bulk crown records, leaving LastSeen and MHCC Crown fields empty.
    var rankRecords = crownRecords.map(function (record) {
      //      Member   | UID      | LS         | RankTime | Rank     | MHCC Crowns.
      return [record[0], record[1], /* blank */, record[6], record[5], /* blank */];
    });
    
    // Use the mapped information to fill in the rows as required.
    var timeShifts = [], duplicates = [];
    _fillRankRecords_(rankRecords, rankMapping, crownRecords);

    // Upload the new rows to the Rank DB FusionTable.
    if (rankRecords.length)
      ftBatchWrite_(rankRecords, rankTableId, false);
    
    console.timeEnd("Batch execution: " + uids.length + " members.");
    console.log({ "numDuplicates": duplicates.length, "rowsAdded": rankRecords.length, "rowsDownloaded": crownRecords.length });
    console.log({ "time shifts": timeShifts });
  } while (toDo.length && (new Date() - start < 250 * 1000));

  // Log a status report.
  console.info({ "message": logs.length + " queries completed", "logs": logs, "timeUsed": (new Date() - start) / 1000, "# Remaining": toDo.length });

  // Allow running this again.
  lock.releaseLock();
}

/**
 * Function which drops the given columns from the target table. Returns true only if all requested drops were complete.
 * 
 * @param {String} tableId
 * @param {{matchOn:String, columns:any[]}} columnsToDrop An object which indicates what property of the columns should be matched, and the corresponding identifier to match.
 *                                                     For example, matchOn could be 'name', and columns would contain the names of columns to drop.
 * @return {Boolean}
 */
function columnDropper_(tableId, columnsToDrop)
{
//  if (!tableId) tableId = ftid;
//  if (!columnsToDrop) columnsToDrop = { matchOn: "name", columns: ["Rank", "RankTime"] };
  
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30 * 1000) || !columnsToDrop || !columnsToDrop.columns.length)
    return;
  
  // Make a backup of this table, but do *not* delete the previous one.
  // If no backup is created (for example, FT storage quota is full), quit.
  const backupID = doBackupTable_(tableId, true);
  if (!backupID) return;

  // Ensure a valid ID was given by attempting to read it.
  const options = { quotaUser: "maintenance", "fields": "tableId,name,columns" };
  try { var target = FusionTables.Table.get(tableId, options); }
  catch (e) { console.error(e); return; }

  // Check that the columns exist prior to removing them.
  columnsToDrop.columns.forEach(function (col) {
    if (target.columns.every(function (tc) { return tc[columnsToDrop.matchOn] !== col; }))
      return;
    try { FusionTables.Column.remove(target.tableId, col); }
    catch (e) { console.error(e); }
  });
  lock.releaseLock();

  // Report on the operational success by reading the new resource schema.
  try { var alteredTable = FusionTables.Table.get(target.tableId, options); }
  catch (e) { console.warn(e); }
  const allCompleted = (target.columns.length - columnsToDrop.columns.length === alteredTable.columns.length);
  console.log({ "message": "Column drop " + (allCompleted ? " successful." : " unsuccessful."), "original": target, "altered": alteredTable });
  return allCompleted;
}