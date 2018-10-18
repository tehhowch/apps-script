// @OnlyCurrentDoc
function onOpen(e)
{
  e.source.addMenu("Admin", [
    { name: "Add New Competitor", functionName: "addCompetitor" },
    { name: "Update Scoreboard", functionName: "doScoreboardUpdate" }
  ]);
}

// Collect several names & UIDs to add to the sheet. Performs a duplicate check based on UIDs.
function addCompetitor()
{
  function _getNewMembers_(beingAdded)
  {
    var ui = SpreadsheetApp.getUi();
    var nameResponse = ui.prompt(
      "Adding new competitors...", "Enter the new competitor's name", ui.ButtonSet.OK_CANCEL
    );
    if (nameResponse.getSelectedButton() !== ui.Button.OK)
      return false;

    var uidResponse = ui.prompt(
      "Adding new competitors...", "Enter " + String(nameResponse.getResponseText()) + "'s profile link", ui.ButtonSet.OK_CANCEL
    );
    if (uidResponse.getSelectedButton() !== ui.Button.OK)
      return false;

    var uid = uidResponse.getResponseText().slice(uidResponse.getResponseText().search("=") + 1).toString();
    var confirmation = ui.alert(
      "Adding new competitors...",
      "Is this correct?\nName: " + nameResponse.getResponseText() + "\nProfile: https://www.mousehuntgame.com/profile.php?snuid=" + uid,
      ui.ButtonSet.YES_NO
    );
    if (confirmation === ui.Button.YES)
      beingAdded.push([nameResponse.getResponseText().trim(), uidResponse.getResponseText().trim(), uid]);

    // Check if more should be added.
    return ui.Button.YES === ui.alert("Add another?", ui.ButtonSet.YES_NO);
  }


  var startTime = new Date().getTime();
  var existing = getCompetitors_().map(function (value) {return value[2];} );
  
  // Create a dialog to get the new person(s).
  var newMembers = [];
  // Assume all the rowid data can be obtained in a single query. We need 1 second per 2 days since
  // the beginning of the year, and some extra time to run the scoreboard function.
  var numberQueries = 1 + Math.floor((startTime - (new Date(Date.UTC(2018, 0, 1))).getTime()) / 86400000);
  while (_getNewMembers_(newMembers) && (new Date().getTime() - startTime) / 1000 < 225 - numberQueries)
  { // _getNewMembers does the work
  }
  
  // Check for any duplicates.
  const toAdd = [], skipped = [];
  newMembers.forEach(function (maybe) {
    if (existing.indexOf(maybe[2]) === -1)
      toAdd.push(maybe);
    else
      skipped.push(maybe);
  });

  // Import the existing data for these people.
  const wb = SpreadsheetApp.getActive();
  var n = 0;
  if (toAdd.length)
  {
    n = importExistingDailyData(toAdd);
    var memberSheet = wb.getSheetByName('Competitors');
    memberSheet.getRange(1 * 1 + memberSheet.getLastRow() * 1, 1, toAdd.length, toAdd[0].length).setValues(toAdd);
  }
  const messages = [];
  if (n > 0)
    messages.push('Added ' + n + ' rows of data for ' + toAdd.length + ' unique competitors.');
  if (n === 0 && toAdd.length)
    messages.push('Added competitor(s) that had no data rows as of the last MHCC database update. Be sure to click their profile(s)!');
  if (skipped.length)
    messages.push('Skipped ' + skipped.length + ' competitor(s) since they were already added.');
  if (messages.length)
    wb.toast(messages.join('\n'));
  console.info({message: 'Ran adder function', data: {toAdd: toAdd, skipped: skipped, rowsAdded: n}});
}



// Get records with unique LastSeen values just prior to and during this competition.
// If a competitor has not been seen in the 7 days prior to this competition's start date,
// their starting counts will be from the first record seen during the competition.
function importExistingDailyData(members, compStartDate)
{
  if (!members)
    members = getCompetitors_();
  if (!compStartDate)
    compStartDate = new Date(Date.UTC(2018, 0, 1));
  
  // Get the starting record by querying the start date, minus 7 days.
  var begin = new Date(Date.UTC(compStartDate.getUTCFullYear(), compStartDate.getUTCMonth(), compStartDate.getUTCDate()));
  begin.setUTCDate(begin.getUTCDate() - 7);
  var end = new Date();
  var rowidQueries = getRowidQueries_(members, begin, end);
  var rowids = extractROWIDs_(doSQLGET_(rowidQueries));
  
  // Collect the desired data rows
  var data = doSQLGET_(getRowQueries_(rowids));
  return printLog_(formatRows_(data, members));
}


/**
 * Returns a 2D array of the Competitor, competitor link, and competitor UID in MHCC
 * @return {Array <string>[]} a 2D array of name, profile link, and MHCC UID.
 */
function getCompetitors_()
{
  const members = SpreadsheetApp.getActive().getSheetByName("Competitors").getDataRange().getValues();
  if (members.length < 2)
    return;
  
  // Drop the first row (which is headers).
  const headers = members.shift();
  
  // Generate the UID strings from the profile links.
  return members.map(function (member) {
    member[2] = member[1].slice(member[1].search("=") + 1).toString();
    return member;
  });
}



/**
 * Construct the desired queries to get the rowids for records with LastSeen values that fall
 * within the given datespan. If the query would exceed the allowed POST length (~8000 char)
 * then multiple queries will be returned.
 * 
 * @param {Array <string>[]} members 2D array of Name | Link | UID for which to obtain Crown data
 * @param {Date} dateStart     Beginning Date object for when the member needed to have been seen.
 * @param {Date} dateEnd       Ending Date object for when the member needed to have been seen before.
 * @return {string[]}          Array of SQL queries that request member rows between the desired dates.
 */
function getRowidQueries_(members, dateStart, dateEnd)
{
  if (!members || !members.length || dateStart.getTime() === dateEnd.getTime())
  {
    console.warn({message: "Insufficient data for querying", data: {members: members, dateStart: dateStart, dateEnd: dateEnd}});
    return [];
  }
  
  const queries = [];
  const memUIDs = members.map(function (value) { return value[2]; });
  var SQL = "SELECT ROWID, UID, LastSeen, LastTouched FROM " + ftid + " WHERE LastSeen < " + dateEnd.getTime();
  SQL += " AND LastSeen >= " + dateStart.getTime() + " AND UID IN (";
  const sqlEnd = ") ORDER BY UID ASC, LastSeen ASC, LastTouched ASC";
  while (memUIDs.length)
  {
    queries.push(SQL);
    var sqlUIDs = [];
    var q = queries[queries.length - 1];
    do {
      sqlUIDs.push(memUIDs.pop());
    } while ((q + sqlUIDs.join(",") + sqlEnd).length < 8000 && memUIDs.length);
    
    queries[queries.length - 1] += sqlUIDs.join(",") + sqlEnd;
  }
  return queries;
}



/**
 * Extract the desired rowids from the day's data.
 * 
 * @param {Array[]} queryData [[ROWID | Member ID (ascending) | LastSeen (ascending) | LastTouched (ascending)]]
 * @return {string[]} Array of rowid information from the given records
 */
function extractROWIDs_(queryData)
{
  if (!queryData || !queryData.length || queryData[0].length !== 4)
    return [];
  
  // Iterate rows and keep the first value for each new member for each LS
  // (i.e. the first record of a new LastSeen instance).
  const rowids = [];
  const seen = {};
  for (var row = 0, len = queryData.length; row < len; ++row)
  {
    try {
      // Check if the member and this LastSeen is known.
      var uid = queryData[row][1];
      var ls = new Date(queryData[row][2]);
      // If we have seen this particular LastSeen, we don't need to collect this next record.
      if (seen[uid] && seen[uid][ls])
        continue;
      else
      {
        // This is a new rowid that needs to be fetched.
        rowids.push(queryData[row][0]);
        // Update the tracking container.
        if (!seen[uid])
          seen[uid] = {};
        seen[uid][ls] = true;
      }
    }
    catch(e) { console.error({error: e, data: {row: row, data: queryData, seen: seen}}); }
  }
  return rowids;
}
  

/**
 * Convert the input array of rowids into queries for the desired data.
 * [['Member', 'Link', 'Date', 'Last Seen', 'Last Crown', 'Gold', 'Silver', 'Bronze']];
 * @param {string[]} rowids The rowids of records to be obtained
 * @return {string[]} The SQL queries to execute in order to obtain all desired rowids.
 */
function getRowQueries_(rowids)
{
  if (!rowids || !rowids.length || !rowids[0].length)
    return [];
  
  const queries = [];
  const SQL = "SELECT Member, UID, LastTouched, LastSeen, LastCrown, Gold, Silver, Bronze FROM " + ftid + " WHERE ROWID IN (";
  const sqlEnd = ") ORDER BY LastTouched ASC";
  do {
    queries.push(SQL);
    var sqlRowIDs = [];
    var q = queries[queries.length - 1];
    do {
      sqlRowIDs.push(rowids.pop());
    } while ((q + sqlRowIDs.join(",") + sqlEnd).length < 8000 && rowids.length);
    
    queries[queries.length - 1] += sqlRowIDs.join(",") + sqlEnd;
  } while(rowids.length);
  
  return queries;
}



/**
 * Execute the given queries, and return the aggregated row response.
 * @param {string[]} queries SQL GET queries to be executed by the FusionTables service
 * @return {Array[]} the aggregated data records
 */
function doSQLGET_(queries)
{
  if (!queries || !queries.length || !queries[0].length)
    return;
  
  const data = [];
  do {
    var sql = queries.pop();
    if (!sql.length)
      console.error("No query to perform");
    else
    {
      try
      {
        var response = FusionTables.Query.sqlGet(sql);
        if (response.rows)
          Array.prototype.push.apply(data, response.rows);
      }
      catch(e)
      {
        console.error({message: 'SQL get error from FusionTables', params: {error: e, query: sql, remaining: queries}});
        // Re-raise the error.
        throw e;
      }
    }
    // Obey API rate limits.
    if (queries.length)
      Utilities.sleep(500);
  } while (queries.length);
  
  console.log({data: data, length: data.length});
  return data;
}



/**
 * Convert the given rows into printable, append-ready log data
 * @param {Array[]} rows Raw FusionTable data records
 * @param {Array <string>[]} competitors 2D array of the minimal competitor information
 * @return {Array[]} A combination of input row data and competitor data
 */
function formatRows_(rows, competitors)
{
  if (!rows || !rows.length || !rows[0].length)
    return [];

  /** @type {Object <string, string>} object converting a uid to the desired name */
  const nameFinder = (competitors || getCompetitors_()).reduce(function (acc, mem) {
    acc[mem[2]] = mem[0];
    return acc;
  }, {});

  return rows.map(function (data) {
    return [
      nameFinder[data[1]],
      "https://www.mousehuntgame.com/profile.php?snuid=" + data[1],
      new Date(data[2]),
      new Date(data[3]),
      new Date(data[4]),
      data[5],
      data[6],
      data[7]
    ];
  });
}



/**
 * Write to the end of the log sheet, and return the number of rows added.
 * @param {Array[]} newRows Data to be written to the log sheet
 * @return {number} number of rows of data added to the log sheet.
 */
function printLog_(newRows)
{
  if (!newRows || !newRows.length || !newRows[0].length)
    return 0;
  // Access the "Daily Log" spreadsheet.
  const log = SpreadsheetApp.getActive().getSheetByName('Daily Log');
  
  // Fill in the headers (i.e. this is the first time the sheet has been used).
  if (log.getDataRange().getValues().length === 0 || log.getDataRange().isBlank())
  {
    printHeaders_(log);
    SpreadsheetApp.flush();
  }
  
  // Bounds check all the data.
  const numCol = log.getLastColumn();
  const badRows = newRows.filter(function (row) { return row.length !== numCol; });
  if (badRows.length > 0)
  {
    console.error({message: "Incorrect log width in some rows.", badRows: badRows});
    return 0;
  }
  
  // Add new rows to the end if the sheet is nearly full.
  // (This shouldn't particularly be required, but is proactive in case Apps Script
  // decides to have issues with getRange reaching beyond the existing sheet and
  // requiring new rows be inserted for setData to work.)
  if (log.getLastRow() + 2 * newRows.length > log.getMaxRows())
    log.insertRowsAfter(log.getMaxRows() - 1, newRows.length);
  
  // Append new logs to the end of the log sheet.
  log.getRange(1 + log.getLastRow(), 1, newRows.length, newRows[0].length).setValues(newRows);
  SpreadsheetApp.flush();
  sortLog_(log);
  return newRows.length;
}


/**
 * Sorts the daily log sheet.
 * @param {GoogleAppsScript.Spreadsheet.Sheet} [log] the sheet to sort (defaults to the sheet titled "Daily Log").
 */
function sortLog_(log)
{
  if(!log)
    log = SpreadsheetApp.getActive().getSheetByName('Daily Log');
  
  log.getRange(2, 1, log.getLastRow() - 1, log.getLastColumn())
    .sort([
      { column: 4, ascending: true }, // Sort by Last Seen
      { column: 1, ascending: true }, // Then by Name
      { column: 3, ascending: true }  // Then by Last Touched
    ]);
}



/**
 * Append the log headers to the given sheet.
 * @param {GoogleAppsScript.Spreadsheet.Sheet} logSheet The sheet used to track daily crown updates
 */
function printHeaders_(logSheet)
{
  if(!logSheet)
    return;
  
  var header = ['Member', 'Link', 'Date', 'Last Seen', 'Last Crown', 'Gold', 'Silver', 'Bronze'];
  logSheet.appendRow(header);
}



function runDaily()
{
  // Determine the last allowable time a record may have.
  const now = new Date();
  var queryEnd = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  var queryBegin = new Date(queryEnd);
  queryBegin.setDate(queryEnd.getDate() - 1);
  
  const members = getCompetitors_();
  if (members && members.length)
  {
    var dayQuery = getRowidQueries_(members, queryBegin, queryEnd);
    var rowids = extractROWIDs_(doSQLGET_(dayQuery));
    var queries = getRowQueries_(rowids);
    var data = doSQLGET_(queries);
    var toPrint = formatRows_(data);
    if (printLog_(toPrint))
      doScoreboardUpdate();
  }
  
  console.info({message: 'Ran daily crown race update', data: toPrint});
}



// Summarize the data for a Scoreboard update.
function doScoreboardUpdate()
{
  const memberList = getCompetitors_();
  // Assemble a per-competitor object, indexed by link (since link is printed on the log sheet).
  /** @type {Object <string, Object <string, any>>} */
  const members = memberList.reduce(function (acc, member) {
    var uid = member[2], link = "https://www.mousehuntgame.com/profile.php?snuid=" + uid;
    acc[link] = {
      name: member[0],
      uid: uid,
      link: link,
      historyLink: "https://script.google.com/macros/s/AKfycbxvvtBNQ66BBlB-md1jn_y-TlujQf1ytDkYG-7nEAG4SDaecMFF/exec?uid=" + uid,
      startSilver: 0,
      startGold: 0,
      startRecordDate: new Date(2099, 0, 1),
      currentRecordDate: new Date(0),
      gold: 0,
      silver: 0,
      bronze: 0,
      lastSeen: new Date(0),
      lastCrown: new Date(0),
      data: []
    };
    return acc;
  }, {});

  // Load the current datalog (which is sorted chronologically and by member).
  const log = SpreadsheetApp.getActive().getSheetByName('Daily Log');
  sortLog_(log);
  var data = log.getDataRange().getValues();
  const headers = data.shift();

  // Assign the relevant data indexes.
  const linkIndex = headers.indexOf('Link'),
    silverIndex = headers.indexOf("Silver"),
    goldIndex = headers.indexOf("Gold"),
    bronzeIndex = headers.indexOf("Bronze"),
    seenIndex = headers.indexOf("Last Seen"),
    crownIndex = headers.indexOf("Last Crown");

  // First, loop through and collate each row with each member so that starting counts can be assessed.
  data.forEach(function (row) { members[row[linkIndex]].data.push(row); });

  const competitionBegin = new Date(Date.UTC(2018, 0, 1)),
    output = [];
  for (var link in members) {
    var m = members[link];
    // Compute the starting counts, and determine the most recent counts.
    m.data.forEach(function (rowData) {
      var recordDate = new Date(rowData[seenIndex]);
      // Only records with a relevant LastSeen value have been collected (within 7 days of the comp start). The
      // first one that is non-zero is used as the starting count record (unless others records closer to the
      // beginning of the competition are available).
      if (rowData[bronzeIndex]-0 + rowData[silverIndex]-0 + rowData[goldIndex]-0 > 0
        && (recordDate < m.startRecordDate || (recordDate < competitionBegin && recordDate >= m.startRecordDate)))
      {
        m.startSilver = rowData[silverIndex];
        m.startGold = rowData[goldIndex];
        m.startRecordDate = recordDate;
      }
      // Update the member's object with this record.
      if (recordDate > m.currentRecordDate)
      {
        m.gold = rowData[goldIndex];
        m.silver = rowData[silverIndex];
        m.bronze = rowData[bronzeIndex];
        m.lastSeen = new Date(rowData[seenIndex]);
        m.lastCrown = new Date(rowData[crownIndex]);
        m.currentRecordDate = recordDate;
      }
    });

    // Summarize the member's progress
    output.push([
      0,
      "=hyperlink(\"" + m.link + "\", \"" + m.name + "\")",
      ((m.silver - m.startSilver) * 1 + (m.gold - m.startGold) * 1),
      m.startSilver,
      m.gold,
      m.silver,
      m.bronze,
      "=hyperlink(\"" + m.historyLink + "\", \"" + (m.gold + m.silver + m.bronze) + "\")",
      m.lastSeen,
      m.lastCrown
    ]);
  }

  // Sort the scoreboard data table by silvers earned.
  output.sort(function (a, b) {return b[2] - a[2];});
  
  // Update the ranks in the sorted scoreboard.
  var rank = 0;
  output.forEach(function (row) { row[0] = ++rank; });
  
  SpreadsheetApp.getActive().getSheetByName("Scoreboard").getRange(2, 1, output.length, output[0].length)
      .setValues(output).getSheet().getRange("L1").setValue(new Date());
}
