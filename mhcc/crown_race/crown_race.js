// @OnlyCurrentDoc
function onOpen()
{
  SpreadsheetApp.getActiveSpreadsheet()
      .addMenu("Admin",
               [{name:"Add New Competitor", functionName:"addCompetitor"},
                {name:"Update Scoreboard", functionName:"doScoreboardUpdate"}
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
    if (confirmation == ui.Button.YES)
      beingAdded.push([String(nameResponse.getResponseText()).trim(), String(uidResponse.getResponseText()).trim(), uid]);

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
  while(_getNewMembers_(newMembers) && ((new Date().getTime() - startTime)/1000) < (225 - numberQueries))
  {
  }
  
  // Check for any duplicates.
  var toAdd = [], skipped = [];
  for(var maybe = 0; maybe < newMembers.length; ++maybe)
  {
    if(existing.indexOf(newMembers[maybe][2]) === -1)
      toAdd.push(newMembers[maybe]);
    else
      skipped.push(newMembers[maybe]);
  }
  
  // Import the existing data for these people.
  var wb = SpreadsheetApp.getActive();
  var n = 0;
  if(toAdd.length)
  {
    n = importExistingDailyData(toAdd);
    var memberSheet = wb.getSheetByName('Competitors');
    memberSheet.getRange(1 * 1 + memberSheet.getLastRow() * 1, 1, toAdd.length, toAdd[0].length).setValues(toAdd);
  }
  var message = '';
  if(n > 0)
    message += 'Added ' + n + ' rows of data for ' + toAdd.length + ' unique competitors.\n';
  if(n === 0 && toAdd.length)
    message += 'Added competitor(s) that had no data rows as of the last MHCC database update. Be sure to click their profile(s)!\n';
  if(skipped.length)
    message += 'Skipped ' + skipped.length + ' competitor(s) since they were already added.';
  if(message != '')
    wb.toast(message);
  console.info({message: 'Ran adder function', data: {toAdd: toAdd, skipped: skipped, rowsAdded: n}});
}



// Get records with unique LastSeen values just prior to and during this competition.
// If a competitor has not been seen in the 7 days prior to this competition's start date,
// their starting counts will be from the first record seen during the competition.
function importExistingDailyData(members, compStartDate)
{
  if(!members)
    members = getCompetitors_();
  if(!compStartDate)
    compStartDate = new Date(Date.UTC(2018, 0, 1));
  
  var rowidQueries = [];
  
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
 * @return {String[][]} 
 */
function getCompetitors_()
{
  var members = SpreadsheetApp.getActive().getSheetByName("Competitors").getDataRange().getValues();
  if(members.length <= 1)
    return;
  
  // Drop the first row (which is headers).
  members.splice(members[0], 1);
  
  // Generate the UID strings from the profile links.
  for(var row = 0, len = members.length; row < len; ++row)
    members[row][2] = members[row][1].slice(members[row][1].search("=") + 1).toString();
  
  return members;
}



/**
 * Construct the desired queries to get the rowids for records with LastSeen values that fall
 * within the given datespan. If the query would exceed the allowed POST length (~8000 char)
 * then multiple queries will be returned.
 * 
 * @param {String[][]} members 2D array of Name | Link | UID for which to obtain Crown data
 * @param {Date} dateStart     Beginning Date object for when the member needed to have been seen.
 * @param {Date} dateEnd       Ending Date object for when the member needed to have been seen before.
 */
function getRowidQueries_(members, dateStart, dateEnd)
{
  if(!members || !members.length || dateStart.getTime() === dateEnd.getTime())
  {
    console.warn({message: "Insufficient data for querying", data: {members: members, dateStart: dateStart, dateEnd: dateEnd}});
    return [];
  }
  
  var queries = [];
  var memUIDs = members.map(function (value) { return value[2] });
  var SQL = "SELECT ROWID, UID, LastSeen, LastTouched FROM " + ftid + " WHERE LastSeen < " + dateEnd.getTime();
  SQL += " AND LastSeen >= " + dateStart.getTime() + " AND UID IN (";
  const sqlEnd = ") ORDER BY UID ASC, LastSeen ASC, LastTouched ASC";
  while(memUIDs.length)
  {
    queries.push(SQL);
    var sqlUIDs = [];
    var q = queries[queries.length - 1];
    do {
      sqlUIDs.push(memUIDs.pop());
    } while((q + sqlUIDs.join(",") + sqlEnd).length < 8000 && memUIDs.length)
    
    queries[queries.length - 1] += sqlUIDs.join(",") + sqlEnd;
  }
  return queries;
}



/**
 * Extract the desired rowids from the day's data.
 * 
 * @param {any[][]} queryData [[ROWID | Member ID (ascending) | LastSeen (ascending) | LastTouched (ascending)]]
 * @return {String[]}
 */
function extractROWIDs_(queryData)
{
  if(!queryData || !queryData.length || queryData[0].length != 4)
    return [];
  
  // Iterate rows and keep the first value for each new member for each LS
  // (i.e. the first record of a new LastSeen instance).
  var rowids = [];
  var seen = {};
  for(var row = 0, len = queryData.length; row < len; ++row)
  {
    try {
      // Check if the member and this LastSeen is known.
      var uid = queryData[row][1];
      var ls = new Date(queryData[row][2]);
      // If we have seen this particular LastSeen, we don't need to collect this next record.
      if(seen[uid] && seen[uid][ls])
        continue;
      else
      {
        // This is a new rowid that needs to be fetched.
        rowids.push(queryData[row][0]);
        // Update the tracking container.
        if(!seen[uid])
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
 * @param {String[]} rowids
 */
function getRowQueries_(rowids)
{
  if(!rowids || !rowids.length || !rowids[0].length)
    return [];
  
  var queries = [];
  const SQL = "SELECT Member, UID, LastTouched, LastSeen, LastCrown, Gold, Silver, Bronze FROM " + ftid + " WHERE ROWID IN (";
  const sqlEnd = ") ORDER BY LastTouched ASC";
  do {
    queries.push(SQL);
    var sqlRowIDs = [];
    var q = queries[queries.length - 1];
    do {
      sqlRowIDs.push(rowids.pop());
    } while((q + sqlRowIDs.join(",") + sqlEnd).length < 8000 && rowids.length)
    
    queries[queries.length - 1] += sqlRowIDs.join(",") + sqlEnd;
  } while(rowids.length);
  
  return queries;
}



/**
 * Execute the given queries, and return the aggregated row response.
 * @param {String[]} queries
 * @return {any[][]}
 */
function doSQLGET_(queries)
{
  if(!queries || !queries.length || !queries[0].length)
    return;
  
  const data = [];
  do {
    var sql = queries.pop();
    if(!sql.length)
      console.error("No query to perform");
    else
    {
      try
      {
        var response = FusionTables.Query.sqlGet(sql);
        if(response.rows)
          Array.prototype.push.apply(data, response.rows);
      }
      catch(e)
      {
        console.error({message: 'SQL get error from FusionTables', params:{error: e, query: sql, remaining: queries}});
        // Re-raise the error.
        throw e;
      }
    }
    // Obey API rate limits.
    if(queries.length)
      Utilities.sleep(500);
  } while (queries.length);
  
  console.log({data: data, length: data.length});
  return data;
}



/**
 * Convert the given rows into printable, append-ready data
 * @param {any[][]} rows
 * @param {String[][]} competitors
 */
function formatRows_(rows, competitors)
{
  if(!rows || !rows.length || !rows[0].length)
    return [];
  
  if(!competitors)
    competitors = getCompetitors_();
  var lookup = {};
  for(var row = 0, len = competitors.length; row < len; ++row)
    lookup[competitors[row][2]] = competitors[row][0];
  
  var output = [];
  for(var row = 0, len = rows.length; row < len; ++row)
  {
    var data = rows[row];
    output.push(
      [
        lookup[data[1]],
        ("https://www.mousehuntgame.com/profile.php?snuid=" + data[1]),
        (new Date(data[2])),
        (new Date(data[3])),
        (new Date(data[4])),
        data[5],
        data[6],
        data[7]
      ]
    );
  }
  
  return output;
}



/**
 * Write to the end of the log sheet, and return the number of rows added.
 * @param {any[][]} newRows
 * @return {Number}
 */
function printLog_(newRows)
{
  if(!newRows || !newRows.length || !newRows[0].length)
    return 0;
  // Access the "Daily Log" spreadsheet.
  var log = SpreadsheetApp.getActive().getSheetByName('Daily Log');
  
  // Fill in the headers (i.e. this is the first time the sheet has been used).
  if(log.getDataRange().getValues().length === 0 || log.getDataRange().isBlank())
  {
    printHeaders_(log);
    SpreadsheetApp.flush();
  }
  
  // Bounds check all the data.
  var numCol = log.getLastColumn();
  var badRows = newRows.filter(function (row) { return row.length !== numCol });
  if(badRows.length > 0)
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



function sortLog_(log)
{
  if(!log)
    log = SpreadsheetApp.getActive().getSheetByName('Daily Log');
  
  log.getRange(2, 1, log.getLastRow() - 1, log.getLastColumn())
      .sort([{column: 3, ascending: true}, {column: 1, ascending: true}]);
}



// Append the log headers to the given sheet.
function printHeaders_(logSheet)
{
  if(!logSheet)
    return;
  
  var head = ['Member', 'Link', 'Date', 'Last Seen', 'Last Crown', 'Gold', 'Silver', 'Bronze'];
  logSheet.appendRow(head);
}



function runDaily()
{
  // Determine the last allowable time a record may have.
  var now = new Date();
  var queryEnd = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  var queryBegin = new Date(queryEnd);
  queryBegin.setDate(queryEnd.getDate() - 1);
  
  var members = getCompetitors_();
  if(members)
  {
    var dayQuery = getRowidQueries_(members, queryBegin, queryEnd);
    var rowids = extractROWIDs_(doSQLGET_(dayQuery));
    var queries = getRowQueries_(rowids);
    var data = doSQLGET_(queries);
    var toPrint = formatRows_(data);
    if(printLog_(toPrint))
      doScoreboardUpdate();
  }
  
  console.info({message: 'Ran daily crown race update', data: toPrint});
}



// Summarize the data for a Scoreboard update.
function doScoreboardUpdate()
{
  var memberList = getCompetitors_();
  // Assemble a per-competitor object.
  var members = {};
  for(var row = 0, len = memberList.length; row < len; ++row)
    members[memberList[row][0]] = {
      uid: memberList[row][2],
      link: "https://www.mousehuntgame.com/profile.php?snuid=" + memberList[row][2],
      historyLink: "https://script.google.com/macros/s/AKfycbxvvtBNQ66BBlB-md1jn_y-TlujQf1ytDkYG-7nEAG4SDaecMFF/exec?uid=" + memberList[row][2],
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
  
  // Load the current datalog (which is sorted chronologically and by member).
  var log = SpreadsheetApp.getActive().getSheetByName('Daily Log');
  sortLog_(log);
  var data = log.getDataRange().getValues();
  var headers = data.splice(0, 1)[0];
  
  // Assign the relevant data indexes.
  var silverIndex = headers.indexOf("Silver");
  var goldIndex = headers.indexOf("Gold");
  var bronzeIndex = headers.indexOf("Bronze");
  
  // First, loop through and collate each row with each member so that starting counts can be assessed.
  for(var row = 0, len = data.length; row < len; ++row)
    members[data[row][0]].data.push(data[row]);
  
  var competitionBegin = new Date(Date.UTC(2018, 0, 1));
  for(var name in members)
    for(var row = 0, len = members[name].data.length; row < len; ++row)
    {
      var rowData = members[name].data[row];
      var recordDate = new Date(rowData[2]);
      // Only records with a relevant LastSeen value have been collected (within 7 days of the comp start). The
      // first one that is non-zero is used as the starting count record (unless others records closer to the
      // beginning of the competition are available).
      if((rowData[bronzeIndex] - 0 + rowData[silverIndex] - 0 + rowData[goldIndex] - 0) > 0
         && (recordDate < members[name].startRecordDate || (recordDate < competitionBegin
           && recordDate >= members[name].startRecordDate)))
      {
        members[name].startSilver = rowData[silverIndex];
        members[name].startGold = rowData[goldIndex];
        members[name].startRecordDate = recordDate;
      }
      // Update the member's object with this record.
      if(recordDate > members[name].currentRecordDate)
      {
        members[name].gold = rowData[goldIndex];
        members[name].silver = rowData[silverIndex];
        members[name].bronze = rowData[bronzeIndex];
        members[name].lastSeen = new Date(rowData[3]);
        members[name].lastCrown = new Date(rowData[4]);
        members[name].currentRecordDate = recordDate;
      }
    }
  
  // Generate the output array.
  var output = [];
  for(var name in members)
    output.push(
      [
        0,
        ("=hyperlink(\"" + members[name].link + "\", \"" + name + "\")"),
        ((members[name].silver - members[name].startSilver) * 1 + (members[name].gold - members[name].startGold) * 1),
        members[name].startSilver,
        members[name].gold,
        members[name].silver,
        members[name].bronze,
        ("=hyperlink(\"" + members[name].historyLink + "\", \"" + String(members[name].gold * 1 + members[name].silver * 1 + members[name].bronze * 1) + "\")"),
        members[name].lastSeen,
        members[name].lastCrown
      ]
    );
  
  // Sort the scoreboard data table by silvers earned.
  output.sort(function (a, b) {return b[2] - a[2];});
  
  // Update the ranks in the sorted scoreboard.
  var rank = 0;
  for(var row = 0, len = output.length; row < len; ++row)
    output[row][0] = ++rank;
  
  SpreadsheetApp.getActive().getSheetByName("Scoreboard").getRange(2, 1, output.length, output[0].length)
      .setValues(output).getSheet().getRange("L1").setValue(new Date());
}
