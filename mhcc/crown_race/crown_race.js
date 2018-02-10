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
  var startTime = new Date().getTime();
  var existing = getCompetitors_().map( function (value, index) { return value[2] } );
  
  // Create a dialog to get the new person(s).
  var newMembers = [];
  // Assume all the rowid data can be obtained in a single query. We need 1 second per 2 days since
  // the beginning of the year, and some extra time to run the scoreboard function.
  var numberQueries = 1 + Math.floor((startTime - (new Date(Date.UTC(2018, 0, 1))).getTime()) / 86400000);
  while(getNewMembers_(newMembers) && ((new Date().getTime() - startTime)/1000) < (225 - numberQueries))
  {
  }
  
  // Check for any duplicates.
  var toAdd = [], skipped = [];
  for(var maybe = 0; maybe < newMembers.length; ++maybe)
  {
    if(existing.indexOf(newMembers[maybe][2]) == -1)
      toAdd.push(newMembers[maybe]);
    else
      skipped.push(newMembers[maybe]);
  }
  
  // Import the existing data for these people.
  if(toAdd.length)
  {
    importExistingDailyData(toAdd);
    var memberSheet = SpreadsheetApp.openById(ssid).getSheetByName('Competitors');
    memberSheet.getRange(1 * 1 + memberSheet.getLastRow() * 1, 1, toAdd.length, toAdd[0].length).setValues(toAdd);
  }
  if(toAdd.length || skipped.length)
    SpreadsheetApp.getActiveSpreadsheet().toast(
      "Finished adding data for " + toAdd.length + " unique competitors.\nSkipped: "
      + skipped.length + " competitor(s) since they were already added.");
  console.info({message:'Ran adder function', data:{toAdd:toAdd, skipped:skipped}});
}



function getNewMembers_(toAdd)
{
  var ui = SpreadsheetApp.getUi();
  var nameResponse = ui.prompt(
    "Adding new competitors...", "Enter the new competitor's name", ui.ButtonSet.OK_CANCEL
  );
  if(nameResponse.getSelectedButton() != ui.Button.OK)
    return false;
  
  var uidResponse = ui.prompt(
    "Adding new competitors...", "Enter " + String(nameResponse.getResponseText()) + "'s profile link", ui.ButtonSet.OK_CANCEL
  );
  if(uidResponse.getSelectedButton() != ui.Button.OK)
    return false;
  
  var uid = uidResponse.getResponseText().slice(uidResponse.getResponseText().search("=") + 1).toString();
  var confirmation = ui.alert(
    "Adding new competitors...",
    "Is this correct?\nName: " + nameResponse.getResponseText() + "\nProfile: https://www.mousehuntgame.com/profile.php?snuid=" + uid,
    ui.ButtonSet.YES_NO
  );
  if(confirmation == ui.Button.YES)
    toAdd.push([nameResponse.getResponseText(), uidResponse.getResponseText(), uid]);
  
  // Check if more should be added.
  return ui.Button.YES == ui.alert("Add another?", ui.ButtonSet.YES_NO);
}



// Loop over all days leading up to this one, and grab the most recent row still on that date.
// e.g. get 2017-12-31 23:59:59, 2018-01-01 23:59:59, etc.
function importExistingDailyData(members)
{
  if(!members)
    members = getCompetitors_();
  
  var begin = new Date(Date.UTC(2018, 0, 2));
  var end = new Date();
  // Create a query for each day that has elapsed.
  var rowidQueries = [];
  while(begin < end)
  {
    var queryBegin = new Date(begin);
    queryBegin.setUTCDate(queryBegin.getUTCDate() - 1);
    rowidQueries.push(getRowidQueries_(members, queryBegin, begin));
    begin.setUTCDate(begin.getUTCDate() * 1 + 1 * 1);
  }
  // Evaluate each day's query to collect the needed ROWID information.
  var rowids = extractROWIDs_(doSQLGET_(rowidQueries));
  // Search for the preliminary starting counts record from the whole month of December.
  // (If maintenance is run, it is possible that a member's record from the 31st does not exist.
  // Due to GWH, it is unlikely that members do not have a record change in the whole month of December.)
  var firstQuery = getRowidQueries_(members, new Date(Date.UTC(2017, 10, 1)), new Date(Date.UTC(2018, 0, 1)));
  rowids.push(extractStartingROWID_(doSQLGET_(firstQuery)));
  
  // Construct the query to obtain the desired log information.
  var queries = getRowQueries_(rowids);
  // Collect the desired data rows
  var data = doSQLGET_(queries);
  var toPrint = formatRows_(data, members);
  printLog_(toPrint);
}



function getCompetitors_()
{
  var members = SpreadsheetApp.openById(ssid).getSheetByName("Competitors").getDataRange().getValues();
  if(members.length <= 1)
    return;
  
  // Drop the first row (which is headers).
  members.splice(members[0], 1);
  
  // Generate the UID strings from the profile links.
  for(var row = 0; row < members.length; ++row)
    members[row][2] = members[row][1].slice(members[row][1].search("=") + 1).toString();
  
  return members;
}



// Construct the desired queries to get the rowids for records falling within the datespan.
function getRowidQueries_(members, dateStart, dateEnd)
{
  if(!members || dateStart == dateEnd)
  {
    console.warn({message:"Insufficient data for querying",data:{members:members, dateStart:dateStart, dateEnd:dateEnd}});
    return members;
  }
  var queries = [];
  var memUIDs = members.map( function (value, index) { return value[2] } );
  var SQL = "SELECT ROWID, UID, LastTouched FROM " + ftid + " WHERE LastTouched < " + dateEnd.getTime();
  SQL += " AND LastTouched >= " + dateStart.getTime() + " AND UID IN (";
  var sqlEnd = ") ORDER BY UID ASC, LastTouched DESC";
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



// Extract the desired rowids from the day's data.
function extractROWIDs_(queryData)
{
  if(!queryData || !queryData.length || queryData[0].length != 3)
    return [];
  
  // ROWID | Member ID (ascending) | LastTouched (descending).
  // Iterate rows and keep the first value for each new member for each date.
  var rowids = [];
  var seen = {};
  for(var row = 0; row < queryData.length; ++row)
  {
    try {
      // Check if the member and this date is known.
      var uid = queryData[row][1];
      var day = new Date(queryData[row][2]);
      day = new Date(Date.UTC(day.getUTCFullYear(), day.getUTCMonth(), day.getUTCDate()));
      if(seen[uid] && seen[uid][day])
        continue;
      // Check if this member is known (but the date is not).
      else if(seen[uid])
      {
        seen[uid][day] = true;
        rowids.push(queryData[row][0]);
      }
      // Add a new member and the new date.
      else
      {
        seen[uid] = {};
        seen[uid][day] = true;
        rowids.push(queryData[row][0]);
      }
    }
    catch(e)
    {
      console.error({error:e, data:{row:row, data:queryData, seen:seen}});
    }
  }
  return rowids;
}
function extractStartingROWID_(queryData)
{
  if(!queryData || !queryData.length || queryData[0].length != 3)
    return [];
  
  // ROWID | Member ID (ascending) | LastTouched (descending).
  // Iterate rows and keep the first value for each new member.
  var rowids = [];
  var seen = {};
  for(var row = 0; row < queryData.length; ++row)
  {
    try {
      // Check if the member and this date is known.
      var uid = queryData[row][1];
      var day = new Date(queryData[row][2]);
      day = new Date(Date.UTC(day.getUTCFullYear(), day.getUTCMonth(), day.getUTCDate()));
      if(seen[uid])
        continue;
      else
      {
        seen[uid] = true;
        rowids.push(queryData[row][0]);
      }
    }
    catch(e)
    {
      console.error({error:e, data:{row:row, data:queryData, seen:seen}});
    }
  }
  return rowids;
}
  


// Construct data queries from the given rowids.
// [['Member', 'Link', 'Date', 'Last Seen', 'Last Crown', 'Gold', 'Silver', 'Bronze']];
function getRowQueries_(rowids)
{
  if(!rowids || !rowids.length || !rowids[0].length)
    return [];
  
  var queries = [];
  var SQL = "SELECT Member, UID, LastTouched, LastSeen, LastCrown, Gold, Silver, Bronze FROM " + ftid + " WHERE ROWID IN (";
  var sqlEnd = ") ORDER BY LastTouched ASC";
  while(rowids.length)
  {
    queries.push(SQL);
    var sqlRowIDs = [];
    var q = queries[queries.length - 1];
    do {
      sqlRowIDs.push(rowids.pop());
    } while((q + sqlRowIDs.join(",") + sqlEnd).length < 8000 && rowids.length)
    
    queries[queries.length - 1] += sqlRowIDs.join(",") + sqlEnd;
  }
  return queries;
}



// Execute the given queries, and return the aggregated row response.
function doSQLGET_(queries)
{
  if(!queries || !queries.length || !queries[0].length)
    return;
  
  var data = [];
  while(queries.length)
  {
    var sql = queries.pop();
    if(!sql.length)
      console.error("No query to perform");
    else
    {
      var response = FusionTables.Query.sqlGet(sql);
      if(response.rows)
        data = [].concat(data, response.rows);
    }
    // Obey API rate limits.
    if(queries.length)
      Utilities.sleep(500);
  }
  console.log({data:data, length:data.length});
  return data;
}



// Convert the given rows into printable, append-ready data
function formatRows_(rows, competitors)
{
  if(!rows || !rows.length || !rows[0].length)
    return [];
  
  if(!competitors)
    competitors = getCompetitors_();
  var lookup = {};
  for(var row = 0; row < competitors.length; ++row)
    lookup[competitors[row][2]] = competitors[row][0];
  
  var output = [];
  for(var row = 0; row < rows.length; ++row)
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



// Write to the end of the log sheet
function printLog_(newRows)
{
  if(!newRows || !newRows[0])
    return;
  // Access the "Daily Log" spreadsheet.
  var log = SpreadsheetApp.openById(ssid).getSheetByName('Daily Log');
  
  // Fill in the headers (i.e. this is the first time the sheet has been used).
  if(log.getDataRange().getValues().length == 0 || log.getDataRange().isBlank())
  {
    printHeaders_(log);
    SpreadsheetApp.flush();
  }
  
  // Bounds check all the data.
  var numCol = log.getLastColumn()
  for(var row = 0; row < newRows.length; ++row)
    if(newRows[row].length != numCol)
    {
      console.error({message:"Incorrect log width in row " + row, data:{data:newRows, badRow:newRows[row]}});
      return;
    }
  
  // Append new logs to the end of the log sheet.
  log.getRange(1 + log.getLastRow(), 1, newRows.length, newRows[0].length).setValues(newRows);
  SpreadsheetApp.flush();
  sortLog_(log);
}



function sortLog_(log)
{
  if(!log)
    log = SpreadsheetApp.openById(ssid).getSheetByName('Daily Log');
  
  log.getRange(2, 1, log.getLastRow(), log.getLastColumn())
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
    printLog_(toPrint);
    doScoreboardUpdate();
  }
  
  console.info({message:'Ran daily crown race update', data:toPrint});
}



// Summarize the data for a Scoreboard update.
function doScoreboardUpdate()
{
  var memberList = getCompetitors_();
  // Assemble a per-competitor object.
  var members = {};
  for(var row = 0; row < memberList.length; ++row)
    members[memberList[row][0]] = {
      uid: memberList[row][2],
      link: "https://www.mousehuntgame.com/profile.php?snuid=" + memberList[row][2],
      historyLink: "https://script.google.com/macros/s/AKfycbwCT-oFMrVWR92BHqpbfPFs_RV_RJPQNV5pHnZSw6yO2CoYRI8/exec?uid=" + memberList[row][2],
      startSilver: 0,
      startGold: 0,
      startRecordDate: new Date(2020, 0, 1),
      currentRecordDate: new Date(0),
      gold: 0,
      silver: 0,
      bronze: 0,
      lastSeen: new Date(2020, 0, 1),
      lastCrown: new Date(2020, 0, 1),
      data: []
    };
  
  // Load the current datalog (which is sorted chronologically and by member).
  var log = SpreadsheetApp.openById(ssid).getSheetByName('Daily Log');
  sortLog_(log);
  var data = log.getDataRange().getValues();
  var headers = data.splice(0, 1)[0];
  
  // Assign the relevant data indexes.
  var silverIndex = headers.indexOf("Silver");
  var goldIndex = headers.indexOf("Gold");
  var bronzeIndex = headers.indexOf("Bronze");
  
  // First, loop through and collate each row with each member so that starting counts can be assessed.
  for(var row = 0; row < data.length; ++row)
    members[data[row][0]].data.push(data[row]);
  
  var competitionBegin = new Date(Date.UTC(2018, 0, 1));
  for(var name in members)
    for(var row = 0; row < members[name].data.length; ++row)
    {
      var recordDate = new Date(members[name].data[row][2]);
      // Since the default "start" record is after the end of the competition, and these datasets are
      // chronologically ascending, the first record that is before the default is the starting metric.
      // If others are greater than it but also before the start time of the competition, those are to
      // be used instead.
      if(members[name].data[row][bronzeIndex] > 0 && (recordDate < members[name].startRecordDate
         || (recordDate < competitionBegin && recordDate >= members[name].startRecordDate)))
      {
        members[name].startSilver = members[name].data[row][silverIndex];
        members[name].startGold = members[name].data[row][goldIndex];
        members[name].startRecordDate = recordDate;
      }
      // Update the member's object with this record.
      if(recordDate > members[name].currentRecordDate)
      {
        members[name].gold = members[name].data[row][goldIndex];
        members[name].silver = members[name].data[row][silverIndex];
        members[name].bronze = members[name].data[row][bronzeIndex];
        members[name].lastSeen = new Date(members[name].data[row][3]);
        members[name].lastCrown = new Date(members[name].data[row][4]);
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
  
  // Sort the scoreboard data table.
  output.sort(
    function (a, b) { if(a[2]*1 < b[2]*1){ return 1; } else if(a[2]*1 > b[2]*1) { return -1; } return 0; }
  );
  
  // Update the ranks in the sorted scoreboard.
  var rank = 0;
  for(var row = 0; row < output.length; ++row)
    output[row][0] = ++rank;
  
  SpreadsheetApp.openById(ssid).getSheetByName("Scoreboard").getRange(2, 1, output.length, output[0].length)
      .setValues(output).getSheet().getRange("L1").setValue(new Date());
  
}
