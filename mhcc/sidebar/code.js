function getSidebar() {
  SpreadsheetApp.getUi().showSidebar(doSideBar_());
}

function doSideBar_() {
  var filename = "adminPanel\\UI.html";
  var sb = HtmlService.createHtmlOutputFromFile(filename);
  sb.setTitle("MHCC Admin Panel").setFaviconUrl("https://i.imgur.com/QMghA1l.png");
  return sb;
}

// Repeat the data validation performed on the sidebar.
function validateSidebarInput_(form)
{
  var validity = false, name = "", uid = "", error;
  try
  {
    name = form.memberName.trim();
    uid = form.memberLink.slice(form.memberLink.search("=") + 1).toString();
    validity = (name.length > 0 && uid.length > 0);
  }
  catch (e) { error = e;}
  return {form:form, isValid: validity, name: name, uid: uid, error: error};
}

function getMemberInfo(uid)
{
  var q = "SELECT Member, UID, ROWID FROM " + utbl + " WHERE UID = '" + uid + "'";
  // Get the member information from the user table.
  var resp = FusionTables.Query.sqlGet(q);
  if (resp.kind !== "fusiontables#sqlresponse" || !resp.columns.length)
  {
    var e = TypeError("Invalid response received");
    e.inputData = {sql: q, response: resp};
    throw e;
  }
  console.log({message: "GetMemberInfo", ftResponse: resp, ftQuery: q});
  // Received a well-formed response.
  if (resp.rows && resp.rows.length > 0)
  {
    var member = resp.rows;
    var index = member.map(function (v) { return v[1]; }).indexOf(uid);
    if (index > -1)
      return {name: member[index][0], uid: uid, rowid: member[index][2]};
  }
  // No members found in the response.
  else
    return {
      name: "", uid: uid, rowid: "",
      error: Error("No member found with uid='" + uid + "'")
    };
}

// Query FT to determine if this member can be added.
function canAdd(form)
{
  var input = validateSidebarInput_(form);
  var output = {
    reset: false,
    request: input,
    canDo: false,
    log: "",
    report: "",
  };
  if(input.isValid)
  {
    var member = getMemberInfo(input.uid);
    // If an error occurred (i.e. there is no member info), we can add this member.
    if (member.error)
    {
      output.canDo = true;
      output.log = "No existing member found, add away!";
    }
    else
    {
      // We found a member with this uid. If the name is different,
      // we can perform a name change operation. Otherwise, no operation is possible.
      output.isNameChange = (input.name !== member.name);
      output.rowid = member.rowid;
      output.currentName = member.name;
      output.uid = member.uid;
      if (output.isNameChange)
      {
        output.canDo = true;
        output.log = "Member already exists as '" + member.name + "'.\n\tUpdate name?";
      }
      else
        output.log = "'" + input.name + "' already exists with UID='" + input.uid + "'.";
    }
  }
  else if (input.error)
    output.log = input.error.message;
  
  return output;
}

// Query FT to determine if this member can be deleted.
function canDelete(form)
{
  var input = validateSidebarInput_(form);
  var output = {
    reset: false,
    request: input,
    canDo: false,
    log: "",
    report: "",
  };
  if(input.isValid)
  {
    var member = getMemberInfo(input.uid);
    if (member.error)
    {
      // No member was found with this uid.
      output.canDo = false;
      output.log = member.error.message;
    }
    else
    {
      // Deletes require that the name is matched exactly.
      output.canDo = (input.name === member.name);
      if (!output.canDo)
        output.log = "Member exists with name '" + member.name + "'. Names must match.";
      else
      {
        output.rowid = member.rowid;
        // Get the count of rows in the crowns table.
        var q = "SELECT COUNT(UID) FROM " + ftid + " WHERE UID = '" + input.uid + "'";
        var resp = FusionTables.Query.sqlGet(q);
        if (resp.rows && resp.rows.length)
        {
          output.dataRows = resp.rows[0][0] + " ";
        }
        output.log = "Deleting '" + member.name + "' will also delete their " + output.dataRows + "crown records.";
      }
    }
  }
  else if(input.error)
    output.log = input.error.message;
  
  return output;
}

function changeMemberName(form)
{
  // Revalidate input, in case of trickery.
  var input = canAdd(form);
  var output = {
    reset: input.reset,
    request: input,
    log: "",
    report: ""
  };
  console.log({message: "Name Change", isNameChange: input.isNameChange, can_do: input.canDo, row: input.rowid, misc: input});
  if(input.canDo === true && input.isNameChange === true && input.rowid !== undefined && input.rowid !== null)
  {
    var sql = "UPDATE " + utbl + " SET Member = '" + input.request.name + "' WHERE ROWID = '" + input.rowid + "'";
    try {FusionTables.Query.sql(sql);}
    catch (e) {throw e;}
    output.report = "Name for uid='" + input.request.uid + "' is now '" + input.request.name + "'";
    output.reset = true;
  }
  else
    output.log = (input.canDo === false) ? "Cannot perform request." : "Invalid request. Revalidation is required.";
  
  return output;
}
  
function addMemberToFusion(form)
{
  // Revalidate input, in case of trickery.
  var input = canAdd(form);
  var output = {
    reset: input.reset,
    request: input,
    log: "",
    report: ""
  };
  if(input.canDo === true && !input.isNameChange)
  {
    var now = new Date().getTime(), resp = [], dbSheet = SpreadsheetApp.getActive().getSheetByName("SheetDb");
    var newRank = dbSheet.getLastRow();
    
    var memCsv = [[input.request.name, input.request.uid]];
    var uUpload = Utilities.newBlob(array2CSV_(memCsv), "application/octet-stream");
    
    var crownCsv = [[input.request.name, input.request.uid, now - 5000000000, now, new Date().getTime(), 0, 0, 0, 0, newRank, "Weasel", now]];
    var cUpload = Utilities.newBlob(array2CSV_(crownCsv), "application/octet-stream");
    while (crownCsv.length) { dbSheet.appendRow(crownCsv.pop()); }
    
    resp[0] = FusionTables.Table.importRows(utbl, uUpload).numRowsReceived;
    resp[1] = FusionTables.Table.importRows(ftid, cUpload).numRowsReceived;
    
    output.report = "Added '" + input.request.name + "' to the database.";
    output.reset = true;
  }
  else
    output.log = (input.canDo === false) ? "Cannot perform request." : "Invalid request. Revalidation is required.";
  
  console.log({message: "Adding member", output: output, can_Add: input, fn_Arg: form});
  return output;
}


function delMemberFromFusion(form)
{
  // Revalidate input, in case of trickery.
  var input = canDelete(form);
  var output = {
    reset: input.reset,
    request: input,
    log: "",
    report: ""
  };
  if (input.canDo === true && input.rowid !== undefined && input.rowid !== null)
  {
    var resp = [];
    try{
      ["DELETE FROM " + utbl + " WHERE ROWID = '" + input.rowid + "'",
       "DELETE FROM " + ftid + " WHERE UID = '" + input.request.uid + "'"].forEach(function (query, i) {
         resp[i] = FusionTables.Query.sql(query);
       });
      output.reset = true;
    }
    catch (e) {console.log({message: "Error while deleting member.", error: e, input: input}); throw e;};
    
    var r = {message:"Deleted user '" + input.request.name + "' from Member and Crown tables.", input: input, responses: resp};
    console.log(r);
    output.report = r.message;
  }
  else
    output.log = (input.canDo === false) ? "Cannot perform request." : "Invalid request. Revalidation is required.";
    
  return output;
}
