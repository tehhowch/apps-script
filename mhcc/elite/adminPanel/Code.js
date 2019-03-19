//@ts-check
function getSidebar() {
  SpreadsheetApp.getUi().showSidebar(doSideBar_());
}

function doSideBar_() {
  var filename = "adminPanel/UI.html";
  var sb = HtmlService.createHtmlOutputFromFile(filename);
  sb.setTitle("MHCC Elite Admin Panel").setFaviconUrl("https://i.imgur.com/QMghA1l.png");
  return sb;
}

/**
 * @typedef {Object} SidebarForm
 * @property {string} memberName The string entered in the "memberName" input div.
 * @property {string} memberLink The string entered in the "memberLink" input div.
 */

/**
 * @typedef {Object} ValidatedInput
 * @property {SidebarForm} form The form input that generated this object.
 * @property {boolean} isValid Whether or not the particular input was valid.
 * @property {string} name The validated name of the individual (whom may or may not be in MHCC).
 * @property {string} uid An identifier appropriate for use in the MHCC Elite Members table to refer to this individual.
 * @property {Error} [error] Any error which occurred during the validation process.
 */

/**
 * @typedef {Object} MemberQueryResult
 * @property {string} name The member's name, as found in the MHCC Elite Members FusionTable.
 * @property {string} rowid The rowid in the MHCC Elite Member's FusionTable that holds this member's record.
 * @property {string} uid The member's MHCC Elite identifier.
 * @property {Error} [error] Any error that occurred during a search of the Members table for this member.
 */

/**
 * @typedef {Object} OperationFeasibility
 * @property {boolean} canDo Whether or not this operation is possible.
 * @property {boolean} reset Whether the sidebar form will be cleared by sending this object.
 * @property {ValidatedInput} request The validated form input sent to the operation checker.
 * @property {string} log The log text to be displayed to the administrator (feedback).
 * @property {string} report The report text to be displayed to the administrator (feedback).
 * @property {boolean} [isNameChange] If an "add member" operation is actually a name change.
 * @property {string} [uid] The validated MHCC Elite identifier for the individual.
 * @property {string} [currentName] The existing display name for the MHCC member with the given identifier
 * @property {string} [rowid] The table rowid for the MHCC Elite member with the given identifier.
 * @property {number} [dataRows] The number of rows of data this member has in the Rank DB FusionTable.
 */

/**
 * @typedef {Object} OperationReport
 * @property {boolean} reset Whether the sidebar form will be cleared by sending this object.
 * @property {OperationFeasibility} request Analysis of whether the requested operation is possible.
 * @property {string} log The log text to be displayed to the administrator (feedback).
 * @property {string} report The report text to be displayed to the administrator (feedback).
 */

/**
 * Repeat the data validation performed on the sidebar.
 * @param {SidebarForm} form The sidebar form data.
 * @returns {ValidatedInput} Validated form input, or an error.
 */
function validateSidebarInput_(form)
{
  var validity = false, name = "", uid = "", error;
  try
  {
    name = form.memberName.trim();
    uid = form.memberLink.slice(form.memberLink.search("=") + 1).toString();
    validity = name.length > 0 && uid.length > 0;
  }
  catch (e) { error = e;}
  return { "form": form, "isValid": validity, "name": name, "uid": uid, "error": error};
}

/**
 * Query the MHCC Elite Members FusionTable to acquire this member's information.
 *
 * @param {string} uid The MHCC identifier for this particular individual.
 * @returns {MemberQueryResult} If found, the individual's known name and table row. Otherwise, an error.
 */
function getMemberInfo(uid)
{
  const query = "SELECT Member, UID, ROWID FROM " + eliteUserTable + " WHERE UID = '" + uid + "'";
  // Get the member information from the user table.
  /** @type {{kind: string, rows: [string, string, string][], columns: string[]}} */
  const resp = FusionTables.Query.sqlGet(query);
  if (resp.kind !== "fusiontables#sqlresponse" || !resp.columns || !resp.columns.length)
  {
    const e = TypeError("Invalid response received");
    e.inputData = { "sql": query, "response": resp };
    throw e;
  }
  console.log({ "message": "GetMemberInfo(" + uid + ")", "ftResponse": resp, "ftQuery": query });
  // Received a well-formed response.
  if (resp.rows && resp.rows.length > 0)
  {
    const member = resp.rows;
    // Find this specific member's row within all the rows that were returned.
    const index = member.map(function (v) { return v[1]; }).indexOf(uid);
    if (index > -1)
      return { "name": member[index][0], "uid": uid, "rowid": member[index][2] };
  }
  // No members found in the response.
  else
    return {
      "name": "", "uid": uid, "rowid": "",
      "error": Error("No member found with uid='" + uid + "'")
    };
}

/**
 * Query the MHCC Elite Member table to determine if this member can be added.
 *
 * @param {SidebarForm} form The current sidebar form data.
 * @returns {OperationFeasibility} An object instructing the sidebar how to react.
 */
function canAdd(form)
{
  const input = validateSidebarInput_(form);
  /** @type {OperationFeasibility} */
  const output = {
    "reset": false,
    "request": input,
    "canDo": false,
    "log": "",
    "report": ""
  };
  if(input.isValid)
  {
    const member = getMemberInfo(input.uid);
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
      output.isNameChange = input.name !== member.name;
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

/**
 * Query FT to determine if this member can be deleted.
 * @param {SidebarForm} form The current sidebar form data.
 * @returns {OperationFeasibility} An object instructing the sidebar how to react.
 */
function canDelete(form)
{
  const input = validateSidebarInput_(form);
  /** @type {OperationFeasibility} */
  const output = {
    "reset": false,
    "request": input,
    "canDo": false,
    "log": "",
    "report": ""
  };
  if(input.isValid)
  {
    const member = getMemberInfo(input.uid);
    if (member.error)
    {
      // No member was found with this uid.
      output.canDo = false;
      output.log = member.error.message;
    }
    else
    {
      // Deletes require that the name is matched exactly.
      output.canDo = input.name === member.name;
      if (!output.canDo)
        output.log = "Member exists with name '" + member.name + "'. Names must match.";
      else
      {
        output.rowid = member.rowid;
        // Get the count of rows in the Crown and Rank tables.
        /** @type {{kind: string, rows: string[][]}[]} */
        const resp = [
          "SELECT COUNT(UID) FROM " + eliteRankTable + " WHERE UID = '" + input.uid + "'"
        ].map(function (query) { return FusionTables.Query.sqlGet(query); });
        try { output.dataRows = resp.reduce(function (acc, val) { return acc + ((val.rows && val.rows.length) ? val.rows[0][0] * 1 : 0); }, 0); }
        catch (e)
        {
          console.warn(e);
          output.dataRows = 0;
        }

        output.log = "Deleting '" + member.name + "' will also delete their " + (output.dataRows ? output.dataRows + " " : "") + "crown records.";
      }
    }
  }
  else if (input.error)
    output.log = input.error.message;

  return output;
}

/**
 * Method called by the sidebar after an "Add Member" operation validates as a name change, and the administrator OKs it.
 *
 * @param {SidebarForm} form The sidebar form data.
 * @returns {OperationReport} An object instructing the sidebar how to react.
 */
function changeMemberName(form)
{
  // Revalidate input, in case of trickery.
  const input = canAdd(form);
  /** @type {OperationReport} */
  const output = {
    "reset": input.reset,
    "request": input,
    "log": "",
    "report": ""
  };
  console.log({ "message": "Name Change", "isNameChange": input.isNameChange, "can_do": input.canDo, "row": input.rowid, "misc": input });
  if (input.canDo === true && input.isNameChange === true && input.rowid)
  {
    const sql = "UPDATE " + eliteUserTable + " SET Member = '" + input.request.name + "' WHERE ROWID = '" + input.rowid + "'";
    try { FusionTables.Query.sql(sql); }
    catch (e) {throw e;}
    output.report = "Name for uid='" + input.request.uid + "' is now '" + input.request.name + "'";
    output.reset = true;
  }
  else
    output.log = (input.canDo === false) ? "Cannot perform request." : "Invalid request. Revalidation is required.";

  return output;
}

/**
 * Add the given individual to the MHCC Members FusionTable.
 *
 * @param {SidebarForm} form The current sidebar form data.
 * @returns {OperationReport} An object instructing the sidebar how to react.
 */
function addMemberToFusion(form)
{
  // Revalidate input, in case of trickery.
  const input = canAdd(form);
  /** @type {OperationReport} */
  const output = {
    "reset": input.reset,
    "request": input,
    "log": "",
    "report": ""
  };
  if (input.canDo === true && !input.isNameChange)
  {
    const memCsv = [[input.request.name, input.request.uid]];
    const uUpload = Utilities.newBlob(array2CSV_(memCsv), "application/octet-stream");
    const resp = FusionTables.Table.importRows(eliteUserTable, uUpload).numRowsReceived;

    output.report = "Added '" + input.request.name + "' to the database.";
    output.reset = true;
  }
  else
    output.log = (input.canDo === false) ? "Cannot perform request." : "Invalid request. Revalidation is required.";

  console.log({ "message": "Adding member", "output": output, "can_Add": input, "fn_Arg": form });
  return output;
}

/**
 * Delete the given individual from the MHCC Members, MHCC Crowns DB, and MHCC Rank DB FusionTables.
 *
 * @param {SidebarForm} form The current sidebar form data.
 * @returns {OperationReport} An object instructing the sidebar how to react.
 */
function delMemberFromFusion(form)
{
  // Revalidate input, in case of trickery.
  const input = canDelete(form);
  /** @type {OperationReport} */
  const output = {
    "reset": input.reset,
    "request": input,
    "log": "",
    "report": ""
  };
  if (input.canDo === true && input.rowid)
  {
    const resp = [];
    try
    {
      [
        "DELETE FROM " + eliteUserTable + " WHERE ROWID = '" + input.rowid + "'",
        "DELETE FROM " + eliteRankTable + " WHERE UID = '" + input.request.uid + "'"
      ].forEach(function (query) { resp.push(FusionTables.Query.sql(query)); });
      output.reset = true;
    }
    catch (e)
    {
      console.warn({ "message": "Error while deleting member.", "error": e, "input": input });
      throw e;
    }

    const r = { "message": "Deleted user '" + input.request.name + "' from the Member, Rank DB, and Crown DB FusionTables.", "input": input, "responses": resp };
    console.log(r);
    output.report = r.message;
  }
  else
    output.log = (input.canDo === false) ? "Cannot perform request." : "Invalid request. Revalidation is required.";

  return output;
}
