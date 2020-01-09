function getSidebar() {
  SpreadsheetApp.getUi().showSidebar(doSideBar_());
}

function doSideBar_() {
  var filename = "adminPanel/UI.html";
  var sb = HtmlService.createHtmlOutputFromFile(filename);
  sb.setTitle("MHCC Admin Panel").setFaviconUrl("https://i.imgur.com/QMghA1l.png");
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
 * @property {string} uid An identifier appropriate for use in the MHCC Members table to refer to this individual.
 * @property {Error} [error] Any error which occurred during the validation process.
 */

/**
 * @typedef {Object} MemberQueryResult
 * @property {string} name The member's name, as found in the MHCC Members table.
 * @property {string} uid The member's MHCC identifier.
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
 * @property {string} [uid] The validated MHCC identifier for the individual.
 * @property {string} [currentName] The existing display name for the MHCC member with the given identifier
 * @property {number} [dataRows] The number of rows of data this member has in the Crowns DB and Rank DB tables.
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
 * Query the MHCC Members table to acquire this member's information.
 * @param {string} uid The MHCC identifier for this particular individual.
 * @returns {MemberQueryResult} If found, the individual's known name and table row. Otherwise, an error.
 */
function getMemberInfo(uid)
{
  // Get the member information from the user table.
  const allMembers = bq_getMemberBatch_();
  const requestedMember = allMembers.filter(function (member) { return member[1] === uid; })[0];
  return (requestedMember
    ? { name: requestedMember[0].toString(), uid: uid }
    : { name: '', uid: uid, error: Error("No member found with uid='" + uid + "'") }
  );
}

/**
 * Query the MHCC Members table to determine if this member can be added.
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
 * Inspect BigQuery to determine if this member can be deleted.
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
        // Get the count of rows in the Crown and Rank tables.
        const crownTable = [dataProject, 'Core', 'Crowns'].join('.');
        const rankTable = [dataProject, 'Core', 'Ranks'].join('.');
        const counts = [
          "SELECT COUNT(UID) FROM `" + crownTable + "` WHERE UID = '" + input.uid + "'",
          "SELECT COUNT(UID) FROM `" + rankTable + "` WHERE UID = '" + input.uid + "'"
        ].map(function (query) { return bq_querySync_(query).rows; });
        console.log({ counts: counts });
        try { output.dataRows = counts.reduce(function (acc, val) { return acc + (val && val.length) ? parseInt(val[0][0], 10) : 0; }, 0); }
        catch (e) { console.warn(e); output.dataRows = 0; }

        output.log = "Deleting '" + member.name + "' will also delete their " + (output.dataRows ? output.dataRows + " " : "") + "stored records.";
      }
    }
  }
  else if (input.error)
    output.log = input.error.message;

  return output;
}

/**
 * Method called by the sidebar after an "Add Member" operation validates as a name change, and the administrator OKs it.
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
  console.log({ "message": "Name Change", "isNameChange": input.isNameChange, "can_do": input.canDo, "misc": input });
  if (input.canDo === true && input.isNameChange === true && input.request.uid)
  {
    const utbl = [dataProject, 'Core', 'Members'].join('.');
    const sql = "UPDATE `" + utbl + "` SET Member = '" + input.request.name + "' WHERE UID = '" + input.request.uid + "'";
    try { Bigquery.Jobs.query({ query: sql, useLegacySql: false }, dataProject); } // DML requires billing.
    catch (e) {throw e;}
    output.report = "Name for uid='" + input.request.uid + "' is now '" + input.request.name + "'";
    output.reset = true;
  }
  else
    output.log = (input.canDo === false) ? "Cannot perform request." : "Invalid request. Revalidation is required.";

  return output;
}

/**
 * Add the given individual to the MHCC Members Table.
 * @param {SidebarForm} form The current sidebar form data.
 * @returns {OperationReport} An object instructing the sidebar how to react.
 */
function addMemberToTable(form)
{
  // Revalidate input.
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
    const job = _insertTableData_([[input.request.name, input.request.uid]], {
      projectId: dataProject,
      datasetId: 'Core',
      tableId: 'Members'
    });
    console.info({ message: 'Added MHCC member ' + input.request.name, memberAddJob: job });
    output.report = "Added '" + input.request.name + "' to the database.";
    output.reset = true;
  }
  else
    output.log = (input.canDo === false) ? "Cannot perform request." : "Invalid request. Revalidation is required.";

  console.log({ "message": "Adding member", "output": output, "can_Add": input, "fn_Arg": form });
  return output;
}

/**
 * Delete the given individual from the MHCC Members, MHCC Crowns DB, and MHCC Rank DB tables.
 * @param {SidebarForm} form The current sidebar form data.
 * @returns {OperationReport} An object instructing the sidebar how to react.
 */
function delMemberFromTable(form)
{
  // Revalidate input.
  const input = canDelete(form);
  /** @type {OperationReport} */
  const output = {
    "reset": input.reset,
    "request": input,
    "log": "",
    "report": ""
  };
  if (input.canDo === true && input.request.uid)
  {
    const utbl = [dataProject, 'Core', 'Members'].join('.');
    const ftid = [dataProject, 'Core', 'Crowns'].join('.');
    const rankTableId = [dataProject, 'Core', 'Ranks'].join('.');
    const resp = [];
    try
    {
      [
        "DELETE FROM `" + utbl + "` WHERE UID = '" + input.request.uid + "'",
        "DELETE FROM `" + ftid + "` WHERE UID = '" + input.request.uid + "'",
        "DELETE FROM `" + rankTableId + "` WHERE UID = '" + input.request.uid + "'"
      ].forEach(function (query) { resp.push(Bigquery.Jobs.query({ query: query, useLegacySql: false }, dataProject)); });
      output.reset = true;
    }
    catch (e) { console.warn({ "message": "Error while deleting member.", "error": e, "input": input }); throw e; }

    const r = { "message": "Deleted user '" + input.request.name + "' from the Member, Rank DB, and Crown DB tables.", "input": input, "responses": resp };
    console.info(r);
    output.report = r.message;
  }
  else
    output.log = (input.canDo === false) ? "Cannot perform request." : "Invalid request. Revalidation is required.";

  return output;
}
