"""Deprecated code.

Deprecation came because of any number of reasons, but it came nevertheless.
"""
def identify_desirable_records(uids: list, tableId: str) -> list:
    '''
    Returns a list of rowids for which the LastSeen values are unique, or the rank is unique
    (for a given LastSeen), for the given members. Uses SQLGet since only GET is done.

    @params:
        uids: list
            Member identifiers (for whom records should be retrieved).

        tableId: str
            The ID of the FusionTable with records to retrieve.

    @return: list
            The ROWIDs of records which should be retrieved.
    '''
    rowids = []
    if not tableId or not isinstance(tableId, str) or len(tableId) != 41:
        raise ValueError('Invalid table ID received.')
    if not uids:
        raise ValueError('Invalid UIDs received.')

    uids.reverse()
    num_removable_rows = 0
    member_count = len(uids)
    # Index the column headers to find the indices of interest.
    ROWID_INDEX = 0
    UID_INDEX = 2
    LASTSEEN_INDEX = 3
    RANK_INDEX = 4

    sql_prefix = 'SELECT ROWID, Member, UID, LastSeen, Rank FROM ' + tableId + ' WHERE UID IN ('
    print('Sifting through %i member\'s stored data.' % member_count)
    ppb(member_count - len(uids), member_count, "Members sifted: ", "", 1, 50)
    while uids:
        sql_suffix = ''
        query_uids = []
        query_batch_start = time.monotonic()
        while uids and (len(sql_prefix + sql_suffix) <= 8000):
            query_uids.append(uids.pop())
            sql_suffix = ','.join(query_uids) + ') ORDER BY LastSeen ASC'
        resp = get_query_result(''.join([sql_prefix, sql_suffix]), 0.1)
        try:
            query_record_count = len(resp['rows'])
        except KeyError:
            print()
            print('No data in received response', resp)
            return []
        # Each rowid should only occur once (i.e.  a list should be
        # sufficient), but use a Set container just to be sure.
        kept = set()
        seen = {}
        for row in resp['rows']:
            if is_interesting_record(row, UID_INDEX, LASTSEEN_INDEX, RANK_INDEX, seen):
                kept.add(row[ROWID_INDEX])

        # Store these interesting rows for later retrieval.
        rowids.extend(kept)
        num_removable_rows += query_record_count - len(kept)
        elapsed = time.monotonic() - query_batch_start
        if elapsed < .75:
            time.sleep(.75 - elapsed)
        print_progress_bar(member_count - len(uids), member_count, "Members sifted: ", "", 1, 50)

    print('Found %i values to trim from %i records'
          % num_removable_rows, num_removable_rows + len(rowids))
    return rowids



def is_interesting_record(record: list, uidIndex: int, lsIndex: int, rankIndex: int, tracker: dict) -> bool:
    '''
    Identifies which rows of the input are "interesting" and should be kept. Modifies the passed
    "tracker" dictionary to support repeated calls (i.e. while querying to get the input).

    @params:
        record: list
            An individual crown record (or subset) (list)

        uidIndex: int
            The column index for the UID (unsigned)

        lsIndex: int
            The column index for the LastSeen datestamp (unsigned)

        rankIndex: int
            The column index for the Rank value (unsigned)

        tracker: dict
            An object that tracks members, seen dates, and ranks.

    @return: bool
            Whether or not the input record should be uploaded.
    '''
    if not record:
        return False
    if len({uidIndex, lsIndex, rankIndex}) < 3:
        raise ValueError('Different properties given same column index.')

    try:
        uid = record[uidIndex].__str__()
        ls = int(record[lsIndex]).__str__()
        rank = record[rankIndex].__str__()
    except IndexError as err:
        print('Invalid access into record:\n', record, '\n', uidIndex, lsIndex, rankIndex, '\n')
        print(err)
        return False

    # We should keep this record if:
    # 1) It belongs to an as-yet unseen member.
    if uid not in tracker:
        tracker[uid] = dict([(ls, {rank})])
    # 2) It is (the first) record for a given LastSeen datetime.
    elif ls not in tracker[uid]:
        tracker[uid][ls] = {rank}
    # 3) The member has a different rank than previously seen for that LastSeen datetime.
    elif rank not in tracker[uid][ls]:
        tracker[uid][ls].add(rank)
    # Otherwise, we don't care about it.
    else:
        return False

    return True



def validate_retrieved_records(records: list, sourceTableId: str) -> bool:
    '''
    Inspect the given records to ensure that each member with a record in source table has a record
    in the retrieved records, and that the given records satisfy the same uniqueness criteria that
    was used to obtain them from the source table.

    @params:
        records: list
            All records that are to be uploaded to the FusionTable (list of lists)

        sourceTableId: str
            The FusionTable that will be uploaded to.

    @return: bool
            Whether or not the input records are valid and can be uploaded.
    '''
    if not records or not sourceTableId or len(sourceTableId) != 41:
        return False

    # Determine how many rows exist per user in the source table.  Every member with at least 1
    # row should have at least 1 and at most as many rows in the retrieved records list.
    user_row_counts_sql = 'SELECT UID, COUNT(Gold) FROM ' + sourceTableId + ' GROUP BY UID'
    remote_row_counts = get_query_result(user_row_counts_sql, .05)
    mhcc_members = set([row[1] for row in get_user_batch()])

    # Parse the given records into a dict<str, int> object to simplify dataset comparisons.
    local_row_counts = {}
    rows_with_errors = []
    for row in records:
        if len(row) != 12:
            rows_with_errors.append(row)
        # Always count this row, even if it was erroneously too short or too long.
        # The UID is in the 2nd column.  (Because [] does not default-construct, handling
        # KeyError is necessary, as one will occur for the first record of each member.)
        try:
            local_row_counts[str(row[1])] += 1
        except KeyError:
            local_row_counts[str(row[1])] = 1
    if rows_with_errors:
        print(rows_with_errors)

    # Loop the source counts to verify the individual has data in the supplied records.
    is_valid_recordset = True
    for row in remote_row_counts['rows']:
        if int(row[1]) > 0:
            # Validation rules apply only to current members.
            if row[0] not in mhcc_members:
                continue
            # For current members with remote data, removing all their data is unallowable.
            elif row[0] not in local_row_counts:
                print('No rows in upload data for member with ID=\'%s\', expected 1 - %i'
                      % row[0], row[1])
                is_valid_recordset = False
            # The uploaded data should not have 0 rows, or more rows than the source,
            # for each member.
            elif local_row_counts[row[0]] > int(row[1]) or local_row_counts[row[0]] < 1:
                print('Improper row count in upload data for member with ID=\'%s\'' % row[0])
                is_valid_recordset = False

    # Perform the uniqueness checks on the records (all should be "interesting").
    # Index the column headers to find the indices of interest.
    UID_INDEX = 1
    LASTSEEN_INDEX = 2
    RANK_INDEX = 9
    observed_records = {}
    for row in records:
        if not is_interesting_record(row, UID_INDEX, LASTSEEN_INDEX, RANK_INDEX, observed_records):
            is_valid_recordset = False
            break

    print('Validation of new records', 'completed.' if is_valid_recordset else 'failed.')
    return is_valid_recordset
