"""Script for exporting all FusionTables to a corresponding BigQuery Table"""
import csv
from datetime import datetime
from re import sub as regex_replace
from services import FusionTableHandler
from regression_fixer import get_as_int

from google.cloud import bigquery

def to_safe_name(name: str) -> str:
    """Convert text to be made BQ-compatible (alphanumeric + underscores)"""
    return regex_replace(r'\-|\.|:', "", name.replace(' ', '_'))

def create_dataset(client: bigquery.Client, dataset_name: str, description: str = 'Automatic imports of known FusionTables') -> bigquery.Dataset:
    ds = bigquery.Dataset(f'{client.project}.{to_safe_name(dataset_name)}')
    ds.description = description
    return client.create_dataset(ds)

def decode_fusionTable_schema(tables: list) -> dict:
    """Returns a dictionary mapping from a FusionTable ID to its corresponding BigQuery TableColumn Schema
    @params
        `tables`: list['dict']
            A list of dicts, each of which contains the basic identity and schema metadata associated with a FusionTable
    @returns
        { [fusiontableId]: list[BigQuery.SchemaField] }
    """
    def _map_col(col: dict) -> dict:
        col_schema = {k: col.get(k, '') for k in ('name', 'columnId', 'description')}
        if col['name'] != 'Comment':
            col_schema['mode'] = 'REQUIRED'
        # We can actually use int/float now!
        col_type: str = col['type']
        if col_type == 'NUMBER':
            col_type = 'INT64' if col['formatPattern'] == 'NUMBER_INTEGER' else 'FLOAT64'
        col_schema['type'] = col_type
        return col_schema

    def _map_table(table: dict) -> dict:
        table_schema = {k: table.get(k, '') for k in ('name', 'tableId', 'description')}
        table_schema['columns'] = list(map(_map_col, table['columns']))
        return table_schema

    return dict((s.get('tableId'), s) for s in map(_map_table, tables))

def create_tables(client: bigquery.Client, tableSchemas: dict) -> dict:
    """Create empty BigQuery tables for the given partial Table schemas.

    Returns a dict of `{ftId : bqId}` to the caller
    """
    ds = create_dataset(client, f'FusionTable_Autoimport_{datetime.now()}')

    def _create_field_schema(col_schema: dict) -> bigquery.SchemaField:
        """Create a SchemaField from the dict"""
        name = to_safe_name(col_schema['name'])
        return bigquery.SchemaField(
            name,
            col_schema.get('type'),
            col_schema.get('mode', 'NULLABLE'),
            col_schema.get('description', '')
        )

    def _table_from_ft(ft_schema: dict) -> bigquery.Table:
        """Create a local representation of a BigQuery table"""
        # A "TableSchema" is just a sequence of SchemaFields https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.Table.html
        schema = list(map(_create_field_schema, ft_schema['columns']))
        table = bigquery.Table(
            bigquery.TableReference(ds, to_safe_name(ft_schema['name'])),
            schema
        )
        table.description = ft_schema.get('description', '')
        return table

    return {
        ftId: client.create_table(_table_from_ft(ftSchema))
            for (ftId, ftSchema) in tableSchemas.items()
    }

def upload_table_data(client: bigquery.Client, tableRef: bigquery.Table, fusionFile: str) -> bigquery.LoadJob:
    """Given the client, BigQuery table target, and data, upload the data"""
    with open(fusionFile, mode='rb') as file:
        job = client.load_table_from_file(file, tableRef)
    return job

def download_table_data(ft: FusionTableHandler, tableId: str, table: bigquery.Table) -> list:
    """Download the data from the given FusionTable and process it to match the given schema"""
    data: dict = ft.get_query_result(f'select * from {tableId}')
    if 'rows' in data:
        transform_table_data(data['rows'], table)
        return data['rows']

def transform_table_data(tableRows: list, table: bigquery.Table):
    """Convert floats to ints where required prior to uploading. Convert NaN to 0 for numeric types"""
    colSchema: list = table.schema
    assert len(tableRows[0]) <= len(colSchema), f'table should have at most as many columns as its schema: {len(tableRows[0])} ! <= {len(colSchema)}'
    formatter = []
    for schemaField in colSchema:
        fn = None
        if schemaField.field_type in ('INT64', 'INTEGER'):
            fn = get_as_int
        elif schemaField.field_type == ('FLOAT64', 'FLOAT'):
            fn = float
        elif schemaField.field_type != 'STRING': print(schemaField.field_type)
        formatter.append(fn)

    for row in tableRows:
        for (idx, val) in enumerate(row):
            fn = formatter[idx]
            if fn is not None:
                result = fn(val)
                row[idx] = result if result is not None else 0
    return

def write_table_data(tableId: str, tableRows: list):
    """Write the given data to local disk in prep for uploading"""
    filename = f'table_{tableId}.csv'
    with open(filename, 'w', newline='', encoding='utf-8') as f_:
        csv.writer(f_, quoting=csv.QUOTE_NONNUMERIC).writerows(tableRows)
    return filename

def export(ft: FusionTableHandler, client: bigquery.Client, allTables=True, tableIds: list = None):
    """Exports either all known FusionTables, or the given FusionTable IDs, to BigQuery"""
    schemas = dict()
    if allTables:
        all_tables = []
        request = ft.table.list(fields="items(name,tableId,description,columns(name,columnId,description,type,formatPattern))")
        while request is not None:
            response = request.execute()
            all_tables.extend(response.get('items', []))
            request = ft.table.list_next(request, response)

        schemas.update(decode_fusionTable_schema(all_tables))
    elif not tableIds:
        return
    else:
        raise NotImplementedError()

    jobs = []
    for (tableId, tableRef) in create_tables(client, schemas).items():
        rows = download_table_data(ft, tableId, tableRef)
        job: bigquery.LoadJob = upload_table_data(client, tableRef, write_table_data(tableId, rows))
        job.add_done_callback(lambda job, ftId=tableId: print(f'Load job {"finished" if not job.error_result else "failed"} for FT {ftId}'))
        jobs.append(job)

    while True:
        if all(job.running() == False for job in jobs):
            print('Done exporting')
            break
        elif any(job.error_result for job in jobs):
            for job in jobs:
                if job.running():
                    job.cancel()

