"""Script for exporting all FusionTables to a corresponding BigQuery Table"""
from datetime import datetime
from re import sub as regex_replace
from services import FusionTableHandler

from google.cloud import bigquery

def to_safe_name(name: str) -> str:
    """Convert text to be made BQ-compatible (alphanumeric + underscores)"""
    return regex_replace(r'\-|\.|:', "", name.replace(' ', '_'))

def create_dataset(client: bigquery.Client, dataset_name: str, description: str = 'Automatic imports of known FusionTables') -> bigquery.Dataset:
    ds = bigquery.Dataset(f'{client.project}.{to_safe_name(dataset_name)}')
    ds.description = description
    return client.create_dataset(ds)

def decode_fusionTable_schema(ft: FusionTableHandler) -> dict:
    """Returns a dictionary mapping from a FusionTable ID to its corresponding BigQuery TableColumn Schema"""
    all_tables = []
    request = ft.table.list(fields="items(name,tableId,description,columns(name,columnId,description,type,formatPattern))")
    while request is not None:
        response = request.execute()
        all_tables.extend(response.get('items', []))
        request = ft.table.list_next(request, response)

    def _map_col(col: dict) -> dict:
        col_schema = {k: col.get(k, '') for k in ('name', 'columnId', 'description')}
        if col['name'] != 'Comment':
            col_schema['MODE'] = 'REQUIRED'
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

    return dict((s.get('tableId'), s) for s in map(_map_table, all_tables))

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
