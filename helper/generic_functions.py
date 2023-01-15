import json
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent


def get_schema(config_file, tablename):
    """Returns the configured list of columns in sorted manner as per column number"""
    with open(config_file, "r") as f:
        schemas = json.load(f)
    table_schema = sorted(schemas[tablename], key=lambda col: col['column_number'])
    columns = [col['column_name'] for col in table_schema]
    return columns


def create_table_sql(db_name,tablename, schema_file, location, table_type = '', delimeter =',', fileformat='TEXTFILE' ):
    """Returns Hive Create table statement after passing details
    (tablename, schema_file, location, delimeter =',', fileformat='TEXTFILE' )"""
    with open(schema_file, "r") as f:
        schemas = json.load(f)
    columns = []
    for column in schemas[tablename]:
        columns.append({"column_name": column["column_name"], "column_type": column["column_type"]})
    sql_stmt = f"CREATE {table_type} TABLE {db_name}.{tablename} ("
    for column in columns:
        sql_stmt += column['column_name'] + " " + column['column_type'] + ", "
    sql_stmt = sql_stmt[:-2] + ")"
    sql_stmt = sql_stmt + f" ROW FORMAT DELIMITED FIELDS TERMINATED BY '{delimeter}' STORED AS {fileformat} LOCATION '{location}' ;"
    return sql_stmt

