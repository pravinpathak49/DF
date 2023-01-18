import csv
import pandas as pd
from pathlib import Path
from  generic_functions import get_schema
from testing_functions import time_it
#read file header and check with schema registered.

BASE_DIR = Path(__file__).parent.parent

def get_source_columns(fpath):
    with open(fpath, 'r') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
    return fieldnames

@time_it
def compare_columns(source_data_file,schema_file,table_name):
    source_column = get_source_columns(source_data_file)
    config_column = get_schema(schema_file, table_name)
    if len(source_column) == len(config_column):
        if source_column == config_column:
            message = f'Source columns: {source_column} and config columns {config_column} Matched.'
    else:
        if len(config_column) > len(source_column):
            col_diff = set(config_column) - set(source_column)
            message = f"Source Data has  {col_diff} column/s missing."
        else:
            col_diff = set(source_column) - set(config_column)
            message = f"Config Data has  {col_diff}  column/s missing."
        return False, message
    return True, message


def clean_data_for_schema_mismatch(source_data_file, schema_file, table_name):
    source_column = get_source_columns(source_data_file)
    config_column = get_schema(schema_file, table_name)
    if len(source_column) < len(config_column):
        message = 'Data Cannot be cleaned!!!'
    else:
        col_diff = set(source_column) - set(config_column)
        data_frame = pd.read_csv(source_data_file, header='infer', sep = ',')
        clean_df = data_frame.drop(list(col_diff), axis=1)
        clean_df.to_csv(source_data_file[:-4]+'_clean.csv', index=False)



source_data_file ='/Users/pravin/PycharmProjects/DF/data/MOCK_DATA_updated.csv'
schema_file = f'{BASE_DIR}/config/schema.json'
table_name = "mock"

if __name__ == "__main__":
    res, message = compare_columns(source_data_file, schema_file, table_name)
    print(message)
    if not res:
        clean_data_for_schema_mismatch(source_data_file, schema_file, table_name)