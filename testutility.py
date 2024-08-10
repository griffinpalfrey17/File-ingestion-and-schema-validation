import logging
import testutility
import yaml
import subprocess
import pandas as pd
import datetime 
import gc
import re
import os
import modin.pandas as pd

# Next let's do basic data validation and cleaning using testutility code 
def read_config_file(filepath):
  with open(filepath, 'r') as stream:
    try:
      return yaml.safe_load(stream)
    except yaml.YAMLError as exc:
      logging.error(exc)

def replacer(string, char):
  pattern = char + '{,}'
  string = re.sub(pattern, char, string)
  return string

def col_header_val(df,table_config):
    '''
    replace whitespaces in the column
    and standardized column names
    '''
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('[^\w]','_',regex=True)
    df.columns = list(map(lambda x: x.strip('_'), list(df.columns)))
    df.columns = list(map(lambda x: replacer(x,'_'), list(df.columns)))
    expected_col = list(map(lambda x: x.lower(),  table_config['columns']))
    expected_col.sort()
    df.columns =list(map(lambda x: x.lower(), list(df.columns)))
    df = df.reindex(sorted(df.columns), axis=1)
    if len(df.columns) == len(expected_col) and list(expected_col)  == list(df.columns):
        print("column name and column length validation passed")
        return 1
    else:
        print("column name and column length validation failed")
        mismatched_columns_file = list(set(df.columns).difference(expected_col))
        print("Following File columns are not in the YAML file",mismatched_columns_file)
        missing_YAML_file = list(set(expected_col).difference(df.columns))
        print("Following YAML columns are not in the file uploaded",missing_YAML_file)
        logging.info(f'df columns: {df.columns}')
        logging.info(f'expected columns: {expected_col}')
        return 0

# Step 1: Read the data using Modin, since it reads the fastest
df_modin = pd.read_csv('metadata.csv')

# Step 2: Load the YAML schema

import modin.pandas as pd
import yaml

# Load YAML schema
with open('file.yaml', 'r') as file:
    schema = yaml.safe_load(file)

expected_columns = schema['columns']

# Base directory and output file path
input_file_path = 'metadata.csv'
output_file_path = 'reduced_file.csv.gz'

# Parameters
chunk_size = 100000  # Number of rows per chunk to process
sample_fraction = 0.1  # Fraction of rows to keep from each chunk

# Initialize an empty list to store the reduced data
reduced_dataframes = []

# Process the file in chunks
for chunk in pd.read_csv(input_file_path, chunksize=chunk_size):
    
    # Sample the rows in the chunk
    reduced_chunk = chunk.sample(frac=sample_fraction)
    
    # Validate the schema (optional, depending on your needs)
    df_columns_sorted = sorted(reduced_chunk.columns.str.lower())
    expected_columns_sorted = sorted([col.lower() for col in expected_columns])

    if df_columns_sorted != expected_columns_sorted:
        print("Column names do not match the schema.")
        print(f"Expected columns: {expected_columns_sorted}")
        print(f"Found columns: {df_columns_sorted}")
        raise ValueError("Column names mismatch.")
    
    # Append the reduced chunk to the list
    reduced_dataframes.append(reduced_chunk)

# Concatenate all reduced chunks
final_reduced_df = pd.concat(reduced_dataframes, axis=0)

# Write the reduced dataset to a new file
final_reduced_df.to_csv(output_file_path, sep='|', index=False, compression='gzip')

# Summary
print(f"Reduced file saved to: {output_file_path}")
print(f"Total number of rows: {len(final_reduced_df)}")
print(f"Total number of columns: {len(final_reduced_df.columns)}")
print(f"File size: {os.path.getsize(output_file_path) / 1e6:.2f} MB") 