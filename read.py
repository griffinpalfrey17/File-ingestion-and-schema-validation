import pandas as pd
import testutility as util
import dask.dataframe as dd
import modin.pandas as mpd
import ray
import os
import glob
import time

# Read with pandas
start_time = time.time()
df_pandas = pd.read_csv('metadata.csv')
pandas_time = time.time() - start_time
print(f"Pandas read time: {pandas_time} seconds")

# Read with dask
start_time = time.time()
df_dask = dd.read_csv('metadata.csv').compute()
dask_time = time.time() - start_time
print(f"Dask read time: {dask_time} seconds")

# Read with modin
start_time = time.time()
df_modin = pd.read_csv('metadata.csv')
modin_time = time.time() - start_time
print(f"Modin read time: {modin_time} seconds")

# Read with ray
'''
ray.init()
start_time = time.time()
@ray.remote
def read_csv(file_path):
    return pd.read_csv(file_path)

ray_time = time.time() - start_time
print(f"Ray read time: {ray_time} seconds")
'''
# Results are as follows 
'''
Pandas read time: 0.0024881362915039062 seconds
Dask read time: 0.6301560401916504 seconds
Modin read time: 0.001531839370727539 seconds
Ray read time: 0.7520239353179932 seconds
'''
# Thus it is clear that Modin has the best computational efficiency in reading the csv file. Thus we will use Modin for reading the csv file.