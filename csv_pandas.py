'''Load and converting data from CSV using Pandas'''
import pandas as pd

def load_df(file_name, time_cols):
  return pd.read_csv(file_name, parse_dates=time_cols)

def iter_df(file_name, time_cols):
  yield from pd.read_csv(file_name,parse_dates=time_cols, chunksize=100)
  
def print_file(file_name, time_cols):
  print(load_df(file_name,time_cols).head())
  for i, df in enumerate(iter_df(file_name, time_cols)):
    if i > 10:
      break
    print(len(df))  

def unit_test():
  fn = 'taxi.csv.bz2'
  tc =['tpep_dropoff_datetime', 'tpep_pickup_datetime']
  print_file(fn, tc)
  
  
if __name__ =='__main__': unit_test()
