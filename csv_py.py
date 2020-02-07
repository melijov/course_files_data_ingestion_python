'''Load and convert data from CSV file using Python built-in csv module'''

import bz2
import csv
from collections import namedtuple
from datetime import datetime

def parse_timestamp(text):
  return datetime.strptime(text, '%Y-%m-%d %H:%M:%S')

def iter_records(file_name,columns):
  with bz2.open(file_name,'rt') as fp:
    reader = csv.DictReader(fp)
    for csv_record in reader:
      record = {}
      for col in columns:
        value = csv_record[col.src]        
        record[col.dest] = col.convert(value)
      yield record

def print_file(file_name,columns):
  from pprint import pprint
  for i, record in enumerate(iter_records(file_name,columns)):
    if i >= 10:
      break
    pprint(record)
      
def unit_test():
  fn = 'taxi.csv.bz2'
  Column = namedtuple('Column', 'src dest convert')
  columns = [
  Column('VendorID', 'vendor_id', int),
  Column('passenger_count', 'num_passengers', int),
  Column('tip_amount', 'tip', float),
  Column('total_amount', 'price', float),
  Column('tpep_dropoff_datetime','dropoff_time',parse_timestamp),
  Column('tpep_pickup_datetime', 'pickup_time',parse_timestamp),
  Column('trip_distance', 'distance', float),
  ]

  print_file(fn, columns)


if __name__=='__main__': unit_test()