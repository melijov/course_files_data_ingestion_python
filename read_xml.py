''' Load rides data from XML '''
import bz2
import xml.etree.ElementTree as xml
import pandas as pd

def iter_rides(file_name, conversion):
  #with bz2.open(file_name, 'rt') as fp:
  #  tree = xml.parse(fp)
  tree = xml.parse(file_name)    
  root = tree.getroot()
  for elem in root:
    record = {}
    for tag, func in conversion:
      text = elem.find(tag).text
      record[tag] = func(text)
    yield record

def load_xml(file_name,conversion):
  records = iter_rides(file_name,conversion)
  return pd.DataFrame.from_records(records)

def unit_test():
  filename = 'taxi.xml'
  # Data conversions
  conversion = [
    ('vendor', int),
    ('people', int),
    ('tip', float),
    ('price', float),
    ('pickup', pd.to_datetime),
    ('dropoff', pd.to_datetime),
    ('distance',float),
  ]
  df = load_xml(filename, conversion)
  print(df.dtypes)
  print(df.head())

if __name__ == '__main__': unit_test()
