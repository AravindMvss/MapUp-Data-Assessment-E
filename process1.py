import pandas as pd
import argparse

# Parsing Arguments
parser = argparse.ArgumentParser()
parser.add_argument('--to_process', help="path to input file")
parser.add_argument('--output_dir', help="path to output directory")

args = parser.parse_args()

input_file_path = args.to_process
output_file_path = args.output_dir

# Reading parquet file for trips analysis
df = pd.read_parquet(input_file_path)
df['timestamp'] = pd.to_datetime(df['timestamp'])

#Trips dataframe
trip_df = pd.DataFrame() 

for unit, unit_df in df.groupby('unit'):
    unit_df['time_difference_in_hr'] = unit_df['timestamp'].diff().dt.total_seconds()/3600
    total_trips = unit_df[unit_df['time_difference_in_hr']>7].reset_index()
    for idx,row in total_trips.iterrows():
        unit_df.loc[row['index']-1,'trip'] = idx 
    unit_df['trip'] = unit_df.trip.bfill().fillna(total_trips.shape[0])
    trip_df = pd.concat([trip_df,unit_df])

trip_df['unit_trip'] = trip_df['unit'].astype(str) + "_" +trip_df['trip'].astype(int).astype(str)
trip_df['timestamp'] = trip_df['timestamp'].apply(lambda x:x.strftime("%Y-%m-%dT%H:%M:%SZ"))

# Writing Unit specific data and its trips as csv.
for vehicle_trip, df in trip_df.groupby('unit_trip'):
    df[['latitude','longitude','timestamp']].to_csv(f'{output_file_path}/{vehicle_trip}.csv',index=False)