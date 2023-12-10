import pandas as pd
import argparse
import json
import glob

# Parsing Arguments
parser = argparse.ArgumentParser()
parser.add_argument('--to_process', help="Path to the JSON responses folder")
parser.add_argument('--output_dir', help="path to output directory")

args = parser.parse_args()

input_file_path = args.to_process
output_file_path = args.output_dir

# Required information to extract from the json response
columns=['unit', 'trip_id', 'toll_loc_id_start', 'toll_loc_id_end',
       'toll_loc_name_start', 'toll_loc_name_end', 'toll_system_type',
       'entry_time', 'exit_time', 'tag_cost', 'cash_cost',
       'license_plate_cost']

transformed_data = []

for each_file in glob.glob(f"{input_file_path}\*.json"):
    unit = each_file.split("\\")[-1]
    unit_name = unit.split("_")[0]
    unit_trip = unit.split(".json")[0]
    
    with open(each_file, 'r') as json_file:
        data = json.load(json_file)
    
    for each_trip in data['route']['tolls']:
        toll_loc_id_start = each_trip['start']['id']
        toll_loc_id_end = each_trip['end']['id']
        toll_loc_name_start = each_trip['start']['name']
        toll_loc_name_end = each_trip['end']['name']
        toll_system_type = each_trip['type']
        entry_time = each_trip['start']['arrival']['time']
        exit_time = each_trip['end']['arrival']['time']
        tag_cost = each_trip['tagCost']
        cash_cost = each_trip['cashCost']
        license_plate_cost = each_trip['licensePlateCost']
        transformed_data.append([unit_name,unit_trip,toll_loc_id_start,toll_loc_id_end,toll_loc_name_start,toll_loc_name_end,toll_system_type,
               entry_time,exit_time,tag_cost,cash_cost,license_plate_cost])

# Final transformed data.
output = pd.DataFrame(transformed_data,columns=columns)
output.to_csv(f'{output_file_path}/transformed_data.csv',index=False)