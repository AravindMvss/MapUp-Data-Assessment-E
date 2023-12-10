import pandas as pd
import argparse
import requests
import os
import threading
import glob
from dotenv import load_dotenv

load_dotenv()
# Parsing Arguments
parser = argparse.ArgumentParser()
parser.add_argument('--to_process', help="path to csv folder")
parser.add_argument('--output_dir', help="path to output directory")

args = parser.parse_args()

input_file_path = args.to_process
output_file_path = args.output_dir
# Loading api key and url via .env file using python-dotval
api_key = os.getenv('TOLLGURU_API_KEY')
url = os.getenv('TOLLGURU_API_URL')

headers = {'x-api-key': api_key, 'Content-Type': 'text/csv'}

# sending api request function along with correspondingly writing to json
def send_api_request(csv_file, output_dir):

    with open(csv_file, 'rb') as file:
        response = requests.post(url, data=file, headers=headers)

    csv_file_name = csv_file.split('\\')[-1]
    json_response_file = f"{csv_file_name.replace('.csv', '.json')}"

    with open(f"{output_dir}\{json_response_file}", 'w') as json_file:
        json_file.write(response.text)

    print(f"Request for {csv_file} processed. JSON response saved to {json_response_file}")

if __name__ == "__main__":
    
    # Using threads to execute concurrently.
    csv_files = glob.glob(f"{input_file_path}\*.csv")
    threads = []
    for csv_file in csv_files:
        thread = threading.Thread(target=send_api_request, args=(csv_file,output_file_path))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print("All requests processed.")
