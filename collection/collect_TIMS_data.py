"""
Julyan van der Westhuizen
16/07/25

A script for accessing an exposed API from the TRANSPORT FOR LONDON (TFL) server. 
https://tfl.gov.uk/info-for/open-data-users/our-open-data

Details on HTTP request structure for this API can be foudn at: https://api-portal.tfl.gov.uk/api-details#api=Road&operation=Road_DisruptionByPathIdsQueryStripContentQuerySeveritiesQueryCategoriesQuery

This script accesses the API with the use of my API keys (as per registration with TFL). It works as follows: 
1) Accesses the API with the relevant API key and API ID (as per registration with TFL)
2) Fetches the data
3) Saves the data as a JSON snapshot in a directory which stores all raw info
"""

import requests
import json
from datetime import datetime
import os

API_ID = "julyan-tims-pipeline"
API_KEY = "cdfd168c1c934e259e8cbeafd3d00cdc"
API_ENDPOINT = "https://api.tfl.gov.uk/Road/All/Disruption"

query_params = {
        "app_id": API_ID, 
        "app_key":API_KEY
    }   

try: 
    print("Attempting to fetch data...")
    response = requests.get(API_ENDPOINT, params=query_params) #note: this strips off the meta-data header and leaves us with a list of disruptions
    response.raise_for_status() 

    # make sure that the server returns JSON (according to server spec)
    try:
        data = response.json()

        timestamp = datetime.now()
        timestamp = timestamp.strftime("%Y-%m-%d-%H-%M-%S")
        filename = f"TIMS-snapshot-{timestamp}"

        # Make a dir to store snapshots if there is none already
        os.makedirs("../data/raw", exist_ok=True)

        # Write the data to a file
        with open(f"../data/raw/{filename}", "w") as f:
            json.dump(data, f)
        
        print(f"Snapshot length: {len(data)} current incidents")
        print(f"Data saved to {filename} in /data/raw")

    except Exception as e:
        print("Data is not in JSON format", e)

except requests.RequestException as e:
    print("The Request Failed", e)