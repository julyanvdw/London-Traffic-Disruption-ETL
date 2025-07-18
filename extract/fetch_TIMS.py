"""
Julyan van der Westhuizen
16/07/25

A script for accessing an exposed API from the TRANSPORT FOR LONDON (TFL) server. 
https://tfl.gov.uk/info-for/open-data-users/our-open-data

Details on HTTP request structure for this API can be found at: https://api-portal.tfl.gov.uk/api-details#api=Road&operation=Road_DisruptionByPathIdsQueryStripContentQuerySeveritiesQueryCategoriesQuery

This script accesses the API with the use of my API keys (as per registration with TFL). It works as follows: 
1) Accesses the API with the relevant API key and API ID (as per registration with TFL)
2) Fetches the data
3) Saves the data as a JSON snapshot in a 'datalake' (via datalake manager) which stores all raw info
"""

import requests
from datalake_manager import LakeManager
from pipeline_log_manager import shared_logger

API_ID = "julyan-tims-pipeline"
API_KEY = "cdfd168c1c934e259e8cbeafd3d00cdc"
API_ENDPOINT = "https://api.tfl.gov.uk/Road/All/Disruption"

query_params = {
        "app_id": API_ID, 
        "app_key":API_KEY
    }   

def fetch_tims_data():
    try: 
        shared_logger.log("Attempting to fetch data from TIMS API...")
        response = requests.get(API_ENDPOINT, params=query_params) #note: this strips off the meta-data header and leaves us with a list of disruptions
        response.raise_for_status()
        shared_logger.log(f"API Response Status Code: {response.status_code}")

        # Ensure the server returns data in JSON format
        try:
            data = response.json()
            manager = LakeManager()
            manager.write_TIMS_raw_snapshot(data)
            shared_logger.log("Successfully wrote raw snapshot")

        except Exception as e:
            shared_logger.log_warning(f"TIMS Data could not be obtained in JSON format: {e}")

    except requests.RequestException as e: 
        shared_logger.log_warning(f"TIMS Data Request Failed: {e}")

if __name__ == "__main__": 
    fetch_tims_data()

