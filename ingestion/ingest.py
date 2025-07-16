"""
Julyan van der Westhuizen
16/07/25

This script performs the ingestion of raw, collected data files: 
1) Each snapshot contians 0..n incidents, so we'll split those up into seperate objects
2) We'll clean out all of the stuff that's not relevant
3) Validate what we do have

"""

import os
import json
from tims_models import Disruption


# INGEST MODULS

def ingest_tims_data():
    
    # 0. Storage setup
    raw_location = "../data/raw"
    processed_location = '../data/processed'
    os.makedirs(processed_location, exist_ok=True)

    processed_data = []

    # 1. Batch process each snapshot from the raw location
    for filename in os.listdir(raw_location):
        filepath = os.path.join(raw_location, filename)

        with open(filepath, "r") as f:
            data = json.load(f)
            
            test = data[1]
            # print(json.dumps(test, indent=2))
            try:
                d = Disruption(**test)
                print(d.model_dump_json(indent=2))
            except Exception as e:
                print(f"Couldn't parse into a pydantic object {e}")


    #         print(json.dumps(data[-1], indent=2))

    #         # For each data item (disruption), handle each disruption as a individual record 
    #         for d in data:
    #             try:
    #                 disruption = Disruption(**d)
    #                 processed_data.append(disruption)
    #             except Exception as e:
    #                 print(f"Couldn't parse into a pydantic object {e}")

    # # 2. Write processed data items (disruptions) to an output file for the next step in the pipeline
    # test = processed_data[0]
    # # print(test.model_dump_json(indent=2))

            
# MASTER INGEST

def ingest(): 

    ingest_tims_data()

ingest()