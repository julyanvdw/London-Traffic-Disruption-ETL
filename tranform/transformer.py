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
from datetime import datetime

def ingest_tims_data():
    
    # 0. Storage setup
    raw_location = "../datalake/raw"
    processed_location = '../datalake/transformed'
    os.makedirs(processed_location, exist_ok=True)

    processed_data = []

    # 1. Batch process each snapshot from the raw location
    for filename in os.listdir(raw_location):
        filepath = os.path.join(raw_location, filename)

        with open(filepath, "r") as f:
            data = json.load(f)

            # For each data item (representing a disruption), handle each disruption as an individual record
            for d in data: 
                # try auto-converting using into pydantic
                try:
                    disruption = Disruption(**d)
                    processed_data.append(disruption)
                except Exception as e:
                    print(f"Couldn't parse into pydantic object. {e}")

            # Remove possible duplicates
            seen = {}
            for disruption in processed_data:
                if disruption.tims_id not in seen:
                    seen[disruption.tims_id] = disruption

            deduplicated_data = list(seen.values())

    # # 2. Write processed data items (disruptions) to an output file for the next step in the pipeline
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    filename = f"transformed-snapshot-{timestamp}.json"
    output_path = os.path.join(processed_location, filename)

    with open(output_path, "w") as f:
        dumped_data = []
        for d in deduplicated_data:
            dumped_data.append(d.model_dump())
        json.dump(dumped_data, f, indent=2, default=str)   

def ingest(): 

    ingest_tims_data()

    # add other ingest streams here 
    # also we can do some data integration of applicable

ingest()