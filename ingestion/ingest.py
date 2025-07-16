"""
Julyan van der Westhuizen
16/07/25

This script performs the ingestion of raw, collected data files: 
1) Each snapshot contians 0..n incidents, so we'll split those up into seperate objects
2) We'll clean out all of the stuff that's not relevant
3) Validate what we do have



Schema of an incident
1. TFL id (may need to reconcile)
2. status
3. severity
4. levelOfInterest
5. category
6. start time
7. end time
8. location
9. corridor
10. comment
11. current update (if I understand correctly, this may update as we go along - all related to the same ID tho)
12. cause area - this is nested tho


"""

import os
import json

# INGEST MODULS

def ingest_tims_data():
    
    # Storage setup
    raw_location = "../data/raw"
    processed_location = '../data/processed'
    os.makedirs(processed_location, exist_ok=True)

    processed_data = []

    # Batch process each snapshot in the raw location
    for filename in os.listdir(raw_location):
        filepath = os.path.join(raw_location, filename)
        print(filepath)

        with open(filepath, "r") as f:
            data = json.load(f)

            # For each data item (disruption), handle each disruption as a individual record 
            for d in data:

                # stuff
                pass
                
                processed_data.append(d)

    # Write processed data items (disruptions) to an output file for the next step in the pipeline
    

            
# MASTER INGEST

def ingest(): 

    ingest_tims_data()

ingest()