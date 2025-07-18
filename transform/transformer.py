"""
Julyan van der Westhuizen
16/07/25

This script performs the ingestion of raw, collected data files: 
1) Each snapshot contians 0..n traffic disruptions
2) Data then undergoes validation and cleaning by parisng the JSON data with pydantic models defined in tims_models.py
"""

import sys
sys.path.append("../")
from transform.tims_models import Disruption
from datalake_manager import LakeManager


def ingest_tims_data():
    
    processed_data = []
    manager = LakeManager()
    files_data = manager.read_TIMS_raw_snapshot()

    # note: files_data represnets data in the format [file_in_raw_tims_dir][data_item_for_that_file]
    for data in files_data:
        # data represetns a collection of data items (disruptions)
        for d in data:
            # try auto-converting by parsing with pydantic
            try: 
                disruption = Disruption(**d)
                processed_data.append(disruption)
            except Exception as e:
                print(f"Could not parse into pydantic object. {e}")
            
    # remove possible duplicates in the data
    seen = {}
    for disruption in processed_data:
        if disruption.tims_id not in seen: 
            seen[disruption.tims_id] = disruption

    deduplicated_data = list(seen.values())
    manager.write_TIMS_transformed_snapshot(deduplicated_data)


if __name__ == "__main__":
    ingest_tims_data()

    # add other ingest streams here 
    # also we can do some data integration of applicable