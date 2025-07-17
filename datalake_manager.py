"""
Julyan van der Westhuizen
17/07/25

This file describes management infrastructure to abstract file handling away. 
In real-world scenarios, we'd use enterprise solutions in place filesystems for blob storage etc. 
This addition is meant to simulate / abstract away that component.
When incorporating proper enterprise solutions, this class can be altered - leaving other components untouched. 
"""

import os
import json
from datetime import datetime

class LakeManager:

    def __init__(self):
        self.tims_raw_dir_location = "../datalake/raw/tims"

        # File Structure Setup
        os.makedirs(self.tims_raw_dir_location, exist_ok=True)

    # DATASTREAM-SPECIFIC METHODS

    def write_TIMS_raw_snapshot(self, raw_data):
        # Create filename
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        filename = self.tims_raw_dir_location + "/"+  f"TIMS-snapshot-{timestamp}.json"

        # Write to file
        with open(filename, "w") as f:
            json.dump(raw_data, f)

    

