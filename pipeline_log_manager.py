"""
Julyan van der Westhuizen 
18/07/25

This file defines a simple class to collect logs throughout the system.
- Aims to centralise log info (instead of having random print statements everywhere)
- provides access to log functionality through a shared instance imported in usage locations
"""

from datetime import datetime 
import os
import json

class PipelineLogger:
    def __init__(self, verbose = True, to_file = True):
        # Makes sure that the class prints logs to a dir in the project root of the pipeline
        project_root = os.path.dirname(os.path.abspath(__file__))

        self.logs_location =  f"{project_root}/pipeline_logs"
        self.logs_filename = 'pipeline_logs.log'
        self.history_filename = 'pipeline_history.log'
        self.last_run_info_filename = 'last_run_info.json'

        # Some settings
        self.verbose = verbose
        self.to_file = to_file

        # Information on last run 
        self.last_run_info =  {
            "Last-fetch": "",
            "Fetch-count": "",
            "Data-transformed": "",
            "Fields-stripped": "",
            "Last-load": "",
            "Items-loaded": "",
            "Last-added-rows":"",
            "Total-rows":"",
            "Extract-status": 0,
            "Transform-status": 0,
            "Load-status": 0,
            "Database-status": 0
        }

        # Creates logs dir if it doesn't exist yet
        os.makedirs(self.logs_location, exist_ok=True)

    def log(self, message, log_type="LOG"):
        # default log is of type LOG
        # each log has a timestamp and a message
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log = f"[{log_type}]  {timestamp}  {message}"

        # Output logs according to settings
        if self.verbose:
            print(log)
        if self.to_file:
            with open(f"{self.logs_location}/{self.logs_filename}", "a") as f:
                f.write(log + "\n")

    def log_warning(self, message):
        self.log(message, log_type="WARNING")

    def log_error(self, message):
        self.log(message, log_type="ERROR")

    def log_pipeline_phase(self, phase):
        self.log("", log_type=f"PIPELINE-PHASE: {phase}")

    def save_last_run_info(self):
        # This method writes a json file containing information from the last pipeline run. 
        # This info will be read by the TUI and therefore update the user on the current stance of things. 

        with open(f"{self.logs_location}/{self.last_run_info_filename}", "w") as f:
            json.dump(self.last_run_info, f)

        # Write pipeline history
        with open(f"{self.logs_location}/{self.history_filename}", "a") as f:
            f.write("[PIPELINE HISTORY SLICE OPEN]\n")
            f.write(f"[EXTRACT ENTRY:]  {self.last_run_info["Last-fetch"]}\n")
            f.write(f"Items Fetched:  {self.last_run_info["Fetch-count"]}\n")
            f.write(f"[TRANSFORM ENTRY]\n")
            f.write(f"Data transformed:  {self.last_run_info["Data-transformed"]}\n")
            f.write(f"Fields stripped:  {self.last_run_info["Fields-stripped"]}\n")
            f.write(f"[LOAD ENTRY:]  {self.last_run_info["Last-load"]}\n")
            f.write(f"Items loaded:  {self.last_run_info["Items-loaded"]}\n")
            f.write(f"[DATABASE ENTRY]\n")
            f.write(f"Most recent rows added:  {self.last_run_info["Last-added-rows"]}\n")
            f.write(f"Total DB rows:  {self.last_run_info["Total-rows"]}\n")
            f.write(f"[STATUS - Extract:] {"OK" if self.last_run_info["Extract-status"] == 0 else "ERROR"}\n")
            f.write(f"[STATUS - Transform:] {"OK" if self.last_run_info["Transform-status"] == 0 else "ERROR"}\n")
            f.write(f"[STATUS - Load:] {"OK" if self.last_run_info["Load-status"] == 0 else "ERROR"}\n")
            f.write(f"[STATUS - Database:] {"OK" if self.last_run_info["Database-status"] == 0 else "ERROR"}\n")
            f.write("[PIPELINE HISTORY SLICE CLOSE]\n")


# creating a shared logger
shared_logger = PipelineLogger()
    