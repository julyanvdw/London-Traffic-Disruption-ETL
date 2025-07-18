"""
Julyan van der Westhuizen 
18/07/25

This file defines a simple class to collect logs throughout the system.
- Aims to centralise log info (instead of having random print statements everywhere)
- provides access to log functionality through a shared instance imported in usage locations
"""

from datetime import datetime 
import os

class PipelineLogger:
    def __init__(self, verbose = True, to_file = True):
        # Makes sure that the class prints logs to a dir in the base dir of the pipeline
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.logs_location =  f"{base_dir}/pipeline_logs"
        self.logs_filename = 'pipeline_logs.txt'

        # Some settings
        self.verbose = verbose
        self.to_file = to_file

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

# creating a sample logger
shared_logger = PipelineLogger()
    