"""
Julyan van der Westhuizen
17/06/25

This scripts loads the transformed data from the datalake into the data store (PostgreSQL database). 
"""

import psycopg2
import json
from datalake_manager import LakeManager
from datetime import datetime
from pipeline_log_manager import shared_logger

DB_NAME = "datawarehouse"
DB_USER = "julyan"
DB_PASSWORD = "1234"
DB_HOST = "localhost"


def connect_to_db():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST
    )

    return conn

def flatten_disruption(disruption):
        # Takes the nested JSON object and flattens it into a tuple according to the DB schema
        # Also flattens the streets component
        # Returns both
        
        adding_timestamp = datetime.now()   
        disruption_row = (
            disruption["tims_id"],
            adding_timestamp,
            disruption.get("url"),
            disruption.get("severity"),
            disruption.get("ordinal"),
            disruption.get("category"),
            disruption.get("subCategory"),
            disruption.get("comments"),
            disruption.get("currentUpdate"),
            disruption.get("currentUpdateDateTime"),
            json.dumps(disruption.get("corridorIds")),
            disruption.get("startDateTime"),
            disruption.get("endDateTime"),
            disruption.get("lastModifiedTime"),
            disruption.get("levelOfInterest"),
            disruption.get("location"),
            disruption.get("status"),
            json.dumps(disruption.get("geography")),
            json.dumps(disruption.get("geometry")),
            disruption.get("isProvisional"),
            disruption.get("hasClosures")
        )

        street_rows = []
        if disruption.get("streets") != None:
            for street in disruption.get("streets", []):
                street_rows.append(
                    (
                        disruption["tims_id"],
                        adding_timestamp,
                        street.get("name"),
                        street.get("closure"),
                        street.get("directions"),
                        json.dumps(street.get("segments"))
                    )
                )

        return disruption_row, street_rows

def load_tims_data(conn, cursor):
        
    manager = LakeManager()
    files_data = manager.read_TIMS_transformed_snapshot() #note files_data results in the form [file_in_dir][data_item_in_file]
    
    for data in files_data: 
        for d in data:
            disruption_row, street_rows = flatten_disruption(d)

            # EXECUTE SQL COMMANDS ON DB
            cursor.execute("""
                    INSERT INTO disruptions_history 
                        (
                            tims_id, snapshot_time, url, severity, ordinal, category, subCategory, comments,
                            currentUpdate, currentUpdateDateTime, corridorIds, startDateTime, endDateTime,
                            lastModifiedTime, levelOfInterest, location, status, geography, geometry,
                            isProvisional, hasClosures        
                        ) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tims_id, snapshot_time) DO NOTHING;
            """, disruption_row)

            for street in street_rows:
                cursor.execute("""
                        INSERT INTO streets 
                            (
                                tims_id, snapshot_time, name, closure, directions, segments
                            ) 
                            VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                """, street)

            conn.commit()

def load():
    # SET UP DB CONNECTION
    shared_logger.log("Attempting to connect to DB...")
    conn = connect_to_db()
    cursor = conn.cursor()
    shared_logger.log("Connected to DB")

    # LOAD DATA FROM VARIOUS SOURCES
    load_tims_data(conn=conn, cursor=cursor)
    shared_logger.log("Succesfully loaded snapshot into DB.")

    # CLOSE THE DB CONNECTION
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load()
