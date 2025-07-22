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
from psycopg2.extras import execute_batch

DB_URL = "postgresql://postgres.stmxtgfmlvovmdomnhfq:julyanvdwlondonetl@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"

def connect_to_db():
    conn = psycopg2.connect(DB_URL)
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

def query_for_info(cursor, start_time):

    # Chcek which rows are younger than the start time - these are the newly added rows
    cursor.execute("SELECT COUNT(*) FROM disruptions_history WHERE snapshot_time > %s;", (start_time,))
    last_added_disruptions = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM streets WHERE snapshot_time > %s;", (start_time,))
    last_added_streets = cursor.fetchone()[0]

    last_added_rows = last_added_disruptions + last_added_streets

    # Get the total row count from all tables
    cursor.execute("SELECT COUNT(*) FROM disruptions_history;")
    disruptions_total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM streets;")
    streets_total = cursor.fetchone()[0]

    total_rows = disruptions_total + streets_total

    # Update info in the logger class
    shared_logger.last_run_info["Last-added-rows"] = str(last_added_rows)
    shared_logger.last_run_info["Total-rows"] = str(total_rows)

def load_tims_data(conn, cursor):
        
    # Set up the manager to get data from file system
    manager = LakeManager()
    files_data = manager.read_TIMS_transformed_snapshot() #note files_data results in the form [file_in_dir][data_item_in_file]

    # flatten all the rows and handle the nested streets inside each disruption object
    disruption_rows = []
    street_rows = []

    for data in files_data:
        for d in data:
            disruption_row, current_street_rows = flatten_disruption(d)
            disruption_rows.append(disruption_row)
            street_rows.extend(current_street_rows)

    items_loaded_count = len(disruption_rows) + len(street_rows)

    # Batch insert disruptions
    if len(disruption_rows) != 0:
        execute_batch(cursor, """
            INSERT INTO disruptions_history (
                tims_id, snapshot_time, url, severity, ordinal, category, subCategory, comments,
                currentUpdate, currentUpdateDateTime, corridorIds, startDateTime, endDateTime,
                lastModifiedTime, levelOfInterest, location, status, geography, geometry,
                isProvisional, hasClosures
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (tims_id, snapshot_time) DO NOTHING;
        """, disruption_rows)

    # Batch insert streets
    if len(street_rows) != 0:
        execute_batch(cursor, """
            INSERT INTO streets (
                tims_id, snapshot_time, name, closure, directions, segments
            ) VALUES (
                %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT DO NOTHING;
        """, street_rows)

    # Update metric
    shared_logger.last_run_info["Items-loaded"] = str(items_loaded_count)

    conn.commit()

def load():
    # SET UP DB CONNECTION
    try: 
        shared_logger.log("Attempting to connect to DB...")
        conn = connect_to_db()
        cursor = conn.cursor()
        shared_logger.log("Connected to DB")
        shared_logger.last_run_info["Database-status"] = 0
    except Exception as e: 
        shared_logger.log_warning(f"Could not connect to database: {e}")
        shared_logger.last_run_info["Database-status"] = 1

    # time-keep the operation to detect new records
    start_time = datetime.now()

    # LOAD DATA FROM VARIOUS SOURCES
    try:
        load_tims_data(conn=conn, cursor=cursor)
        shared_logger.last_run_info["Last-load"] = datetime.now().strftime("%H:%M:%S")
        shared_logger.log("Succesfully loaded snapshot into DB.")
        shared_logger.last_run_info["Load-status"] = 0
    except Exception as e:
        shared_logger.log_warning(f"Could not load data to database: {e}")
        shared_logger.last_run_info["Load-status"] = 1

    # Query the DB for logging info
    query_for_info(cursor, start_time)

    # CLOSE THE DB CONNECTION
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load()
