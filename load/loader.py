"""
Julyan van der Westhuizen
17/06/25

This scripts loads the transformed data from the datalake into the data store (PostgreSQL database). 
"""

import os
import json
import psycopg2
from datetime import datetime

DB_NAME = "datawarehouse"
DB_USER = "julyan"
DB_PASSWORD = "1234"
DB_HOST = "localhost"

def load_tims_data():
    #0. setup storage locations and db connection
    transformed_location = "../datalake/transformed"
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST
    )
    cursor = conn.cursor()

    #1. Batch process the tansformed snapshots
    for filename in os.listdir(transformed_location):
        filepath = os.path.join(transformed_location, filename)

        with open(filepath, "r") as f:
            data = json.load(f)

            #2. load each disruption into the database
            for d in data:
                # manually flatten the disruption object into a tuple
                adding_timestamp = datetime.now()   
                disruption_row = (
                    d["tims_id"],
                    adding_timestamp,
                    d.get("url"),
                    d.get("severity"),
                    d.get("ordinal"),
                    d.get("category"),
                    d.get("subCategory"),
                    d.get("comments"),
                    d.get("currentUpdate"),
                    d.get("currentUpdateDateTime"),
                    json.dumps(d.get("corridorIds")),
                    d.get("startDateTime"),
                    d.get("endDateTime"),
                    d.get("lastModifiedTime"),
                    d.get("levelOfInterest"),
                    d.get("location"),
                    d.get("status"),
                    json.dumps(d.get("geography")),
                    json.dumps(d.get("geometry")),
                    d.get("isProvisional"),
                    d.get("hasClosures")
                )

                # execute the psql command
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

                # Flatten and insert streets
                if d.get("streets") != None:
                    for street in d.get("streets", []):
                        street_row = (
                            d["tims_id"],
                            adding_timestamp,
                            street.get("name"),
                            street.get("closure"),
                            street.get("directions"),
                            json.dumps(street.get("segments"))
                        )
                        cursor.execute("""
                            INSERT INTO streets 
                                (
                                    tims_id, snapshot_time, name, closure, directions, segments
                                ) 
                                VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING;
                        """, street_row)
                
    conn.commit()
    cursor.close()
    conn.close()



def load():
    #load TIMS data into 
    load_tims_data()


load()