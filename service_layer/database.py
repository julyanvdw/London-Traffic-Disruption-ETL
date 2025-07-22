"""
Julyan van der Westhuizen
21/07/25

This script outlines various methods called during API requests
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv #type: ignore
import os

# Load the value from the .env file (note: there should be a .env file in the root dir)
load_dotenv()

DB_URL = os.getenv("DB_URL")

# Connecting to the DB
def connect_to_db():
    conn = psycopg2.connect(DB_URL)
    return conn

# DB METHODS - CONNECT TO ENDPOINTS

# General get data method to dump n data items (from the start of the DB)
def get_disruption_data(number_of_items):
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("SELECT * FROM disruptions_history ORDER BY id ASC LIMIT %s;", (number_of_items,))
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    return results

# Find the latest n data items
def get_latest_n_data(n):
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("SELECT * FROM disruptions_history ORDER BY snapshot_time DESC LIMIT %s;", (n,))
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    return results

# Find Disruption by ID
def get_disruption_by_id(disruption_id):
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("SELECT * FROM disruptions_history WHERE id = %s;", (disruption_id,))
    result = cursor.fetchone()

    cursor.close()
    conn.close()

    return result

# Find the Disruptions within a particular date and time range
def get_disruptions_in_time_range(start_datetime, end_datetime):
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("SELECT * FROM disruptions_history WHERE snapshot_time >= %s AND snapshot_time <= %s", (start_datetime, end_datetime))
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    return results

# Select all the unique values based on tims_id
def get_unique_disruptions():
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("SELECT DISTINCT ON (tims_id) * FROM disruptions_history ORDER BY tims_id, snapshot_time ASC;")
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    return results