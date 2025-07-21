"""
Julyan van der Westhuizen
21/07/25

This script outlines various methods called during API requests
"""

import psycopg2
from psycopg2.extras import RealDictCursor

DB_NAME = "datawarehouse"
DB_USER = "julyan"
DB_PASSWORD = "1234"
DB_HOST = "localhost"

# Connecting to the DB
def connect_to_db():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

# DB METHODS - CONNECT TO ENDPOINTS

# General get data method to dump the latest n data items
def get_disruption_data(number_of_items):
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("SELECT * FROM disruptions_history LIMIT %s;", (number_of_items,))
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



