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
def get_all_data(number_of_items):
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM your_table LIMIT %s;", (number_of_items,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_data_by_id(record_id):
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM your_table WHERE id = %s;", (record_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result

def get_stats():
    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM your_table;")
    total = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return {"total_rows": total}

def get_fields():
    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'your_table';")
    fields = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return {"fields": fields}

