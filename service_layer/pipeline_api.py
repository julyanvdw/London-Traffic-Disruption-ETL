"""
Julyan van der Westhuizen
21/07/25

This script outlines a FastAPI python API created for safe access to the Postgres database.
The aim is to provide a 'Sevice Layer' to the data pipeline - completing the full lifecycle of data.
"""

from fastapi import FastAPI
app = FastAPI()

# 

#  EXPOSED ENPOINTS

@app.get("/data")
def get_data():
    # fetch the data from the DB
    return {"data": "data here"}



"""
/data

Returns all data or a default limited set.
/data/{id}

Returns a single record by its unique ID.
/stats

Returns summary statistics (e.g., total rows, last update time).
/status

Returns pipeline/database health/status info.
/fields

Returns metadata about the columns/fields in your data.
"""