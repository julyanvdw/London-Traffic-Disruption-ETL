"""
Julyan van der Westhuizen
21/07/25

This script outlines a FastAPI python API created for safe access to the Postgres database.
The aim is to provide a 'Sevice Layer' to the data pipeline - completing the full lifecycle of data.
"""

from fastapi import FastAPI
from models.tims_models import Disruption
from service_layer import database

app = FastAPI()

# 

#  EXPOSED ENPOINTS
@app.get("/disruption-data", response_model=list[Disruption])
def get_data(n: int = 10):

    # convert database output to Disruption model for validation
    data = database.get_disruption_data(n)
    disruptions = []
    for d in data:
        d.pop("id", None) #Gets rid of the database's auto-generated id field
        d["id"] = d.pop("tims_id")  # Rename tims_id to id for pydantic model
        disruptions.append(Disruption(**d))

    return disruptions


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