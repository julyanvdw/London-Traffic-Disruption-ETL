"""
Julyan van der Westhuizen
21/07/25

This script outlines a FastAPI python API created for safe access to the Postgres database.
The aim is to provide a 'Sevice Layer' to the data pipeline - completing the full lifecycle of data.
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from service_layer import database
from service_layer.response_models import DisruptionResponse
from datetime import datetime

app = FastAPI()

# EXPOSED ENPOINTS
# Bulk query
@app.get("/disruption-data", response_model=list[DisruptionResponse])
def get_data(n: int = 10):
    # convert database output to Disruption model for validation
    data = database.get_disruption_data(n)
    disruptions = []
    for d in data:
        disruptions.append(DisruptionResponse(**d))

    return disruptions

# Get the latest n number of data items from the DB
@app.get("/disruptions-data/latest-n-data", response_model=list[DisruptionResponse])
def get_latest_n_data(n: int = 10):

    data = database.get_latest_n_data(n)
    disruptions = []
    for d in data:
        disruptions.append(DisruptionResponse(**d))

    return disruptions

# Query by ID
@app.get("/disruption-data/filter-id/{disruption_id}", response_model=DisruptionResponse)
def get_disruption(disruption_id: int):
    # Find the data item according to the passed ID.
    data = database.get_disruption_by_id(disruption_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Disruption not found - ID does not exist")
    return DisruptionResponse(**data)

# Get data within a time range (date and time)
@app.get("/disruption-data/by-snapshot-date", response_model=list[DisruptionResponse])
def get_disruptions_in_time_range(start_datetime: datetime, end_datetime: datetime):
    
    data = database.get_disruptions_in_time_range(start_datetime, end_datetime)
    disruptions = []
    for d in data:
        disruptions.append(DisruptionResponse(**d))
    
    return disruptions

# Bulk data download JSON for n data items
@app.get("/disruption-data/export-latest-json")
def export_json(n: int = 10):
    # resuse the latest n database method
    data = database.get_latest_n_data(n) 
    # Make sure the pydantic model can serialize the datetime fields by setting the mode to json
    results = []
    for d in data:
        results.append(DisruptionResponse(**d).model_dump(mode="json"))
    
    return JSONResponse(content=results, headers={"Content-Disposition": "attachment; filename=disruptions.json"})

# Get all the unique disruptions from the DB per tims_id
@app.get("/disruption-data/unique-tims-id", response_model=list[DisruptionResponse])
def get_unique_disruptions():

    data = database.get_unique_disruptions()
    disruptions = []
    for d in data:
        disruptions.append(DisruptionResponse(**d))
    
    return disruptions