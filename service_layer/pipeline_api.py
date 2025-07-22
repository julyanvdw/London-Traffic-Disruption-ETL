"""
Julyan van der Westhuizen
21/07/25

This script outlines a FastAPI python API created for safe access to the Postgres database.
The aim is to provide a 'Sevice Layer' to the data pipeline - completing the full lifecycle of data.
"""


from fastapi import FastAPI, HTTPException
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
def get_latest_n_data(n: int=10):

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


# Get data within the last n minutes or something

# Bulk data download CSV / JSON for n data items

# Filter by attributes

# Distinct Values

