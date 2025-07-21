"""
Julyan van der Westhuizen
21/07/25

This script outlines a FastAPI python API created for safe access to the Postgres database.
The aim is to provide a 'Sevice Layer' to the data pipeline - completing the full lifecycle of data.
"""

from fastapi import FastAPI, HTTPException
from service_layer import database
from service_layer.response_models import DisruptionResponse

app = FastAPI()

#  EXPOSED ENPOINTS
@app.get("/disruption-data", response_model=list[DisruptionResponse])
def get_data(n: int = 10):
    # convert database output to Disruption model for validation
    data = database.get_disruption_data(n)
    disruptions = []
    for d in data:
        disruptions.append(DisruptionResponse(**d))

    return disruptions

@app.get("/disruption-data/{disruption_id}", response_model=DisruptionResponse)
def get_disruption(disruption_id: int):
    # Find the data item according to the passed ID.
    data = database.get_disruption_by_id(disruption_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Disruption not found - ID does not exist")
    return DisruptionResponse(**data)


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