"""
Julyan van der Westhuizen
16/07/25

This script defines pydantic models to be used during ingestion. 
These models are defined based on the packet structured (as documented) received from TIMS
Note: field optionality is specified as per most decent API docs: https://api-portal.tfl.gov.uk/api-details#api=Road&operation=Road_DisruptionByPathIdsQueryStripContentQuerySeveritiesQueryCategoriesQuery&definition=ids-DisruptionGet200TextJsonResponse
"""

# todo: docs might be out of data: some of these 'str' types are probs enumerated. We can build that in here
# todo: might add support for multi-pologons later


from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Any
from datetime import datetime
import json

class CRS(BaseModel):
    crs_type: Optional[str] = Field(None, alias="type")
    properties: Optional[dict] = None

class Geography(BaseModel):
    geo_type: Optional[str] = Field(None, alias="type")  
    coordinates: Optional[List[float]] = None  
    crs: Optional[CRS] = None

class Geometry(BaseModel):
    geo_type: Optional[str] = Field(None, alias="type")
    coordinates: Optional[Any] = None  # Accept any type, custom validation below
    crs: Optional[CRS] = None

    @field_validator('coordinates', mode='before')
    @classmethod
    def only_accept_polygon(cls, value):
        # changed so that it only accepts instances ListListListfloat and nothing else - docs aren't clear on what we can expect, so we use this catch all for now. 
        if (
            isinstance(value, list) and
            all(isinstance(ring, list) and
                all(isinstance(point, list) and
                    all(isinstance(coord, float) for coord in point)
                for point in ring)
            for ring in value)
        ):
            return value
        return None

class StreetSegment(BaseModel):
    toid: Optional[str] = None
    coords: Optional[List[List[float]]] = Field(None, alias="lineString")
    sourceSystemId: Optional[int] = None

    @field_validator('coords', mode='before')
    @classmethod
    def convert_to_lists(cls, value):
        # converting the value (which is the lineString of type str, to a list of lists for future storage)
        if isinstance(value, str): 
            try:
                return json.loads(value)
            except Exception:
                return None
        return value
            

class Street(BaseModel):
    name: Optional[str] = None
    closure: Optional[str] = None
    directions: Optional[str] = None
    segments: Optional[List[StreetSegment]] = None

class Disruption(BaseModel):
    tims_id: str = Field(..., alias="id")
    url: Optional[str] = None
    severity: Optional[str] = None
    ordinal: Optional[int] = None
    category: Optional[str] = None
    subCategory: Optional[str] = None
    comments: Optional[str] = None
    currentUpdate: Optional[str] = None
    currentUpdateDateTime: Optional[datetime] = None
    corridorIds: Optional[List[str]] = None
    startDateTime: Optional[datetime] = None
    endDateTime: Optional[datetime] = None
    lastModifiedTime: Optional[datetime] = None
    levelOfInterest: Optional[str] = None
    location: Optional[str] = None
    status: Optional[str] = None
    geography: Optional[Geography] = None
    geometry: Optional[Geometry] = None
    streets: Optional[List[Street]] = None
    isProvisional: Optional[bool] = None
    hasClosures: Optional[bool] = None

