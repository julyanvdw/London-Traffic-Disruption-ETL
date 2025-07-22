"""
Julyan van der Westhuizen
21/07/25

This script defines pydantic models as API response modules
note: these differ from the models used to transform the data due to the extra fields added while loading data into the database
"""

from pydantic import BaseModel
from typing import Optional, List, Any
from datetime import datetime

# General Data Blob (all fields) response model
class DisruptionResponse(BaseModel):
    id:int
    tims_id: str
    snapshot_time: datetime
    severity: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    comments: Optional[str] = None
    currentupdate: Optional[str] = None
    currentupdatedatetime: Optional[datetime] = None
    corridorids: Optional[List[str]] = None  
    startdatetime: Optional[datetime] = None
    enddatetime: Optional[datetime] = None
    lastmodifiedtime: Optional[datetime] = None
    levelofinterest: Optional[str] = None
    location: Optional[str] = None
    status: Optional[str] = None
    geography: Optional[Any] = None   
    geometry: Optional[Any] = None    
    isprovisional: Optional[bool] = None
    hasclosures: Optional[bool] = None