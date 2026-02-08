from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Dict, Any

class EventBase(BaseModel):
    event_type: str
    payload: Dict[str, Any] = Field(default_factory=dict)

class EventCreate(EventBase):
    pass

class EventResponse(EventBase):
    id: int
    created_at: datetime
