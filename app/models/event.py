from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Dict, Any

class UserBase(BaseModel):
    username: str
    email: str

class UserCreate(UserBase):
    pass

class UserResponse(UserBase):
    id: int
    created_at: datetime

class EventBase(BaseModel):
    event_type: str
    user_id: Optional[int] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class EventCreate(EventBase):
    pass

class EventResponse(EventBase):
    id: int
    user_id: Optional[int] = None
    created_at: datetime
