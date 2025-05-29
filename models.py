from pydantic import BaseModel
from typing import List, Optional
from enum import Enum
from datetime import datetime

class ProcessingStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing" 
    COMPLETED = "completed"
    FAILED = "failed"

class ProcessRequest(BaseModel):
    video_url: str
    file_id: str

class ProcessResponse(BaseModel):
    file_id: str
    status: str = "queued"

class StatusResponse(BaseModel):
    file_id: str
    status: ProcessingStatus
    created_at: datetime
    updated_at: datetime
    error_message: Optional[str] = None

# AI Tagging Models (from original code)
class EntitiesModel(BaseModel):
    Person: List[str]
    Animal: List[str]  
    Object: List[str]

class SceneContextModel(BaseModel):
    Type: str
    HumanMadeArea: List[str]
    NaturalEnv: List[str]

class TemporalContextModel(BaseModel):
    PartOfDay: str
    Season: str
    Weather: str
    Lighting: List[str]

class TagsModel(BaseModel):
    Overview: List[str]
    Entities: EntitiesModel
    ActionsEventsFestivals: List[str]
    SceneContext: SceneContextModel
    TemporalContext: TemporalContextModel
    AffectiveMoodVibe: List[str]
    Color: List[str]
    VisualAttributes: List[str]
    CameraTechniques: List[str]

class OutputSchema(BaseModel):
    Tags: TagsModel

class ProcessingResult(BaseModel):
    file_id: str
    tags: OutputSchema
    processing_time: float
    token_usage: dict