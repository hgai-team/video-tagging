import logging
from fastapi import APIRouter, Query, HTTPException
from typing import List, Dict, Any
from datetime import datetime
from pydantic import BaseModel

from db import task_tracker

logger = logging.getLogger(__name__)

app = APIRouter(
    prefix='/tasks',
    tags=['Tasks']
)


class FailedTaskResponse(BaseModel):
    resource_id: str
    batch_id: str
    start_time: str
    end_time: str
    status: str


@app.get(
    '/failed',
    response_model=List[FailedTaskResponse]
)
async def get_failed_tasks(
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    """
    Get all failed tasks within the specified date range.
    
    Date in ISO 8601 format (YYYY-MM-DDTHH:MM:SS.sssZ)
    """
    try:
        # Validate date format
        try:
            datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        except ValueError:
            raise HTTPException(
                status_code=400, 
                detail="Invalid date format. Please use ISO 8601 format (YYYY-MM-DDTHH:MM:SS.sssZ)"
            )
        
        # Get failed tasks
        failed_tasks = task_tracker.get_failed_tasks(start_date, end_date)
        
        logger.info(f"Retrieved {len(failed_tasks)} failed tasks between {start_date} and {end_date}")
        return failed_tasks
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error retrieving failed tasks: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")