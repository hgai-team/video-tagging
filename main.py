import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from models import ProcessRequest, ProcessResponse, StatusResponse
from database import db_manager
from task_manager import task_manager
from config import MAX_CONCURRENT_TASKS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('video_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """App lifespan events"""
    # Startup
    logger.info("Starting up video processing service")
    
    # Initialize databases
    await db_manager.init_databases()
    logger.info("Databases initialized")
    
    # Start task manager worker
    worker_task = asyncio.create_task(task_manager.start_worker())
    logger.info("Task manager started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down video processing service")
    await task_manager.stop_worker()
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

app = FastAPI(
    title="Video Processing Service",
    description="Async video processing and AI tagging service",
    version="1.0.0",
    lifespan=lifespan
)

@app.post("/process", response_model=ProcessResponse)
async def process_video(request: ProcessRequest):
    """Submit video for processing"""
    try:
        success = await task_manager.submit_task(request)
        
        if not success:
            raise HTTPException(
                status_code=409, 
                detail=f"File {request.file_id} already processed or processing"
            )
        
        return ProcessResponse(file_id=request.file_id, status="queued")
    
    except Exception as e:
        logger.error(f"Error submitting task {request.file_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/status/{file_id}")
async def get_status(file_id: str):
    """Get processing status for a file"""
    try:
        status_info = await task_manager.get_task_status(file_id)
        
        if status_info["status"] == "not_found":
            raise HTTPException(status_code=404, detail="File ID not found")
        
        return status_info
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting status for {file_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/stats")
async def get_system_stats():
    """Get system processing statistics"""
    try:
        return await task_manager.get_stats()
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "video-processing"}

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )