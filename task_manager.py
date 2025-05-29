import asyncio
import logging
from typing import Dict, Set
from datetime import datetime

from models import ProcessingStatus, ProcessRequest
from database import db_manager
from video_processor import VideoProcessor
from config import MAX_CONCURRENT_TASKS

logger = logging.getLogger(__name__)

class TaskManager:
    def __init__(self):
        self.task_queue = asyncio.Queue()
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.processing_files: Set[str] = set()
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        self.worker_running = False
    
    async def start_worker(self):
        """Start the background worker to process tasks"""
        if self.worker_running:
            return
        
        self.worker_running = True
        logger.info("Task manager worker started")
        
        # Start multiple workers to handle queue
        workers = [
            asyncio.create_task(self._worker())
            for _ in range(min(10, MAX_CONCURRENT_TASKS))  # Up to 10 workers
        ]
        
        await asyncio.gather(*workers, return_exceptions=True)
    
    async def _worker(self):
        """Background worker to process tasks from queue"""
        while self.worker_running:
            try:
                # Get task from queue with timeout
                task_data = await asyncio.wait_for(
                    self.task_queue.get(), 
                    timeout=1.0
                )
                
                # Process the task
                await self._process_task(task_data)
                self.task_queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Worker error: {e}")
                await asyncio.sleep(1)
    
    async def _process_task(self, request: ProcessRequest):
        """Process a single video task"""
        file_id = request.file_id
        
        # Check if already processing
        if file_id in self.processing_files:
            logger.warning(f"File {file_id} already being processed")
            return
        
        # Acquire semaphore for concurrent limit
        async with self.semaphore:
            try:
                self.processing_files.add(file_id)
                
                # Update status to processing
                await db_manager.insert_status(file_id, ProcessingStatus.PROCESSING)
                logger.info(f"Started processing {file_id}")
                
                # Process video
                async with VideoProcessor() as processor:
                    result = await processor.process_video_with_retry(
                        file_id, request.video_url
                    )
                
                if result:
                    # Save result and mark as completed
                    await db_manager.insert_result(result)
                    await db_manager.insert_status(file_id, ProcessingStatus.COMPLETED)
                    logger.info(f"Completed processing {file_id}")
                else:
                    # Mark as failed
                    await db_manager.insert_status(
                        file_id, 
                        ProcessingStatus.FAILED,
                        "Processing failed after all retries"
                    )
                    logger.error(f"Failed processing {file_id}")
                
            except Exception as e:
                # Handle unexpected errors
                error_msg = f"Unexpected error: {str(e)}"
                await db_manager.insert_status(
                    file_id, 
                    ProcessingStatus.FAILED, 
                    error_msg
                )
                logger.error(f"Error processing {file_id}: {e}")
                
            finally:
                self.processing_files.discard(file_id)
    
    async def submit_task(self, request: ProcessRequest) -> bool:
        """Submit a new task for processing"""
        file_id = request.file_id
        
        # Check if already exists
        existing_status = await db_manager.get_status(file_id)
        if existing_status:
            if existing_status.status in [ProcessingStatus.PROCESSING, ProcessingStatus.COMPLETED]:
                logger.warning(f"File {file_id} already processed or processing")
                return False
        
        # Add to status tracking
        await db_manager.insert_status(file_id, ProcessingStatus.QUEUED)
        
        # Add to queue
        await self.task_queue.put(request)
        logger.info(f"Queued task for {file_id}")
        
        return True
    
    async def get_queue_size(self) -> int:
        """Get current queue size"""
        return self.task_queue.qsize()
    
    async def get_active_count(self) -> int:
        """Get number of actively processing tasks"""
        return len(self.processing_files)
    
    async def get_task_status(self, file_id: str) -> dict:
        """Get comprehensive task status"""
        status = await db_manager.get_status(file_id)
        result = await db_manager.get_result(file_id)
        
        response = {
            "file_id": file_id,
            "status": status.status.value if status else "not_found",
            "queue_position": None,
            "active_processing": file_id in self.processing_files
        }
        
        if status:
            response.update({
                "created_at": status.created_at.isoformat(),
                "updated_at": status.updated_at.isoformat(),
                "error_message": status.error_message
            })
        
        if result:
            response["result"] = result
        
        return response
    
    async def stop_worker(self):
        """Stop the background worker"""
        self.worker_running = False
        logger.info("Task manager worker stopped")
    
    async def get_stats(self) -> dict:
        """Get system statistics"""
        return {
            "queue_size": await self.get_queue_size(),
            "active_processing": await self.get_active_count(),
            "max_concurrent": MAX_CONCURRENT_TASKS,
            "available_slots": MAX_CONCURRENT_TASKS - len(self.processing_files)
        }

# Global task manager instance
task_manager = TaskManager()