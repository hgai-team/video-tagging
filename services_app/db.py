import os
import sqlite3
import logging
import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

# Configure logging
logger = logging.getLogger(__name__)

class DB:
    """SQLite database manager for task tracking"""
    
    def __init__(self, db_path: str = "database/tasks.db"):
        """Initialize the database connection"""
        self.db_path = db_path
        
        # Ensure database directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Initialize the database if it doesn't exist
        self._init_db()
    
    def _init_db(self):
        """Create the database and tables if they don't exist"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create tasks table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                resource_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT,
                status TEXT NOT NULL
            )
            ''')
            
            # Create index for faster queries
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_resource_id ON tasks (resource_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_batch_id ON tasks (batch_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_start_time ON tasks (start_time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON tasks (status)')
            
            conn.commit()
            logger.info(f"Database initialized at {self.db_path}")
        except Exception as e:
            logger.error(f"Database initialization failed: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
    
    def add_task(self, resource_id: str, batch_id: str) -> int:
        """Add a new task with RUNNING status and return the task ID"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            start_time = datetime.datetime.now().isoformat()
            
            cursor.execute(
                'INSERT INTO tasks (resource_id, batch_id, start_time, status) VALUES (?, ?, ?, ?)',
                (resource_id, batch_id, start_time, 'RUNNING')
            )
            
            task_id = cursor.lastrowid
            conn.commit()
            
            logger.debug(f"Task started: resource_id={resource_id}, batch_id={batch_id}, task_id={task_id}")
            return task_id
        except Exception as e:
            logger.error(f"Failed to add task: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
    
    def update_task_status(self, resource_id: str, batch_id: str, status: str) -> bool:
        """Update task status to SUCCESS or FAILED"""
        if status not in ['SUCCESS', 'FAILED']:
            raise ValueError("Status must be either 'SUCCESS' or 'FAILED'")
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            end_time = datetime.datetime.now().isoformat()
            
            cursor.execute(
                '''UPDATE tasks SET status = ?, end_time = ? 
                WHERE resource_id = ? AND batch_id = ? AND status = 'RUNNING' ''',
                (status, end_time, resource_id, batch_id)
            )
            
            if cursor.rowcount == 0:
                logger.warning(f"No running task found for resource_id={resource_id}, batch_id={batch_id}")
                return False
            
            conn.commit()
            logger.debug(f"Task updated: resource_id={resource_id}, batch_id={batch_id}, status={status}")
            return True
        except Exception as e:
            logger.error(f"Failed to update task: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()
    
    def get_failed_tasks(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Get all failed tasks within the date range"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable row factory to get dict-like rows
            cursor = conn.cursor()
            
            cursor.execute(
                '''SELECT resource_id, batch_id, start_time, end_time, status 
                FROM tasks 
                WHERE status = 'FAILED' 
                AND start_time >= ? AND start_time <= ?''',
                (start_date, end_date)
            )
            
            # Convert to list of dictionaries
            failed_tasks = [dict(row) for row in cursor.fetchall()]
            
            logger.debug(f"Retrieved {len(failed_tasks)} failed tasks between {start_date} and {end_date}")
            return failed_tasks
        except Exception as e:
            logger.error(f"Failed to get failed tasks: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()


class TaskTracker:
    """Utility class for tracking task status throughout the pipeline"""
    
    def __init__(self):
        """Initialize the task tracker with a DB connection"""
        self.db = DB()
    
    def start_task(self, resource_id: str, batch_id: str) -> int:
        """Mark a task as started and return the task ID"""
        return self.db.add_task(resource_id, batch_id)
    
    def mark_success(self, resource_id: str, batch_id: str) -> bool:
        """Mark a task as successfully completed"""
        return self.db.update_task_status(resource_id, batch_id, 'SUCCESS')
    
    def mark_failed(self, resource_id: str, batch_id: str) -> bool:
        """Mark a task as failed"""
        return self.db.update_task_status(resource_id, batch_id, 'FAILED')
    
    def get_failed_tasks(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Get all failed tasks within the date range"""
        return self.db.get_failed_tasks(start_date, end_date)


# Create a singleton instance
task_tracker = TaskTracker()