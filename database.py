import sqlite3
import json
from datetime import datetime
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
import aiosqlite
from models import ProcessingStatus, StatusResponse, ProcessingResult

class DatabaseManager:
    def __init__(self, results_db_path: str = "results.db", status_db_path: str = "status.db"):
        self.results_db_path = results_db_path
        self.status_db_path = status_db_path
    
    async def init_databases(self):
        """Initialize both databases and create tables"""
        await self._init_results_db()
        await self._init_status_db()
    
    async def _init_results_db(self):
        """Initialize results database"""
        async with aiosqlite.connect(self.results_db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS results (
                    file_id TEXT PRIMARY KEY,
                    json_output TEXT NOT NULL,
                    processing_time REAL,
                    token_usage TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.commit()
    
    async def _init_status_db(self):
        """Initialize status tracking database"""
        async with aiosqlite.connect(self.status_db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS status (
                    file_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.commit()
    
    async def insert_status(self, file_id: str, status: ProcessingStatus, error_message: Optional[str] = None):
        """Insert or update processing status"""
        async with aiosqlite.connect(self.status_db_path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO status (file_id, status, error_message, updated_at)
                VALUES (?, ?, ?, ?)
            """, (file_id, status.value, error_message, datetime.now()))
            await db.commit()
    
    async def get_status(self, file_id: str) -> Optional[StatusResponse]:
        """Get processing status by file_id"""
        async with aiosqlite.connect(self.status_db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT file_id, status, error_message, created_at, updated_at 
                FROM status WHERE file_id = ?
            """, (file_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return StatusResponse(
                        file_id=row["file_id"],
                        status=ProcessingStatus(row["status"]),
                        error_message=row["error_message"],
                        created_at=datetime.fromisoformat(row["created_at"]),
                        updated_at=datetime.fromisoformat(row["updated_at"])
                    )
                return None
    
    async def insert_result(self, result: ProcessingResult):
        """Insert processing result"""
        async with aiosqlite.connect(self.results_db_path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO results (file_id, json_output, processing_time, token_usage)
                VALUES (?, ?, ?, ?)
            """, (
                result.file_id,
                result.tags.model_dump_json(),
                result.processing_time,
                json.dumps(result.token_usage)
            ))
            await db.commit()
    
    async def get_result(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Get processing result by file_id"""
        async with aiosqlite.connect(self.results_db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT json_output, processing_time, token_usage, created_at
                FROM results WHERE file_id = ?
            """, (file_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return {
                        "file_id": file_id,
                        "tags": json.loads(row["json_output"]),
                        "processing_time": row["processing_time"],
                        "token_usage": json.loads(row["token_usage"]) if row["token_usage"] else {},
                        "created_at": row["created_at"]
                    }
                return None
    
    async def get_all_queued(self) -> list[str]:
        """Get all file_ids with queued status"""
        async with aiosqlite.connect(self.status_db_path) as db:
            async with db.execute("""
                SELECT file_id FROM status WHERE status = ?
            """, (ProcessingStatus.QUEUED.value,)) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

# Global database manager instance
db_manager = DatabaseManager()