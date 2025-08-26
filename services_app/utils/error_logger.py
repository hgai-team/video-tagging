import csv
import logging
import os
from pathlib import Path
from threading import Lock

logger = logging.getLogger(__name__)

HTTP_404_CSV_FILE = "logs/http_404_errors.csv"
DOWNLOAD_ERRORS_CSV_FILE = "logs/download_errors.csv" # New CSV for all download errors
CSV_LOCK = Lock()

def log_download_error(resource_id: str, url: str, error_message: str):
    """Logs any download error to a CSV file with format: resource_id,url,error_message"""
    try:
        with CSV_LOCK:
            csv_path = Path(DOWNLOAD_ERRORS_CSV_FILE)
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_exists = csv_path.exists()
            
            with open(csv_path, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                if not file_exists:
                    writer.writerow(['resource_id', 'url', 'error_message'])
                    
                writer.writerow([resource_id, url, error_message])
                
            logger.info(f"Logged download error to CSV: {resource_id}")
            
    except Exception as e:
        logger.error(f"Failed to log download error to CSV: {str(e)}")

def log_http_404_error(resource_id: str, url: str):
    """Logs HTTP 404 error to a specific CSV file."""
    try:
        with CSV_LOCK:
            csv_path = Path(HTTP_404_CSV_FILE)
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_exists = csv_path.exists()
            
            with open(csv_path, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                if not file_exists:
                    writer.writerow(['resource_id', 'url'])
                    
                writer.writerow([resource_id, url])
                
            logger.info(f"Logged HTTP 404 error to CSV: {resource_id}")
            
    except Exception as e:
        logger.error(f"Failed to log HTTP 404 error to CSV: {str(e)}")

def get_http_404_count() -> int:
    """Counts the number of logged HTTP 404 errors."""
    try:
        csv_path = Path(HTTP_404_CSV_FILE)
        if not csv_path.exists():
            return 0
            
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)
            return sum(1 for _ in reader)
            
    except Exception as e:
        logger.error(f"Failed to count HTTP 404 errors: {str(e)}")
        return 0

def get_http_404_resource_ids() -> set:
    """Gets the set of resource_ids that have previously resulted in an HTTP 404."""
    try:
        csv_path = Path(HTTP_404_CSV_FILE)
        if not csv_path.exists():
            return set()
            
        resource_ids = set()
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)
            for row in reader:
                if row and len(row) > 0:
                    resource_ids.add(row[0])
                    
        logger.info(f"Loaded {len(resource_ids)} HTTP 404 resource IDs for filtering")
        return resource_ids
        
    except Exception as e:
        logger.error(f"Failed to load HTTP 404 resource IDs: {str(e)}")
        return set()

def should_skip_resource(resource_id: str) -> bool:
    """Checks if a resource_id should be skipped due to a previous 404 error."""
    if not resource_id:
        return False
        
    http_404_ids = get_http_404_resource_ids()
    return resource_id in http_404_ids