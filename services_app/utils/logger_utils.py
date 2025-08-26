import logging
import os
from pathlib import Path
from config.config import LOG_LEVEL


def setup_pipeline_logger(logger_name: str, log_file_name: str) -> logging.Logger:
    """
    Setup dedicated logger for pipeline with file handler.
    
    Args:
        logger_name: Name for the logger (usually __name__)
        log_file_name: Name of log file (e.g., 'unlabeled_VIDEO.log')
    
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(logger_name)
    
    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(LOG_LEVEL)
        
        # Create logs directory if it doesn't exist
        log_dir = Path("./logs")
        log_dir.mkdir(exist_ok=True)
        
        # Create file handler
        log_file_path = log_dir / log_file_name
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(LOG_LEVEL)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(LOG_LEVEL)
        
        # Create formatter
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        # Prevent logging to parent loggers
        logger.propagate = False
    
    return logger