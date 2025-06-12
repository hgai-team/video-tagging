import time
import asyncio
import json
from typing import Dict, Any
from .logging_manager import get_structured_logger

logger = get_structured_logger(__name__)

class MetricsCollector:
    """
    Thu thập và ghi log các metrics của ứng dụng
    """
    def __init__(self):
        self.counters = {}
        self.timers = {}
        self.start_time = time.time()
        self._periodic_task = None
    
    def increment_counter(self, name: str, value: int = 1, **tags):
        """
        Tăng counter cho một metric cụ thể
        """
        key = name
        if name not in self.counters:
            self.counters[key] = 0
        self.counters[key] += value
        
        # Log sự kiện tăng counter
        logger.debug(
            f"Counter increased: {name}", 
            counter_name=name, 
            increment=value, 
            current_value=self.counters[key],
            metric_type="counter",
            **tags
        )
        
        return self.counters[key]
    
    def start_timer(self, name: str):
        """
        Bắt đầu đo thời gian cho một hoạt động
        """
        self.timers[name] = time.time()
        logger.debug(
            f"Timer started: {name}", 
            timer_name=name,
            metric_type="timer_start"
        )
        
        return self.timers[name]
    
    def stop_timer(self, name: str, **tags):
        """
        Kết thúc đo thời gian và ghi log
        """
        if name not in self.timers:
            logger.warning(
                f"Timer not found: {name}", 
                timer_name=name,
                metric_type="timer_error"
            )
            return None
        
        end_time = time.time()
        elapsed = end_time - self.timers[name]
        
        logger.info(
            f"Timer stopped: {name}", 
            timer_name=name, 
            elapsed_seconds=elapsed,
            metric_type="timer_stop",
            **tags
        )
        
        del self.timers[name]
        return elapsed
    
    def log_metrics(self):
        """
        Ghi log tất cả metrics hiện tại
        """
        uptime = time.time() - self.start_time
        
        # Đếm số lượng metric theo loại
        counter_count = len(self.counters)
        timer_count = len(self.timers)
        
        logger.info(
            "Application metrics", 
            uptime_seconds=uptime,
            counters=self.counters,
            active_timers=list(self.timers.keys()),
            metric_type="summary",
            counter_count=counter_count,
            timer_count=timer_count
        )
    
    async def _periodic_log_task(self, interval_seconds: int):
        """
        Task để ghi log metrics định kỳ
        """
        while True:
            self.log_metrics()
            await asyncio.sleep(interval_seconds)
    
    def start_periodic_logging(self, interval_seconds: int = 60):
        """
        Bắt đầu task để ghi log metrics định kỳ
        """
        # Tạo và bắt đầu task định kỳ
        loop = asyncio.get_event_loop()
        self._periodic_task = loop.create_task(self._periodic_log_task(interval_seconds))
        
        logger.info(
            "Started periodic metrics logging", 
            interval_seconds=interval_seconds,
            metric_type="periodic_config"
        )
        return self._periodic_task

# Tạo singleton instance
metrics = MetricsCollector()