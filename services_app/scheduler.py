import asyncio
import logging
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from pipeline import PipelineProcessor

logger = logging.getLogger(__name__)

class DailyScheduler:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.processor = PipelineProcessor()
        
    async def run_daily_pipeline(self):
        """Execute daily pipeline with current date range"""
        try:
            # Use yesterday's date range
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=1)
            
            logger.info("Starting scheduled pipeline", 
                       start_date=start_date.isoformat(), 
                       end_date=end_date.isoformat())
            
            await self.processor.process_daily_batch(
                media_type=2,  # Configure as needed
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                collection_name="test_collection"  # Configure as needed
            )
            
        except Exception as e:
            logger.error("Scheduled pipeline failed", error=str(e))
    
    def start(self):
        """Start the scheduler"""
        # Schedule for 8:00, 14:00, 20:00 daily
        self.scheduler.add_job(
            self.run_daily_pipeline,
            CronTrigger(hour=8, minute=7),
            id='daily_8am7',
            name='Daily Pipeline 8AM7'
        )
        
        self.scheduler.add_job(
            self.run_daily_pipeline,
            CronTrigger(hour=14, minute=0),
            id='daily_2pm',
            name='Daily Pipeline 2PM'
        )
        
        self.scheduler.add_job(
            self.run_daily_pipeline,
            CronTrigger(hour=20, minute=0),
            id='daily_8pm',
            name='Daily Pipeline 8PM'
        )
        
        self.scheduler.start()
        logger.info("Scheduler started - will run at 8:00, 14:00, 20:00 daily")
    
    def stop(self):
        """Stop the scheduler"""
        self.scheduler.shutdown()
        logger.info("Scheduler stopped")

async def main():
    """Main entry point"""
    scheduler = DailyScheduler()
    
    try:
        scheduler.start()
        logger.info("Daily scheduler is running...")
        
        # Keep the program running
        while True:
            await asyncio.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("Shutting down scheduler...")
        scheduler.stop()

if __name__ == "__main__":
    asyncio.run(main())