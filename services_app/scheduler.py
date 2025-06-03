import asyncio
import logging
from datetime import datetime, timedelta
import datetime as dt
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from unlabled_pipeline import PipelineProcessor

logger = logging.getLogger(__name__)

class DailyScheduler:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.processor = PipelineProcessor()

    async def run_daily_pipeline(self):
        """Execute daily pipeline with current date range"""
        try:
            # Use current time in UTC with timezone-aware datetime
            end_date = dt.datetime.now(dt.timezone.utc)
            start_date = end_date - timedelta(days=5)
            
            # Format dates to match YYYY-MM-DDTHH:MM:SS.sssZ
            formatted_start_date = start_date.strftime('%Y-%m-%dT00:00:00.000Z')
            formatted_end_date = end_date.strftime('%Y-%m-%dT00:00:00.000Z')
            
            logger.info(
                "Starting scheduled pipeline", 
                start_date=formatted_start_date, 
                end_date=formatted_end_date
            )
            
            await self.processor.process_daily_batch(
                media_type=2,  # Configure as needed
                start_date=formatted_start_date,
                end_date=formatted_end_date,
                collection_name="test_collection"  # Configure as needed
            )
        except Exception as e:
            logger.error("Scheduled pipeline failed", error=str(e))

    def start(self):
        """Start the scheduler"""
        # Schedule for 8:00, 14:00, 20:00 daily
        self.scheduler.add_job(
            self.run_daily_pipeline, 
            CronTrigger(hour=11, minute=31), 
            id='daily_11am31', 
            name='Daily Pipeline 11AM31'
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
        # Keep the program running while True
        while True:
            await asyncio.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down scheduler...")
        scheduler.stop()

if __name__ == "__main__":
    asyncio.run(main())