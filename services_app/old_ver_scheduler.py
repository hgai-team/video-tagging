import asyncio
import logging
from datetime import datetime, timedelta
import datetime as dt
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from old_ver_pipeline import OldVersionPipelineProcessor

logger = logging.getLogger(__name__)

class OldVersionScheduler:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.processor = OldVersionPipelineProcessor()

    async def run_old_version_pipeline(self):
        """Execute old version pipeline for outdated tag versions"""
        try:
            # Current tag version that we want to update from
            current_tag_version = "v3"
            
            logger.info(
                "Starting scheduled old version pipeline", 
                current_tag_version=current_tag_version
            )
            
            await self.processor.process_old_version_batch(
                current_tag_version=current_tag_version,
                media_type=2,  # Configure as needed
                collection_name="Jun05_old"
            )
        except Exception as e:
            logger.error("Scheduled old version pipeline failed", error=str(e))

    def start(self):
        """Start the scheduler"""
        # Schedule for 2:00, 8:00, 17:00 daily (different times from main scheduler)
        self.scheduler.add_job(
            self.run_old_version_pipeline, 
            CronTrigger(hour=15, minute=57), 
            id='old_version_2am', 
            name='Old Version Pipeline 2AM'
        )
        self.scheduler.add_job(
            self.run_old_version_pipeline, 
            CronTrigger(hour=8, minute=0), 
            id='old_version_8am', 
            name='Old Version Pipeline 8AM'
        )
        self.scheduler.add_job(
            self.run_old_version_pipeline, 
            CronTrigger(hour=17, minute=0), 
            id='old_version_5pm', 
            name='Old Version Pipeline 5PM'
        )
        self.scheduler.start()
        logger.info("Old Version Scheduler started - will run at 2:00, 8:00, 17:00 daily")

    def stop(self):
        """Stop the scheduler"""
        self.scheduler.shutdown()
        logger.info("Old Version Scheduler stopped")

async def main():
    """Main entry point"""
    scheduler = OldVersionScheduler()
    try:
        scheduler.start()
        logger.info("Old Version scheduler is running...")
        # Keep the program running while True
        while True:
            await asyncio.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down Old Version scheduler...")
        scheduler.stop()

if __name__ == "__main__":
    asyncio.run(main())