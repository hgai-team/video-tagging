import asyncio
import logging
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from pipelines.base_pipeline import GenericPipelineProcessor
from pipelines.data_sources import UnlabeledDataSource, OldVersionDataSource, MissUpsertDataSource, TaggedUnclassifiedDataSource
from config.config import UNLABELED_CONFIG, OLD_VERSION_CONFIG, MISS_UPSERT_CONFIG, REAL_DETECTION_CONFIG

logger = logging.getLogger(__name__)

class AppScheduler:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.local_tz = timezone(timedelta(hours=7)) # UTC+7
        self.bangkok_tz = ZoneInfo("Asia/Bangkok")

    async def _run_unlabeled_pipeline(self):
        """Job for unlabeled resources, runs every 2 hours."""
        now_local = datetime.now(self.local_tz)
        # Logic tính window giống hệt `miss_upsert` cũ, nên có thể tạo hàm dùng chung nếu cần
        # Ở đây tôi giữ nguyên để rõ ràng
        index = (now_local.hour // 2) + 1
        if not (1 <= index <= 12):
            logger.warning(f"Unlabeled job triggered at an odd hour {now_local.hour}, skipping.")
            return

        offset_end_days = 0# (index - 1) * 50
        offset_start_days = index * 50

        # Sửa lỗi logic: end_date nên là now_utc - offset_end_days
        now_utc = now_local.astimezone(timezone.utc)
        end_date_utc = now_utc - timedelta(days=offset_end_days)
        start_date_utc = now_utc - timedelta(days=offset_start_days)

        formatted_start_date = start_date_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        formatted_end_date = end_date_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')

        logger.info(f"Starting unlabeled pipeline job for window #{index}")
        processor = GenericPipelineProcessor(UNLABELED_CONFIG, UnlabeledDataSource())
        await processor.run(start_date=formatted_start_date, end_date=formatted_end_date)

    async def _run_old_version_pipeline(self):
        """Job for old version resources, runs on odd hours."""
        logger.info("Starting old version pipeline job.")
        processor = GenericPipelineProcessor(OLD_VERSION_CONFIG, OldVersionDataSource())
        await processor.run() # Không cần start/end date

    async def _run_miss_upsert_pipeline(self):
        """Job for miss-upsert resources, runs every 2 hours."""
        now_local = datetime.now(self.local_tz)
        index = (now_local.hour // 2) + 1
        if not (1 <= index <= 12):
            logger.warning(f"Miss-upsert job triggered at an odd hour {now_local.hour}, skipping.")
            return

        offset_end_days = 0# (index - 1) * 30
        offset_start_days = index * 30

        now_utc = now_local.astimezone(timezone.utc)
        end_date_utc = now_utc - timedelta(days=offset_end_days)
        start_date_utc = now_utc - timedelta(days=offset_start_days)

        formatted_start_date = start_date_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        formatted_end_date = end_date_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')

        logger.info(f"Starting miss-upsert pipeline job for window #{index}")
        processor = GenericPipelineProcessor(MISS_UPSERT_CONFIG, MissUpsertDataSource())
        await processor.run(start_date=formatted_start_date, end_date=formatted_end_date)
        
    async def _run_detection_pipeline(self):
        logger.info("Starting detection pipeline job")
        
        processor = GenericPipelineProcessor(
            REAL_DETECTION_CONFIG, 
            TaggedUnclassifiedDataSource()
        )
        
        await processor.run_detection(batch_size=2000)

    def start(self):
        """Add all jobs to the scheduler and start it."""
        # Job 1: Unlabeled (Chạy giống miss_upsert cũ)
        self.scheduler.add_job(
            self._run_unlabeled_pipeline,
            CronTrigger(hour="*/2", minute=0, timezone=self.local_tz),
            id='unlabeled_pipeline_job',
            name='Unlabeled Pipeline every 2 hours (UTC+7)'
        )

        # Job 2: Old Version
        self.scheduler.add_job(
            self._run_old_version_pipeline,
            CronTrigger(hour="1,3,7,9,11,13,15,17,19,21,23", minute=0, timezone=self.bangkok_tz),
            id='old_version_pipeline_job',
            name='Old Version Pipeline on odd hours (UTC+7)'
        )

        # Job 3: Miss Upsert (Copy lịch từ scheduler gốc)
        self.scheduler.add_job(
            self._run_miss_upsert_pipeline,
            CronTrigger(hour="*/2", minute=1, timezone=self.local_tz),
            id='miss_upsert_pipeline_job',
            name='Miss Upsert Pipeline every 2 hours (UTC+7)'
        )
        
        self.scheduler.add_job(
            self._run_detection_pipeline,
            CronTrigger(hour="*/24", minute=30, timezone=self.local_tz), 
            id='detection_pipeline_job',
            name='Real Detection Pipeline every 24 hours (UTC+7)'
        )

        self.scheduler.start()
        logger.info("Application scheduler started with all jobs.")

    def stop(self):
        self.scheduler.shutdown()
        logger.info("Application scheduler stopped.")

    async def run_initial_jobs(self):
        """Run all jobs once at startup."""
        logger.info("Running initial jobs at startup...")
        await asyncio.gather(
            self._run_unlabeled_pipeline(),
            self._run_old_version_pipeline(),
            self._run_miss_upsert_pipeline(),
            self._run_detection_pipeline()
        )
