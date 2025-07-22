import asyncio
import logging
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from pipelines.video.video_pipeline import VideoPipelineProcessor
from pipelines.audio.audio_pipeline import AudioPipelineProcessor
from pipelines.data_sources import UnlabeledDataSource, OldVersionDataSource, MissUpsertDataSource, TaggedUnclassifiedDataSource
from config.config import (
    UNLABELED_CONFIG_VIDEO, UNLABELED_CONFIG_AUDIO,
    OLD_VERSION_CONFIG_VIDEO, OLD_VERSION_CONFIG_AUDIO,
    MISS_UPSERT_CONFIG_VIDEO, MISS_UPSERT_CONFIG_AUDIO,
    REAL_DETECTION_CONFIG
)

logger = logging.getLogger(__name__)

class AppScheduler:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.local_tz = timezone(timedelta(hours=7))  # UTC+7
        self.bangkok_tz = ZoneInfo("Asia/Bangkok")

    async def _run_unlabeled_video_pipeline(self):
        """Job for unlabeled video resources."""
        now_local = datetime.now(self.local_tz)
        index = (now_local.hour // 2) + 1
        if not (1 <= index <= 12):
            logger.warning(f"Unlabeled video job triggered at an odd hour {now_local.hour}, skipping.")
            return

        offset_end_days = 0
        offset_start_days = index * 50

        now_utc = now_local.astimezone(timezone.utc)
        end_date_utc = now_utc - timedelta(days=offset_end_days)
        start_date_utc = now_utc - timedelta(days=offset_start_days)

        formatted_start_date = start_date_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        formatted_end_date = end_date_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')

        logger.info(f"Starting unlabeled VIDEO pipeline job for window #{index}")
        processor_video = VideoPipelineProcessor(UNLABELED_CONFIG_VIDEO, UnlabeledDataSource())
        await processor_video.run(start_date=formatted_start_date, end_date=formatted_end_date)

    async def _run_unlabeled_audio_pipeline(self):
        """Job for unlabeled audio resources."""
        now_local = datetime.now(self.local_tz)
        index = (now_local.hour // 2) + 1
        if not (1 <= index <= 12):
            logger.warning(f"Unlabeled audio job triggered at an odd hour {now_local.hour}, skipping.")
            return

        offset_end_days = 0
        offset_start_days = index * 50

        now_utc = now_local.astimezone(timezone.utc)
        end_date_utc = now_utc - timedelta(days=offset_end_days)
        start_date_utc = now_utc - timedelta(days=offset_start_days)

        formatted_start_date = start_date_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        formatted_end_date = end_date_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')

        logger.info(f"Starting unlabeled AUDIO pipeline job for window #{index}")
        processor_audio = AudioPipelineProcessor(UNLABELED_CONFIG_AUDIO, UnlabeledDataSource())
        await processor_audio.run(start_date=formatted_start_date, end_date=formatted_end_date)

    async def _run_old_version_video_pipeline(self):
        """Job for old version video resources."""
        logger.info("Starting old version VIDEO pipeline job.")
        processor_video = VideoPipelineProcessor(OLD_VERSION_CONFIG_VIDEO, OldVersionDataSource())
        await processor_video.run()

    async def _run_old_version_audio_pipeline(self):
        """Job for old version audio resources."""
        logger.info("Starting old version AUDIO pipeline job.")
        processor_audio = AudioPipelineProcessor(OLD_VERSION_CONFIG_AUDIO, OldVersionDataSource())
        await processor_audio.run()

    async def _run_miss_upsert_video_pipeline(self):
        """Job for miss-upsert video resources."""
        now_local = datetime.now(self.local_tz)
        index = (now_local.hour // 2) + 1
        if not (1 <= index <= 12):
            logger.warning(f"Miss-upsert video job triggered at an odd hour {now_local.hour}, skipping.")
            return

        offset_end_days = 0
        offset_start_days = index * 30

        now_utc = now_local.astimezone(timezone.utc)
        end_date_utc = now_utc - timedelta(days=offset_end_days)
        start_date_utc = now_utc - timedelta(days=offset_start_days)

        formatted_start_date = start_date_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        formatted_end_date = end_date_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')

        logger.info(f"Starting miss-upsert VIDEO pipeline job for window #{index}")
        processor_video = VideoPipelineProcessor(MISS_UPSERT_CONFIG_VIDEO, MissUpsertDataSource())
        await processor_video.run(start_date=formatted_start_date, end_date=formatted_end_date)

    async def _run_miss_upsert_audio_pipeline(self):
        """Job for miss-upsert audio resources."""
        now_local = datetime.now(self.local_tz)
        index = (now_local.hour // 2) + 1
        if not (1 <= index <= 12):
            logger.warning(f"Miss-upsert audio job triggered at an odd hour {now_local.hour}, skipping.")
            return

        offset_end_days = 0
        offset_start_days = index * 30

        now_utc = now_local.astimezone(timezone.utc)
        end_date_utc = now_utc - timedelta(days=offset_end_days)
        start_date_utc = now_utc - timedelta(days=offset_start_days)

        formatted_start_date = start_date_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        formatted_end_date = end_date_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')

        logger.info(f"Starting miss-upsert AUDIO pipeline job for window #{index}")
        processor_audio = AudioPipelineProcessor(MISS_UPSERT_CONFIG_AUDIO, MissUpsertDataSource())
        await processor_audio.run(start_date=formatted_start_date, end_date=formatted_end_date)
        
    async def _run_detection_pipeline(self):
        """Job for detecting real/AI in video resources."""
        logger.info("Starting video detection pipeline job")
        processor = VideoPipelineProcessor(
            REAL_DETECTION_CONFIG, 
            TaggedUnclassifiedDataSource()
        )
        await processor.run_detection(batch_size=200)

    def start(self):
        """Add all jobs to the scheduler and start it."""
        # Video Jobs - chạy vào giờ chẵn
        self.scheduler.add_job(
            self._run_unlabeled_video_pipeline,
            CronTrigger(hour="0,2,4,6,8,10,12,14,16,18,20,22", minute=0, timezone=self.local_tz),
            id='unlabeled_video_pipeline_job',
            name='Unlabeled Video Pipeline on even hours (UTC+7)'
        )

        self.scheduler.add_job(
            self._run_old_version_video_pipeline,
            CronTrigger(hour="1,5,9,13,17,21", minute=0, timezone=self.bangkok_tz),
            id='old_version_video_pipeline_job',
            name='Old Version Video Pipeline every 4 hours (UTC+7)'
        )

        self.scheduler.add_job(
            self._run_miss_upsert_video_pipeline,
            CronTrigger(hour="0,2,4,6,8,10,12,14,16,18,20,22", minute=15, timezone=self.local_tz),
            id='miss_upsert_video_pipeline_job',
            name='Miss Upsert Video Pipeline on even hours (UTC+7)'
        )
        
        # Audio Jobs - chạy vào giờ lẻ
        self.scheduler.add_job(
            self._run_unlabeled_audio_pipeline,
            CronTrigger(hour="1,3,5,7,9,11,13,15,17,19,21,23", minute=0, timezone=self.local_tz),
            id='unlabeled_audio_pipeline_job',
            name='Unlabeled Audio Pipeline on odd hours (UTC+7)'
        )

        self.scheduler.add_job(
            self._run_old_version_audio_pipeline,
            CronTrigger(hour="3,7,11,15,19,23", minute=0, timezone=self.bangkok_tz),
            id='old_version_audio_pipeline_job',
            name='Old Version Audio Pipeline every 4 hours (UTC+7)'
        )

        self.scheduler.add_job(
            self._run_miss_upsert_audio_pipeline,
            CronTrigger(hour="1,3,5,7,9,11,13,15,17,19,21,23", minute=15, timezone=self.local_tz),
            id='miss_upsert_audio_pipeline_job',
            name='Miss Upsert Audio Pipeline on odd hours (UTC+7)'
        )
        
        # Detection Job - chạy mỗi ngày
        self.scheduler.add_job(
            self._run_detection_pipeline,
            CronTrigger(hour="2", minute=30, timezone=self.local_tz), 
            id='detection_pipeline_job',
            name='Video Detection Pipeline daily at 2:30 AM (UTC+7)'
        )

        self.scheduler.start()
        logger.info("Application scheduler started with all jobs.")

    def stop(self):
        """Stop the scheduler."""
        self.scheduler.shutdown()
        logger.info("Application scheduler stopped.")

    async def run_initial_jobs(self):
        """Run all jobs once at startup."""
        logger.info("Running initial jobs at startup...")
        await asyncio.gather(
            self._run_unlabeled_video_pipeline(),
            self._run_old_version_video_pipeline(),
            self._run_miss_upsert_video_pipeline(),
            self._run_detection_pipeline(),
            self._run_unlabeled_audio_pipeline(),
            self._run_old_version_audio_pipeline(),
            self._run_miss_upsert_audio_pipeline()
        )
