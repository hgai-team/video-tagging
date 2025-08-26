import asyncio
import logging
import argparse 
import seqlog
from schedulers.app_scheduler import AppScheduler
from config.config import LOG_LEVEL

import urllib3
from urllib3.exceptions import InsecureRequestWarning

urllib3.disable_warnings(InsecureRequestWarning)

logging.basicConfig(level=LOG_LEVEL)
seqlog.log_to_seq(
    server_url="http://localhost:5341",
    level=LOG_LEVEL,
    batch_size=20,
    auto_flush_timeout=2
)
logger = logging.getLogger(__name__)


def setup_arg_parser():
    """Sets up the command-line argument parser."""
    parser = argparse.ArgumentParser(description="Run specific pipeline or the full scheduler.")
    parser.add_argument(
        'pipeline',
        nargs='?',  # '?' means 0 or 1 argument
        type=int,
        choices=[1, 2, 3, 4, 5, 6, 7], # 7 pipeline options
        help="""Run a specific pipeline:
                1=Unlabeled Video,
                2=Unlabeled Audio,
                3=OldVersion Video,
                4=OldVersion Audio,
                5=MissUpsert Video,
                6=MissUpsert Audio,
                7=RealDetection Video.
                If no argument is provided, the full scheduler will run."""
    )
    return parser


async def main():
    """Main entry point for the application."""
    parser = setup_arg_parser()
    args = parser.parse_args()

    scheduler = AppScheduler()

    # If a pipeline argument is provided, run only that pipeline and exit
    if args.pipeline:
        logger.info(f"Running specified pipeline: {args.pipeline}")

        task_to_run = None
        
        # Handle each specific pipeline
        if args.pipeline == 1:
            task_to_run = scheduler._run_unlabeled_video_pipeline()
            logger.info("--> Running: Unlabeled Video Pipeline")
        elif args.pipeline == 2:
            task_to_run = scheduler._run_unlabeled_audio_pipeline()
            logger.info("--> Running: Unlabeled Audio Pipeline")
        elif args.pipeline == 3:
            task_to_run = scheduler._run_old_version_video_pipeline()
            logger.info("--> Running: Old Version Video Pipeline")
        elif args.pipeline == 4:
            task_to_run = scheduler._run_old_version_audio_pipeline()
            logger.info("--> Running: Old Version Audio Pipeline")
        elif args.pipeline == 5:
            task_to_run = scheduler._run_miss_upsert_video_pipeline()
            logger.info("--> Running: Miss Upsert Video Pipeline")
        elif args.pipeline == 6:
            task_to_run = scheduler._run_miss_upsert_audio_pipeline()
            logger.info("--> Running: Miss Upsert Audio Pipeline")
        elif args.pipeline == 7:
            task_to_run = scheduler._run_detection_pipeline()
            logger.info("--> Running: Real Detection Pipeline")

        if task_to_run:
            await task_to_run
            logger.info("Pipeline run has completed.")

        return

    logger.info("No specific pipeline requested. Starting full scheduler mode.")
    try:
        scheduler.start()

        # Run all jobs once at startup
        await scheduler.run_initial_jobs()

        # Keep the application running
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler...")
        scheduler.stop()
    except Exception as e:
        logger.critical("An unhandled exception occurred, shutting down.", exc_info=True)
        scheduler.stop()

if __name__ == "__main__":
    asyncio.run(main())