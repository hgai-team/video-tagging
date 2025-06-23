# services_app/main.py
import asyncio
import logging
import argparse  # <-- Thêm thư viện này
import seqlog
from schedulers.app_scheduler import AppScheduler
from config import LOG_LEVEL

# Configure logging... (giữ nguyên)
logging.basicConfig(level=LOG_LEVEL)
seqlog.log_to_seq(
    server_url="http://localhost:5341",
    level=LOG_LEVEL,
    batch_size=10,
    auto_flush_timeout=2
)
logger = logging.getLogger(__name__)


def setup_arg_parser():
    """Thiết lập trình phân tích cú pháp tham số dòng lệnh."""
    parser = argparse.ArgumentParser(description="Run specific pipelines or the full scheduler.")
    parser.add_argument(
        'pipelines',
        nargs='*',  # '*' có nghĩa là 0 hoặc nhiều tham số
        type=int,
        choices=[1, 2, 3], # Chỉ chấp nhận các giá trị 1, 2, 3
        help="Run specific pipelines: 1=Unlabeled, 2=OldVersion, 3=MissUpsert. "
             "If no arguments are provided, the full scheduler will run."
    )
    return parser


async def main():
    """Main entry point for the application."""
    parser = setup_arg_parser()
    args = parser.parse_args()

    scheduler = AppScheduler()

    # Nếu có tham số pipeline được truyền vào, chỉ chạy các pipeline đó và thoát
    if args.pipelines:
        logger.info(f"Running specified pipelines based on arguments: {args.pipelines}")

        tasks_to_run = []
        if 1 in args.pipelines:
            tasks_to_run.append(scheduler._run_unlabeled_pipeline())
            logger.info("--> Queued: 1 (Unlabeled Pipeline)")
        if 2 in args.pipelines:
            tasks_to_run.append(scheduler._run_old_version_pipeline())
            logger.info("--> Queued: 2 (Old Version Pipeline)")
        if 3 in args.pipelines:
            tasks_to_run.append(scheduler._run_miss_upsert_pipeline())
            logger.info("--> Queued: 3 (Miss Upsert Pipeline)")

        # Chạy các pipeline đã chọn đồng thời
        if tasks_to_run:
            await asyncio.gather(*tasks_to_run)
            logger.info("Specified pipeline runs have completed.")

        # Thoát chương trình sau khi chạy xong
        return

    # Nếu không có tham số nào, chạy scheduler như bình thường
    logger.info("No specific pipelines requested. Starting full scheduler mode.")
    try:
        scheduler.start()

        # Chạy tất cả các job một lần khi khởi động
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
