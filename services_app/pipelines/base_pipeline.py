import asyncio
import logging
import os
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Any

import requests

# Các hàm giao tiếp với API bên ngoài
from api.router.stock_router import get_download_url, send_tags
from api.router.video_router import shrink_video, video_tagging, delete_video
from api.router.tags_router import upsert_points

# Các thành phần cốt lõi và tiện ích
from core import VideoProcessor
from db import task_tracker
from settings import get_settings
from .data_sources import BaseDataSource
from config import PipelineConfig  # Cấu hình cho pipeline

logger = logging.getLogger(__name__)


class GenericPipelineProcessor:
    """
    Một bộ xử lý pipeline chung, có thể cấu hình để chạy các loại batch job khác nhau.
    """
    def __init__(self, config: PipelineConfig, data_source: BaseDataSource):
        """
        Khởi tạo processor với cấu hình và nguồn dữ liệu cụ thể.

        Args:
            config (PipelineConfig): Đối tượng chứa các tham số cấu hình cho pipeline.
            data_source (BaseDataSource): Đối tượng cung cấp phương thức để lấy dữ liệu đầu vào.
        """
        self.config = config
        self.data_source = data_source

        # Semaphore để giới hạn số lượng video được xử lý đồng thời (shrink, tag)
        self.processing_semaphore = asyncio.Semaphore(config.max_concurrent_processing)

        # Đảm bảo thư mục tạm tồn tại
        self.config.temp_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"PipelineProcessor for '{config.name}' initialized with: "
            f"max_downloads={config.max_concurrent_downloads}, "
            f"max_processing={config.max_concurrent_processing}, "
            f"temp_dir='{config.temp_dir}'"
        )

    async def run(self, **kwargs: Any):
        """
        Điểm bắt đầu chính để thực thi một batch của pipeline.

        Args:
            **kwargs: Các tham số bổ sung sẽ được truyền cho data_source,
                      ví dụ: start_date, end_date.
        """
        batch_id = f"{self.config.name}_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting batch - batch_id={batch_id}")

        try:
            # Dọn dẹp các file cũ có thể bị kẹt lại từ lần chạy trước
            await self._batch_cleanup_temp_folder(batch_id)

            # Step 1: Lấy danh sách resource từ data_source đã được cung cấp
            logger.info(f"Step 1: Getting resources - batch_id={batch_id}")
            context = {
                "media_type": self.config.media_type,
                "collection_name": self.config.collection_name,
                "current_tag_version": self.config.tag_version,
                **kwargs
            }
            raw_resources = await self.data_source.get_resources(**context)

            if not raw_resources or not isinstance(raw_resources, list):
                logger.warning(f"No resources found from data source - batch_id={batch_id}")
                return

            # Chuẩn bị và sắp xếp resource theo thời lượng
            resources_with_duration = self._prepare_resources_by_duration(raw_resources, batch_id)
            if not resources_with_duration:
                logger.warning(f"No valid resources after filtering - batch_id={batch_id}")
                return

            logger.info(f"Found {len(resources_with_duration)} valid resources to process.")

            # Step 2: Lấy URL download cho các resource
            logger.info(f"Step 2: Getting download URLs - batch_id={batch_id}")
            resources_with_urls = await self._get_download_urls_batch(resources_with_duration, batch_id)

            # Step 3-8: Tải và xử lý video với cơ chế quản lý file tạm
            await self._process_with_file_management(resources_with_urls, batch_id)

            logger.info(f"Batch processing completed successfully - batch_id={batch_id}")

        except Exception as e:
            logger.error(f"Pipeline batch failed critically - batch_id={batch_id}, error={e}", exc_info=True)
            # Re-raise the exception to be handled by the scheduler if needed
            raise

    def _prepare_resources_by_duration(self, resource_data: List[Dict], batch_id: str) -> List[Tuple[str, int]]:
        """Lọc và sắp xếp các resource theo thời lượng (duration)."""
        resources = []
        for item in resource_data:
            resource_id = item.get('id')
            duration = item.get('duration', 0)
            if resource_id and duration > 0:
                resources.append((resource_id, duration))
            elif resource_id:
                logger.debug(f"Skipping resource with zero duration - batch_id={batch_id}, resource_id={resource_id}")

        resources.sort(key=lambda x: x[1]) # Sắp xếp từ thấp đến cao
        return resources

    async def _get_download_urls_batch(self, resources: List[Tuple[str, int]], batch_id: str) -> List[Dict]:
        """Lấy URL download cho một loạt resource một cách đồng thời."""
        logger.info(f"Starting to fetch {len(resources)} URLs concurrently. This may take a while...")
        tasks = [self._get_download_url_safe(res_id, dur, batch_id) for res_id, dur in resources]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_resources = []
        for i, result in enumerate(results):
            resource_id, duration = resources[i]
            if not isinstance(result, Exception):
                valid_resources.append({'resource_id': resource_id, 'duration': duration, 'url_data': result})

        logger.info(f"Got {len(valid_resources)} valid download URLs - batch_id={batch_id}")
        return valid_resources

    async def _process_with_file_management(self, resources: List[Dict], batch_id: str):
        """Quản lý việc tải và xử lý theo từng đợt để không làm đầy thư mục tạm."""
        remaining_resources = resources.copy()
        while remaining_resources:
            current_files = self._count_temp_files()
            available_slots = self.config.max_concurrent_downloads - current_files

            if available_slots <= 0:
                logger.warning(
                    f"Temp folder is full or at capacity ({current_files}/{self.config.max_concurrent_downloads}). "
                    f"Waiting for next run. Remaining resources: {len(remaining_resources)}."
                )
                break

            to_download = remaining_resources[:available_slots]
            remaining_resources = remaining_resources[available_slots:]

            logger.info(f"Downloading batch of {len(to_download)} files. Remaining: {len(remaining_resources)}.")
            downloaded_files = await self._download_batch(to_download, batch_id)

            if downloaded_files:
                await self._process_video_files(downloaded_files, batch_id)

    def _count_temp_files(self) -> int:
        """Đếm số file video trong thư mục tạm của pipeline này."""
        return len([f for f in self.config.temp_dir.iterdir() if f.is_file() and f.suffix == '.mp4'])

    async def _download_batch(self, resources: List[Dict], batch_id: str) -> List[Dict]:
        """Tải một loạt video. Sử dụng `asyncio.gather` để tải đồng thời."""
        tasks = [
            self._download_video_safe(
                res['resource_id'], res['url_data'], res['duration'], batch_id
            ) for res in resources
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_files = [res for res in results if not isinstance(res, Exception)]
        logger.info(f"Successfully downloaded {len(valid_files)}/{len(resources)} files for batch - batch_id={batch_id}")
        return valid_files

    async def _process_video_files(self, downloaded_files: List[Dict], batch_id: str):
        """Xử lý đồng thời một danh sách các file video đã được tải xuống."""
        logger.info(f"Processing {len(downloaded_files)} downloaded videos - batch_id={batch_id}")
        tasks = [self._process_single_video_concurrent(file_info, batch_id) for file_info in downloaded_files]
        await asyncio.gather(*tasks, return_exceptions=True) # return_exceptions để không dừng lại khi 1 task lỗi

    async def _process_single_video_concurrent(self, file_info: Dict, batch_id: str):
        """Wrapper để kiểm soát số lượng xử lý đồng thời bằng semaphore."""
        resource_id = file_info['resource_id']
        async with self.processing_semaphore:
            logger.info(f"Acquired semaphore for processing - resource_id={resource_id}")
            await self._process_single_video(file_info, batch_id)
            logger.info(f"Released semaphore for processing - resource_id={resource_id}")

    # --- Các bước xử lý chi tiết cho một video ---

    async def _get_download_url_safe(self, resource_id: str, duration: int, batch_id: str) -> Dict:
        """Lấy URL download một cách an toàn."""
        try:
            result = await get_download_url(id=resource_id)

            logger.info(f"Successfully fetched URL for resource_id: {resource_id}")

            return result
        except Exception as e:
            logger.error(f"Get download URL failed - batch_id={batch_id}, resource_id={resource_id}, error={e}")
            raise

    async def _download_video_safe(self, resource_id: str, url_data: Dict, duration: int, batch_id: str) -> Dict:
        """Tải một video, kiểm tra kích thước, và ghi nhận vào task_tracker."""
        task_tracker.start_task(resource_id, batch_id)

        # Tìm URL trong các key phổ biến
        url_fields = ['url', 'download_url', 'downloadUrl', 'link', 'videoUrl']
        download_url = next((url_data[f] for f in url_fields if f in url_data and url_data[f]), None)

        if not download_url:
            task_tracker.mark_failed(resource_id, batch_id)
            raise ValueError(f"No download URL found. Available fields: {list(url_data.keys())}")

        output_path = self.config.temp_dir / f"{resource_id}.mp4"

        try:
            async with VideoProcessor() as processor:
                await processor.download_video(url=download_url, output_path=str(output_path))

            if not output_path.exists():
                raise FileNotFoundError(f"Downloaded file not found: {output_path}")

            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            if file_size_mb < self.config.min_video_size_mb:
                logger.warning(f"Video file too small ({file_size_mb:.2f}MB), skipping - resource_id={resource_id}")
                output_path.unlink() # Xóa file nhỏ
                task_tracker.mark_failed(resource_id, batch_id) # Đánh dấu là lỗi
                raise ValueError("Video file size too small")

            logger.info(f"Download completed - resource_id={resource_id}, size={file_size_mb:.2f}MB")
            return {'resource_id': resource_id, 'duration': duration, 'original_path': str(output_path)}

        except Exception as e:
            task_tracker.mark_failed(resource_id, batch_id)
            logger.error(f"Download video failed - resource_id={resource_id}, error={e}", exc_info=True)
            if output_path.exists():
                output_path.unlink() # Dọn dẹp nếu có lỗi
            raise

    async def _process_single_video(self, file_info: Dict, batch_id: str):
        """Thực hiện chuỗi xử lý cho một video: shrink -> tag -> upsert/send -> cleanup."""
        resource_id = file_info['resource_id']
        original_path = file_info['original_path']
        processed_path = str(self.config.temp_dir / f"{resource_id}_processed.mp4")
        cleanup_files = [('original', original_path), ('processed', processed_path)]

        try:
            logger.info(f"Starting full processing pipeline for resource_id={resource_id}")

            # Step 4: Shrink video
            await shrink_video(input_path=original_path, output_path=processed_path)
            logger.info(f"Step 4 (Shrink) completed - resource_id={resource_id}")

            # Step 5: Video tagging
            tagging_result = await video_tagging(input_path=processed_path)
            tags_data = tagging_result.get('data', {})
            logger.info(f"Step 5 (Tagging) completed - resource_id={resource_id}")

            tasks = []
            # Steps 6 & 7: Chạy song song các tác vụ gửi dữ liệu
            # upsert_task = self._upsert_points_safe(tags_data, resource_id, batch_id)
            tasks.append(self._send_tags_safe(resource_id, tags_data, batch_id))
            # Giả sử đây là một hệ thống vector khác cần cập nhật
            tasks.append(self._upsert_stock_vector(tags_data, resource_id, batch_id))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Kiểm tra kết quả của các tác vụ song song
            failed_tasks = [res for res in results if isinstance(res, Exception)]
            if failed_tasks:
                raise RuntimeError(f"One or more post-processing steps failed: {failed_tasks}")

            logger.info(f"Steps 6&7 (Upsert/Send) completed - resource_id={resource_id}")
            task_tracker.mark_success(resource_id, batch_id)
            logger.info(f"Successfully processed resource_id={resource_id}")

        except Exception as e:
            task_tracker.mark_failed(resource_id, batch_id)
            logger.error(f"Video processing pipeline failed - resource_id={resource_id}, error={e}", exc_info=True)
            # Không re-raise để các video khác trong batch vẫn được xử lý
        finally:
            # Step 8: Luôn luôn dọn dẹp file
            await self._cleanup_video_files(cleanup_files, resource_id, batch_id)

    async def _upsert_points_safe(self, tags_data: Dict, resource_id: str, batch_id: str):
        """Gửi vector lên Milvus/Qdrant một cách an toàn."""
        try:
            await upsert_points(
                collection_name=self.config.collection_name,
                points=[tags_data],
                ids=[resource_id]
            )
        except Exception as e:
            logger.error(f"Upsert points failed - batch_id={batch_id}, resource_id={resource_id}", exc_info=True)
            raise

    async def _send_tags_safe(self, resource_id: str, tags_data: Dict, batch_id: str):
        """Gửi tags về hệ thống chính một cách an toàn."""
        try:
            await send_tags(
                id=resource_id,
                tags=tags_data,
                tag_version=self.config.tag_version
            )
        except Exception as e:
            logger.error(f"Send tags failed - batch_id={batch_id}, resource_id={resource_id}", exc_info=True)
            raise

    async def _upsert_stock_vector(self, points: Dict, resource_id: str, batch_id: str, max_retries: int = 5):
        """Gửi vector với logic retry, đã khắc phục vòng lặp vô hạn."""
        settings = get_settings()
        url = f"{settings.TAG_DOMAIN}af/collections/{self.config.collection_name}/points"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        data = {"points": [points], "ids": [resource_id]}

        for attempt in range(max_retries):
            try:
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: requests.put(url=url, headers=headers, json=data, timeout=60)
                )
                response.raise_for_status() # Raise HTTPError cho các mã lỗi 4xx/5xx
                logger.info(f"Upsert stock vector success - resource_id={resource_id}")
                return response.json()
            except requests.RequestException as e:
                logger.warning(f"Upsert stock vector attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt) # Chờ 1, 2, 4 giây...

        raise RuntimeError(f"Upsert stock vector failed after {max_retries} retries for resource {resource_id}")

    # --- Các hàm dọn dẹp ---

    async def _batch_cleanup_temp_folder(self, batch_id: str):
        """Dọn dẹp các file cũ hơn 1 giờ trong thư mục tạm."""
        try:
            old_files = []
            current_time = datetime.now().timestamp()
            for file_path in self.config.temp_dir.glob("*.mp4"):
                if file_path.is_file() and (current_time - file_path.stat().st_mtime > 3600):
                    old_files.append(file_path)

            if old_files:
                logger.warning(f"Found {len(old_files)} old temp files, cleaning up - batch_id={batch_id}")
                for file_path in old_files:
                    try:
                        file_path.unlink()
                        logger.info(f"Cleaned old file: {file_path}")
                    except OSError as e:
                        logger.error(f"Failed to clean old file {file_path}: {e}")
        except Exception as e:
            logger.error(f"Batch temp cleanup failed - batch_id={batch_id}, error={e}")

    async def _cleanup_video_files(self, cleanup_files: List[Tuple[str, str]], resource_id: str, batch_id: str):
        """Dọn dẹp tất cả các file liên quan đến một video (gốc và đã xử lý)."""
        logger.info(f"Step 8: Cleaning up files for resource_id={resource_id}")
        for file_type, file_path_str in cleanup_files:
            if file_path_str:
                file_path = Path(file_path_str)
                if file_path.exists():
                    try:
                        await delete_video(input_path=str(file_path))
                        logger.info(f"Deleted {file_type} file successfully: {file_path}")
                    except Exception as e:
                        logger.error(f"Failed to delete {file_type} file {file_path}: {e}")
