import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Any

import requests

from api.router.stock_router import get_download_url, send_tags
from api.router.video_router import shrink_video, video_tagging, delete_video
from api.router.tags_router import upsert_points
from .api_helpers import update_point_metadata_via_payload_api, update_multiple_points_metadata

from core import VideoProcessor
from db import task_tracker
from config.settings import get_settings
from .data_sources import BaseDataSource
from config.config import PipelineConfig 

logger = logging.getLogger(__name__)

async def get_video_duration(file_path: str) -> float:
    try:
        
        cmd = [
            'ffprobe', 
            '-v', 'error', 
            '-show_entries', 'format=duration', 
            '-of', 'default=noprint_wrappers=1:nokey=1', 
            file_path
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            logger.error(f"ffprobe failed: {stderr.decode()}")
            return 0
        
        duration_str = stdout.decode().strip()
        try:
            duration = float(duration_str)
            return int(duration)  
        except ValueError:
            logger.error(f"Failed to parse duration: {duration_str}")
            return 0
            
    except Exception as e:
        logger.error(f"Error getting video duration: {str(e)}")
        return 0

class GenericPipelineProcessor:
    """
    Một bộ xử lý pipeline chung, có thể cấu hình để chạy các loại batch job khác nhau.
    """
    def __init__(self, config: PipelineConfig, data_source: BaseDataSource):
        self.config = config
        self.data_source = data_source

        # Semaphore để giới hạn số lượng video được xử lý đồng thời (shrink, tag)
        self.processing_semaphore = asyncio.Semaphore(config.max_concurrent_processing)

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
                    f"Tiến hành xóa toàn bộ file để giải phóng không gian."
                )

                temp_files = []
                for file_path in self.config.temp_dir.glob("*.mp4"):
                    if file_path.is_file():
                        temp_files.append(('temp', str(file_path)))
                
                if temp_files:
                    await self._cleanup_video_files(temp_files, f"batch_{batch_id}")
                
                current_files = self._count_temp_files()
                available_slots = self.config.max_concurrent_downloads - current_files
                logger.info(f"Sau khi dọn dẹp, thư mục tạm còn {current_files} files, {available_slots} slots trống.")
                
                # Nếu vẫn không có slot trống sau khi xóa, có thể có vấn đề với quyền truy cập file
                if available_slots <= 0:
                    logger.error(f"Thư mục tạm vẫn đầy sau khi dọn dẹp. Còn {len(remaining_resources)} resources chưa xử lý.")
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

        url_fields = ['url', 'download_url', 'downloadUrl', 'link', 'videoUrl']
        download_url = next((url_data.get('data', {}).get(f) for f in url_fields if f in url_data.get('data', {})), None)
        
        if not download_url:
            # Thử tìm trong url_data trực tiếp nếu không có key 'data'
            download_url = next((url_data.get(f) for f in url_fields if f in url_data), None)
        
        if not download_url:
            task_tracker.mark_failed(resource_id, batch_id)
            logger.error(f"No download URL found for {resource_id}. Available fields: {list(url_data.keys() if isinstance(url_data, dict) else [])}")
            if 'data' in url_data and isinstance(url_data['data'], dict):
                logger.error(f"Data fields: {list(url_data['data'].keys())}")
            raise ValueError(f"No download URL found for {resource_id}")

        logger.info(f"Found download URL for {resource_id}: {download_url[:50]}...")
        output_path = self.config.temp_dir / f"{resource_id}.mp4"

        try:
            async with VideoProcessor() as processor:
                logger.info(f"Starting download for {resource_id} to {output_path}")
                result = await processor.download_video(url=download_url, output_path=str(output_path))
                
                if not result:
                    raise RuntimeError(f"Download failed with VideoProcessor result: {result}")

            if not output_path.exists():
                raise FileNotFoundError(f"Downloaded file not found: {output_path}")

            # Lấy kích thước file
            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"Downloaded file size for {resource_id}: {file_size_mb:.2f}MB")
            
            # Kiểm tra kích thước tối thiểu
            if file_size_mb < self.config.min_video_size_mb:
                logger.warning(f"Video file too small ({file_size_mb:.2f}MB), skipping - resource_id={resource_id}")
                output_path.unlink() # Xóa file nhỏ
                task_tracker.mark_failed(resource_id, batch_id) # Đánh dấu là lỗi
                raise ValueError(f"Video file size too small: {file_size_mb:.2f}MB < {self.config.min_video_size_mb}MB")
            
            # Lấy thời lượng video từ file đã tải
            real_duration = await get_video_duration(str(output_path))
            logger.info(f"Got video duration for {resource_id}: {real_duration}s")

            logger.info(f"Download completed - resource_id={resource_id}, size={file_size_mb:.2f}MB, duration={real_duration}s")
            return {
                'resource_id': resource_id, 
                'duration': real_duration,  # Sử dụng thời lượng thực tế thay vì giá trị từ DB
                'original_path': str(output_path)
            }

        except Exception as e:
            task_tracker.mark_failed(resource_id, batch_id)
            logger.error(f"Download video failed - resource_id={resource_id}, error={e}", exc_info=True)
            if output_path.exists():
                try:
                    output_path.unlink() 
                except Exception as del_err:
                    logger.error(f"Failed to delete partial file {output_path}: {del_err}")
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

    async def _cleanup_video_files(self, cleanup_files: List[Tuple[str, str]], resource_id: str):
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
            
    # ============ Dectection Function ============        
                            
    async def _process_detection_video(self, file_info: Dict, batch_id: str):
        """Thực hiện chuỗi xử lý cho một video: shrink -> detect -> update -> cleanup."""
        resource_id = file_info['resource_id']
        original_path = file_info['original_path']
        duration = file_info['duration'] 
        processed_path = str(self.config.temp_dir / f"{resource_id}_processed.mp4")
        cleanup_files = [('original', original_path), ('processed', processed_path)]

        try:
            logger.info(f"Starting detection pipeline for resource_id={resource_id}")

            # Step 4: Shrink video
            await shrink_video(input_path=original_path, output_path=processed_path)
            logger.info(f"Step 4 (Shrink) completed - resource_id={resource_id}")

            # Step 5: Video detection
            from api.router.video_router import detect_real_or_ai
            detection_result = await detect_real_or_ai(input_path=processed_path)
            detection_data = detection_result.get('data', {})
            is_real = detection_data.get('is_real', 0)
            logger.info(f"Step 5 (Detection) completed - resource_id={resource_id}, is_real={is_real}")

            # Step 6: Update vector với is_real và time (từ duration thực tế) qua API mới
            update_result = await update_point_metadata_via_payload_api(
                collection_name=self.config.collection_name,
                point_id=resource_id,
                is_real=is_real,
                time=duration  # Sử dụng duration đã lấy từ file video
            )
            
            if not update_result.get('success', False):
                raise RuntimeError(f"Failed to update metadata: {update_result.get('error')}")
                
            logger.info(f"Step 6 (Update) completed - resource_id={resource_id}, is_real={is_real}, time={duration}")
            task_tracker.mark_success(resource_id, batch_id)
            logger.info(f"Successfully processed resource_id={resource_id}")

        except Exception as e:
            task_tracker.mark_failed(resource_id, batch_id)
            logger.error(f"Video detection pipeline failed - resource_id={resource_id}, error={e}", exc_info=True)
        finally:
            # Step 7: Luôn luôn dọn dẹp file
            await self._cleanup_video_files(cleanup_files, resource_id, batch_id)
                        
    async def _process_detection_files(self, downloaded_files: List[Dict], batch_id: str):
        """Xử lý đồng thời một danh sách các file video đã được tải xuống cho quá trình phát hiện."""
        logger.info(f"Processing {len(downloaded_files)} downloaded videos for detection - batch_id={batch_id}")
        tasks = [self._process_detection_video_concurrent(file_info, batch_id) for file_info in downloaded_files]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_detection_video_concurrent(self, file_info: Dict, batch_id: str):
        """Wrapper để kiểm soát số lượng xử lý đồng thời bằng semaphore."""
        resource_id = file_info['resource_id']
        async with self.processing_semaphore:
            logger.info(f"Acquired semaphore for detection - resource_id={resource_id}")
            await self._process_detection_video(file_info, batch_id)
            logger.info(f"Released semaphore for detection - resource_id={resource_id}")
            
    async def run_detection(self, **kwargs: Any):
        """
        Điểm bắt đầu chính để thực thi một batch của pipeline detection.
        Xử lý toàn bộ points trong collection bằng cách lặp qua tất cả các batch.
        
        Args:
            **kwargs: Các tham số bổ sung, bao gồm collection_name, batch_size.
        """
        batch_id = f"{self.config.name}_detection_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting detection batch - batch_id={batch_id}")

        try:
            # Dọn dẹp các file cũ có thể bị kẹt lại từ lần chạy trước
            await self._batch_cleanup_temp_folder(batch_id)

            # Thiết lập các tham số cơ bản
            collection_name = kwargs.get('collection_name', self.config.collection_name)
            batch_size = kwargs.get('batch_size', 100)
            offset = 0
            total_processed = 0
            
            # Xử lý lần lượt từng batch cho đến khi hết
            while True:
                # Step 1: Lấy danh sách resource từ data_source với offset hiện tại
                logger.info(f"Getting batch at offset={offset} with size={batch_size} - batch_id={batch_id}")
                context = {
                    "collection_name": collection_name,
                    "batch_size": batch_size,
                    "offset": offset
                }
                raw_resources = await self.data_source.get_resources(**context)

                # Kiểm tra xem còn data không - nếu không có thì dừng ngay
                if not raw_resources or not isinstance(raw_resources, list) or len(raw_resources) == 0:
                    logger.info(f"No more resources found at offset={offset} - batch_id={batch_id}")
                    break

                # Chuyển đổi resources
                resources = []
                for res in raw_resources:
                    resource_id = res.get('id')
                    duration = res.get('duration', 0)
                    if resource_id:
                        resources.append((resource_id, duration))

                if not resources:
                    logger.warning(f"No valid resources after conversion at offset={offset} - batch_id={batch_id}")
                    offset += batch_size
                    continue

                # Step 2: Lấy URL download cho các resource
                logger.info(f"Getting download URLs for {len(resources)} resources at offset={offset} - batch_id={batch_id}")
                resources_with_urls = await self._get_download_urls_batch(resources, batch_id)

                # Step 3-7: Tải và xử lý video
                await self._process_with_file_management_detection(resources_with_urls, batch_id)
                
                # Cập nhật số lượng đã xử lý và offset cho batch tiếp theo
                total_processed += len(resources)
                offset += batch_size
                
                logger.info(f"Processed batch at offset={offset-batch_size}, total processed so far: {total_processed} - batch_id={batch_id}")
                
                # Nếu số lượng resources nhỏ hơn batch_size, đây là batch cuối cùng
                if len(resources) < batch_size:
                    logger.info(f"Found {len(resources)} resources < batch_size {batch_size}, reached end of collection - batch_id={batch_id}")
                    break

            logger.info(f"Detection pipeline completed for all resources, total processed: {total_processed} - batch_id={batch_id}")

        except Exception as e:
            logger.error(f"Detection pipeline batch failed critically - batch_id={batch_id}, error={e}", exc_info=True)
            raise
        
    async def _process_detection_files_batch(self, downloaded_files: List[Dict], batch_id: str):
        """
        Xử lý đồng thời một danh sách các file video cho detection với cập nhật hàng loạt.
        Tối ưu hóa bằng cách cập nhật nhiều points cùng lúc sau khi xử lý xong.
        """
        logger.info(f"Processing {len(downloaded_files)} downloaded videos for detection - batch_id={batch_id}")
        
        # Danh sách để lưu các kết quả xử lý
        detection_results = []
        processed_files = []
        
        # Step 1: Shrink và detect tất cả video
        for file_info in downloaded_files:
            resource_id = file_info['resource_id']
            original_path = file_info['original_path']
            duration = file_info['duration']
            processed_path = str(self.config.temp_dir / f"{resource_id}_processed.mp4")
            
            try:
                # Shrink video
                await shrink_video(input_path=original_path, output_path=processed_path)
                logger.info(f"Shrink completed - resource_id={resource_id}")
                
                # Detection
                from api.router.video_router import detect_real_or_ai
                detection_result = await detect_real_or_ai(input_path=processed_path)
                detection_data = detection_result.get('data', {})
                is_real = detection_data.get('is_real', 0)
                logger.info(f"Detection completed - resource_id={resource_id}, is_real={is_real}")
                
                # Lưu kết quả
                detection_results.append({
                    'resource_id': resource_id,
                    'is_real': is_real,
                    'duration': duration
                })
                
                # Lưu file đã xử lý để dọn dẹp sau
                processed_files.append({
                    'resource_id': resource_id,
                    'original_path': original_path,
                    'processed_path': processed_path
                })
                
            except Exception as e:
                logger.error(f"Processing failed for resource_id={resource_id}: {str(e)}")
                task_tracker.mark_failed(resource_id, batch_id)
        
        # Step 2: Cập nhật metadata cho tất cả points cùng lúc
        if detection_results:
            try:
                # Tạo batch theo is_real để cập nhật cùng lúc
                is_real_batches = {}
                for result in detection_results:
                    is_real = result['is_real']
                    if is_real not in is_real_batches:
                        is_real_batches[is_real] = []
                    is_real_batches[is_real].append(result)
                
                # Cập nhật từng batch theo is_real
                for is_real, batch in is_real_batches.items():
                    point_ids = [item['resource_id'] for item in batch]
                    logger.info(f"Updating {len(point_ids)} points with is_real={is_real}")
                    
                    update_result = await update_multiple_points_metadata(
                        collection_name=self.config.collection_name,
                        point_ids=point_ids,
                        is_real=is_real
                    )
                    
                    if update_result.get('success', False):
                        logger.info(f"Batch update successful for {len(point_ids)} points with is_real={is_real}")
                        # Đánh dấu thành công cho tất cả points trong batch
                        for item in batch:
                            task_tracker.mark_success(item['resource_id'], batch_id)
                    else:
                        logger.error(f"Batch update failed: {update_result.get('error')}")
                        # Đánh dấu thất bại cho tất cả points trong batch
                        for item in batch:
                            task_tracker.mark_failed(item['resource_id'], batch_id)
                
                # Cập nhật time cho từng point riêng lẻ vì time khác nhau
                for result in detection_results:
                    resource_id = result['resource_id']
                    duration = result['duration']
                    
                    # Chỉ cập nhật time
                    await update_point_metadata_via_payload_api(
                        collection_name=self.config.collection_name,
                        point_id=resource_id,
                        time=duration
                    )
                    logger.info(f"Updated time={duration} for resource_id={resource_id}")
                    
            except Exception as e:
                logger.error(f"Batch update failed: {str(e)}")
        
        # Step 3: Dọn dẹp tất cả files
        for file_info in processed_files:
            resource_id = file_info['resource_id']
            cleanup_files = [
                ('original', file_info['original_path']), 
                ('processed', file_info['processed_path'])
            ]
            await self._cleanup_video_files(cleanup_files, resource_id, batch_id)
        
        logger.info(f"Completed processing {len(downloaded_files)} videos for detection - batch_id={batch_id}")

    # Cập nhật phương thức _process_with_file_management_detection để sử dụng xử lý hàng loạt
    async def _process_with_file_management_detection(self, resources: List[Dict], batch_id: str):
        """Quản lý việc tải và xử lý theo từng đợt cho detection pipeline."""
        remaining_resources = resources.copy()
        
        # Thêm tham số để kiểm soát xử lý hàng loạt
        use_batch_processing = True  # Đặt True để sử dụng xử lý hàng loạt, False để xử lý tuần tự
        
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

            logger.info(f"Downloading batch of {len(to_download)} files for detection. Remaining: {len(remaining_resources)}.")
            downloaded_files = await self._download_batch(to_download, batch_id)

            if downloaded_files:
                if use_batch_processing:
                    # Xử lý hàng loạt - cập nhật nhiều points cùng lúc
                    await self._process_detection_files_batch(downloaded_files, batch_id)
                else:
                    # Xử lý tuần tự - từng video một
                    await self._process_detection_files(downloaded_files, batch_id)