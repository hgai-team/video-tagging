import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Any
from datetime import datetime

from api.router.stock_router import send_tags
from api.router.video_router import shrink_video, video_tagging, delete_video, detect_real_or_ai
from api.router.tags_router import upsert_points
from .api_helpers import update_point_metadata_via_payload_api, update_multiple_points_metadata
from core import video_pro
from pipelines.base_pipeline import BasePipelineProcessor
from utils.db import task_tracker

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

class VideoPipelineProcessor(BasePipelineProcessor):

    def _count_temp_files(self) -> int:
        return len([f for f in self.config.temp_dir.iterdir() if f.is_file() and f.suffix == '.mp4'])
    
    def _get_file_patterns(self) -> List[str]:
        return ["*.mp4"]
        
    def _get_temp_files_list(self) -> List[Tuple[str, str]]:
        temp_files = []
        for file_path in self.config.temp_dir.glob("*.mp4"):
            if file_path.is_file():
                temp_files.append(('temp', str(file_path)))
        return temp_files
    
    async def _download_media_safe(self, resource_id: str, url_data: Dict, duration: int, batch_id: str) -> Dict:
        task_tracker.start_task(resource_id, batch_id)

        url_fields = ['url', 'download_url', 'downloadUrl', 'link', 'videoUrl']
        download_url = next((url_data.get('data', {}).get(f) for f in url_fields if f in url_data.get('data', {})), None)
        
        if not download_url:
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
            logger.info(f"Starting download for {resource_id} to {output_path}")
            result = await video_pro.download_video(url=download_url, output_path=str(output_path))
            
            if not result:
                raise RuntimeError(f"Download failed with VideoProcessor result: {result}")

            if not output_path.exists():
                raise FileNotFoundError(f"Downloaded file not found: {output_path}")

            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"Downloaded file size for {resource_id}: {file_size_mb:.2f}MB")
            
            real_duration = await get_video_duration(str(output_path))
            logger.info(f"Got video duration for {resource_id}: {real_duration}s")

            logger.info(f"Download completed - resource_id={resource_id}, size={file_size_mb:.2f}MB, duration={real_duration}s")
            return {
                'resource_id': resource_id, 
                'duration': real_duration,
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

    async def _process_media(self, file_info: Dict, batch_id: str):
        """Thực hiện chuỗi xử lý cho một video: shrink -> tag -> detect -> upsert/send -> cleanup."""
        resource_id = file_info['resource_id']
        original_path = file_info['original_path']
        duration = file_info['duration']
        processed_path = str(self.config.temp_dir / f"{resource_id}_processed.mp4")
        cleanup_files = [('original', original_path), ('processed', processed_path)]

        try:
            logger.info(f"Bắt đầu luồng xử lý tích hợp cho resource_id={resource_id}")

            # Step 4: Shrink video
            await shrink_video(input_path=original_path, output_path=processed_path)
            logger.info(f"Step 4 (Shrink) hoàn thành - resource_id={resource_id}")

            # Step 5: Video tagging
            tagging_result = await video_tagging(input_path=processed_path)
            if not tagging_result or not tagging_result.get('success', False):
                raise RuntimeError(f"Tagging thất bại - resource_id={resource_id}")
            
            tags_data = tagging_result.get('data', {})
            logger.info(f"Step 5 (Tagging) hoàn thành - resource_id={resource_id}")
            
            # Step 6: Video detection
            detection_result = await detect_real_or_ai(input_path=processed_path)
            if not detection_result or not detection_result.get('success', False):
                raise RuntimeError(f"Detection thất bại - resource_id={resource_id}")
            
            detection_data = detection_result.get('data', {})
            is_real = detection_data.get('is_real', 0)
            logger.info(f"Step 6 (Detection) hoàn thành - resource_id={resource_id}, is_real={is_real}")
            
            tags_data["is_real"] = str(is_real)
            tags_data["time"] = duration
            
            # Steps 7 & 8: Chạy song song các tác vụ gửi dữ liệu
            tasks = []
            tasks.append(send_tags(id=resource_id, tags=tags_data, tag_version=self.config.tag_version))
            tasks.append(upsert_points(collection_name=self.config.collection_name, points=[tags_data], ids=[resource_id], media_type="video"))
            tasks.append(self._upsert_stock_vector(tags_data, resource_id, media_type="video"))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Kiểm tra kết quả của các tác vụ song song
            failed_tasks = [res for res in results if isinstance(res, Exception)]
            if failed_tasks:
                raise RuntimeError(f"One or more post-processing steps failed: {failed_tasks}")

            logger.info(f"Steps 7&8 (Upsert/Send) hoàn thành - resource_id={resource_id}")
            task_tracker.mark_success(resource_id, batch_id)
            logger.info(f"Xử lý thành công resource_id={resource_id}")

        except Exception as e:
            task_tracker.mark_failed(resource_id, batch_id)
            logger.error(f"Pipeline xử lý thất bại - resource_id={resource_id}, error={e}", exc_info=True)
        finally:
            # Step 9: Luôn luôn dọn dẹp file
            await self._cleanup_media_files(cleanup_files, resource_id, batch_id)

    async def _cleanup_media_files(self, cleanup_files: List[Tuple[str, str]], resource_id: str, batch_id: str = None):
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
                            
    async def run_detection(self, **kwargs: Any):

        batch_id = f"{self.config.name}_detection_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting detection batch - batch_id={batch_id}")

        try:
            await self._batch_cleanup_temp_folder(batch_id)

            collection_name = kwargs.get('collection_name', self.config.collection_name)
            batch_size = kwargs.get('batch_size', 100)
            offset = 0
            total_processed = 0
            
            while True:
                logger.info(f"Getting batch at offset={offset} with size={batch_size} - batch_id={batch_id}")
                context = {
                    "collection_name": collection_name,
                    "batch_size": batch_size,
                    "offset": offset
                }
                raw_resources = await self.data_source.get_resources(**context)

                if not raw_resources or not isinstance(raw_resources, list) or len(raw_resources) == 0:
                    logger.info(f"No more resources found at offset={offset} - batch_id={batch_id}")
                    break

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

                logger.info(f"Getting download URLs for {len(resources)} resources at offset={offset} - batch_id={batch_id}")
                resources_with_urls = await self._get_download_urls_batch(resources, batch_id)

                await self._process_with_file_management_detection(resources_with_urls, batch_id)
                
                total_processed += len(resources)
                offset += batch_size
                
                logger.info(f"Processed batch at offset={offset-batch_size}, total processed so far: {total_processed} - batch_id={batch_id}")
                
                if len(resources) < batch_size:
                    logger.info(f"Found {len(resources)} resources < batch_size {batch_size}, reached end of collection - batch_id={batch_id}")
                    break

            logger.info(f"Detection pipeline completed for all resources, total processed: {total_processed} - batch_id={batch_id}")

        except Exception as e:
            logger.error(f"Detection pipeline batch failed critically - batch_id={batch_id}, error={e}", exc_info=True)
            raise
            
    async def _process_detection_files_batch(self, downloaded_files: List[Dict], batch_id: str):
        logger.info(f"Processing {len(downloaded_files)} downloaded videos for detection - batch_id={batch_id}")
        
        # Step 1: Xử lý tất cả video BẤT ĐỒNG BỘ
        tasks = [self._process_single_detection_video_concurrent(file_info, batch_id) 
                for file_info in downloaded_files]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Lọc các kết quả thành công
        successful_results = [r for r in results if not isinstance(r, Exception) and r.get('success', False)]
        
        # Step 2: Cập nhật metadata cho tất cả points cùng lúc
        if successful_results:
            try:
                # Nhóm các video theo is_real để cập nhật hàng loạt
                is_real_batches = {}
                for result in successful_results:
                    is_real = result['is_real']
                    if is_real not in is_real_batches:
                        is_real_batches[is_real] = []
                    is_real_batches[is_real].append(result)

                # Cập nhật từng nhóm
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
                        for item in batch:
                            task_tracker.mark_success(item['resource_id'], batch_id)
                    else:
                        logger.error(f"Batch update failed: {update_result.get('error')}")
                        for item in batch:
                            task_tracker.mark_failed(item['resource_id'], batch_id)
                
                # Cập nhật duration cho từng video
                for result in successful_results:
                    resource_id = result['resource_id']
                    duration = result['duration']

                    await update_point_metadata_via_payload_api(
                        collection_name=self.config.collection_name,
                        point_id=resource_id,
                        time=duration
                    )
                    logger.info(f"Updated time={duration} for resource_id={resource_id}")
                    
            except Exception as e:
                logger.error(f"Batch update failed: {str(e)}")
        
        # Step 3: Dọn dẹp tất cả files
        cleanup_tasks = []
        for result in results:
            if not isinstance(result, Exception) and 'cleanup_files' in result:
                resource_id = result.get('resource_id', 'unknown')
                cleanup_tasks.append(self._cleanup_media_files(result['cleanup_files'], resource_id, batch_id))
        
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        
        logger.info(f"Completed processing {len(downloaded_files)} videos for detection - batch_id={batch_id}")

    async def _process_with_file_management_detection(self, resources: List[Dict], batch_id: str):
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

            logger.info(f"Downloading batch of {len(to_download)} files for detection. Remaining: {len(remaining_resources)}.")
            downloaded_files = await self._download_batch(to_download, batch_id)

            if downloaded_files:
                await self._process_detection_files_batch(downloaded_files, batch_id)
                
    async def _process_single_detection_video_concurrent(self, file_info: Dict, batch_id: str):
        """Xử lý một video detection bất đồng bộ và trả về kết quả."""
        resource_id = file_info['resource_id']
        original_path = file_info['original_path']
        duration = file_info['duration']
        processed_path = str(self.config.temp_dir / f"{resource_id}_processed.mp4")
        cleanup_files = [('original', original_path), ('processed', processed_path)]
        
        try:
            # Shrink video
            await shrink_video(input_path=original_path, output_path=processed_path)
            
            # Detection
            detection_result = await detect_real_or_ai(input_path=processed_path)
            detection_data = detection_result.get('data', {})
            is_real = detection_data.get('is_real', 0)
            
            return {
                'resource_id': resource_id,
                'is_real': is_real,
                'duration': duration,
                'cleanup_files': cleanup_files,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Processing failed for resource_id={resource_id}: {str(e)}")
            task_tracker.mark_failed(resource_id, batch_id)
            return {
                'resource_id': resource_id,
                'cleanup_files': cleanup_files,
                'success': False
            }