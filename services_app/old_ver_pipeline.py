import asyncio
import logging
import seqlog
from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime
import traceback
import os

from api.router.stock_router import get_resources_old_version, get_download_url, send_tags
from api.router.video_router import shrink_video, video_tagging, delete_video
from api.router.tags_router import upsert_points
from core import VideoProcessor
from db import task_tracker

# Configure seqlog
seqlog.log_to_seq(
    server_url="http://localhost:5341",  # Change if needed
    level=logging.INFO,
    batch_size=10,
    auto_flush_timeout=2
)
logger = logging.getLogger(__name__)

# Constants
MAX_TEMP_VIDEOS = 10
TAG_VERSION = "v3_Jun05"
TEMP_VIDEOS_DIR = "./temp_videos"
MIN_VIDEO_SIZE_MB = 2  # Minimum video size in MB


class OldVersionPipelineProcessor:
    def __init__(self, max_concurrent_downloads: int = 10, max_concurrent_processing: int = 10):
        self.max_concurrent_downloads = max_concurrent_downloads
        self.max_concurrent_processing = max_concurrent_processing
        
        # Separate semaphores for different operations
        self.download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self.processing_semaphore = asyncio.Semaphore(max_concurrent_processing)
        
        logger.info(f"OldVersionPipelineProcessor initialized - max_downloads={max_concurrent_downloads}, max_processing={max_concurrent_processing}")
        
    async def process_old_version_batch(self, current_tag_version: str, media_type: int, collection_name: str):
        """Main pipeline process for old version resources with batching and duration-based sorting"""
        batch_id = f"old_version_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting old version batch - batch_id={batch_id}, current_tag_version={current_tag_version}, media_type={media_type}")
        
        try:
            # Cleanup any leftover files from previous batches
            await self._batch_cleanup_temp_folder(batch_id)
            
            # Step 1: Get old version resources with duration
            logger.info(f"Step 1: Getting old version resources - batch_id={batch_id}")
            old_version_data = await get_resources_old_version(
                current_tag_version=current_tag_version,
                media_type=media_type
            )
            
            if not old_version_data or not isinstance(old_version_data, list):
                logger.warning(f"No old version resources found - batch_id={batch_id}")
                return
            
            # Extract and sort resources by duration
            resources_with_duration = self._prepare_resources_by_duration(old_version_data, batch_id)
            
            if not resources_with_duration:
                logger.warning(f"No valid old version resources after filtering - batch_id={batch_id}")
                return
            
            logger.info(f"Found valid old version resources - batch_id={batch_id}, count={len(resources_with_duration)}")
            
            # Step 2: Get download URLs concurrently
            logger.info(f"Step 2: Getting download URLs - batch_id={batch_id}")
            resources_with_urls = await self._get_download_urls_batch(resources_with_duration, batch_id)
            
            # Step 3-8: Process with file management
            await self._process_with_file_management(resources_with_urls, collection_name, batch_id)
                
            logger.info(f"Batch processing completed - batch_id={batch_id}")
            
        except Exception as e:
            logger.error(f"Pipeline batch failed - batch_id={batch_id}, error={str(e)}")
            raise
    

    def _prepare_resources_by_duration(self, old_version_data: List[Dict], batch_id: str) -> List[Tuple[str, int]]:
        """Extract resources, filter by duration > 0, and sort by duration"""
        resources = []
        
        for item in old_version_data:
            resource_id = item.get('id')
            duration = item.get('duration', 0)
            
            if resource_id and duration > 0:
                resources.append((resource_id, duration))
            elif resource_id and duration == 0:
                logger.debug(f"Skipping resource with zero duration - batch_id={batch_id}, resource_id={resource_id}")
        
        # Sort by duration (low to high)
        resources.sort(key=lambda x: x[1])
        
        logger.info(f"Resources sorted by duration - batch_id={batch_id}, total={len(resources)}, "
                f"min_duration={resources[0][1] if resources else 0}, "
                f"max_duration={resources[-1][1] if resources else 0}")
        
        return resources
    

    async def _get_download_urls_batch(self, resources_with_duration: List[Tuple[str, int]], batch_id: str) -> List[Dict]:
        """Get download URLs for all resources concurrently"""
        download_tasks = [
            self._get_download_url_safe(resource_id, duration, batch_id) 
            for resource_id, duration in resources_with_duration
        ]
        url_results = await asyncio.gather(*download_tasks, return_exceptions=True)
        
        # Filter successful URLs
        valid_resources = []
        
        for i, result in enumerate(url_results):
            resource_id, duration = resources_with_duration[i]
            if isinstance(result, Exception):
                logger.error(f"Failed to get download URL - batch_id={batch_id}, resource_id={resource_id}, error={str(result)}")
            else:
                valid_resources.append({
                    'resource_id': resource_id,
                    'duration': duration,
                    'url_data': result
                })
        
        logger.info(f"Valid download URLs - batch_id={batch_id}, count={len(valid_resources)}")
        return valid_resources
    
    async def _process_with_file_management(self, resources_with_urls: List[Dict], collection_name: str, batch_id: str):
        """Process resources with file count management"""
        Path(TEMP_VIDEOS_DIR).mkdir(exist_ok=True)
        
        remaining_resources = resources_with_urls.copy()
        
        while remaining_resources:
            # Step 3: Download up to MAX_TEMP_VIDEOS files
            logger.info(f"Step 3: Downloading batch - batch_id={batch_id}, remaining={len(remaining_resources)}")
            
            current_temp_files = self._count_temp_files()
            available_slots = MAX_TEMP_VIDEOS - current_temp_files
            
            if available_slots <= 0:
                logger.warning(f"Temp folder full - batch_id={batch_id}, current_files={current_temp_files}")
                break
            
            # Take resources to download (up to available slots)
            to_download = remaining_resources[:available_slots]
            remaining_resources = remaining_resources[available_slots:]
            
            logger.info(f"Downloading files - batch_id={batch_id}, count={len(to_download)}, remaining_after={len(remaining_resources)}")
            
            # Download current batch
            downloaded_files = await self._download_batch(to_download, batch_id)
            
            if downloaded_files:
                # Step 4-8: Process downloaded files
                await self._process_video_files(downloaded_files, collection_name, batch_id)
            
            # If there are more resources, continue the loop
            if remaining_resources:
                logger.info(f"Continuing with remaining resources - batch_id={batch_id}, count={len(remaining_resources)}")
        
        if remaining_resources:
            logger.warning(f"Some resources were not processed due to file limitations - batch_id={batch_id}, count={len(remaining_resources)}")
    
    def _count_temp_files(self) -> int:
        """Count current files in temp_videos directory"""
        temp_path = Path(TEMP_VIDEOS_DIR)
        if not temp_path.exists():
            return 0
        return len([f for f in temp_path.iterdir() if f.is_file() and f.suffix == '.mp4'])
    
    async def _download_batch(self, resources: List[Dict], batch_id: str) -> List[Dict]:
        """Download a batch of resources sequentially"""
        valid_files = []
        
        for resource in resources:
            resource_id = resource['resource_id']
            try:
                result = await self._download_video_safe(
                    resource_id=resource_id, 
                    url_data=resource['url_data'], 
                    duration=resource['duration'], 
                    batch_id=batch_id
                )
                valid_files.append(result)
            except Exception as e:
                if "Video file size too small" in str(e):
                    logger.info(f"Skipped small video file - batch_id={batch_id}, resource_id={resource_id}")
                    # Task already marked as skipped in _download_video_safe
                else:
                    logger.error(f"Failed to download video - batch_id={batch_id}, resource_id={resource_id}, error={str(e)}")
        
        logger.info(f"Successfully downloaded - batch_id={batch_id}, count={len(valid_files)}")
        return valid_files
    
    async def _process_video_files(self, downloaded_files: List[Dict], collection_name: str, batch_id: str):
        """Process downloaded video files through the pipeline"""
        logger.info(f"Starting video processing - batch_id={batch_id}, video_count={len(downloaded_files)}")
        
        # Process all videos concurrently with semaphore control
        video_processing_tasks = [
            self._process_single_video_concurrent(file_info, collection_name, batch_id)
            for file_info in downloaded_files
        ]
        
        processing_results = await asyncio.gather(*video_processing_tasks, return_exceptions=True)
        
        # Log results
        successful_count = 0
        failed_count = 0
        for i, result in enumerate(processing_results):
            resource_id = downloaded_files[i]['resource_id']
            if isinstance(result, Exception):
                failed_count += 1
                logger.error(f"Video processing failed - batch_id={batch_id}, resource_id={resource_id}, error={str(result)}")
            else:
                successful_count += 1
                logger.info(f"Video processing completed - batch_id={batch_id}, resource_id={resource_id}")
        
        logger.info(f"Processing summary - batch_id={batch_id}, successful={successful_count}, failed={failed_count}, total={len(downloaded_files)}")
    
    async def _get_download_url_safe(self, resource_id: str, duration: int, batch_id: str):
        """Safely get download URL for a resource"""
        try:
            result = await get_download_url(id=resource_id)
            logger.info(f"Get download URL success - batch_id={batch_id}, resource_id={resource_id}, duration={duration}")
            return result
        except Exception as e:
            logger.error(f"Get download URL failed - batch_id={batch_id}, resource_id={resource_id}, duration={duration}, error={str(e)}")
            raise
    
    async def _download_video_safe(self, resource_id: str, url_data: Dict, duration: int, batch_id: str):
        """Safely download video with synchronous requests"""
        # Start tracking the task before we do any work
        task_tracker.start_task(resource_id, batch_id)
        
        # Trích xuất URL từ url_data
        download_url = None
        url_fields = ['url', 'download_url', 'downloadUrl', 'link', 'videoUrl']
        
        for field in url_fields:
            if field in url_data and url_data[field]:
                download_url = url_data[field]
                logger.info(f"Found download URL - batch_id={batch_id}, resource_id={resource_id}, field={field}")
                break
        
        if not download_url:
            available_fields = list(url_data.keys())
            logger.error(f"No download URL found - batch_id={batch_id}, resource_id={resource_id}, available_fields={available_fields}")
            task_tracker.mark_failed(resource_id, batch_id)
            raise ValueError(f"No download URL found. Available fields: {available_fields}")
        
        output_path = f"{TEMP_VIDEOS_DIR}/{resource_id}.mp4"
        
        try:
            logger.info(f"Starting download process - batch_id={batch_id}, resource_id={resource_id}, duration={duration}")
            logger.info(f"Attempting to download - batch_id={batch_id}, resource_id={resource_id}, duration={duration}, url={download_url}, output_path={output_path}")
            
            # Ghi lại thời gian bắt đầu download để tính tốc độ
            start_time = asyncio.get_event_loop().time()
            
            async with VideoProcessor() as processor:
                result = await processor.download_video(url=download_url, output_path=output_path)
            
            end_time = asyncio.get_event_loop().time()
            download_time = end_time - start_time
            
            # Kiểm tra file đã tải xuống
            if os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                file_size_mb = file_size / (1024 * 1024)
                download_speed = file_size_mb / download_time if download_time > 0 else 0
                
                logger.info(f"Download completed - batch_id={batch_id}, resource_id={resource_id}, file_size={file_size} bytes ({file_size_mb:.2f} MB), time={download_time:.2f}s, speed={download_speed:.2f} MB/s")
                
                # Kiểm tra kích thước file nhỏ hơn MIN_VIDEO_SIZE_MB
                if file_size_mb < MIN_VIDEO_SIZE_MB:
                    logger.warning(f"Video file too small, skipping - batch_id={batch_id}, resource_id={resource_id}, size={file_size_mb:.2f}MB < {MIN_VIDEO_SIZE_MB}MB")
                    
                    # Xóa file nhỏ hơn MIN_VIDEO_SIZE_MB
                    try:
                        os.remove(output_path)
                        logger.info(f"Deleted small video file - batch_id={batch_id}, resource_id={resource_id}, path={output_path}")
                    except Exception as del_err:
                        logger.error(f"Failed to delete small video file - batch_id={batch_id}, resource_id={resource_id}, error={str(del_err)}")
                    
                    # Đánh dấu task là failed và kết thúc luồng
                    task_tracker.mark_failed(resource_id, batch_id)
                    raise ValueError(f"Video file size too small: {file_size_mb:.2f}MB < {MIN_VIDEO_SIZE_MB}MB")
                
                # Tiếp tục nếu file đủ lớn
                return {
                    'resource_id': resource_id,
                    'duration': duration,
                    'original_path': output_path,
                    'url_data': url_data,
                    'file_size': file_size,
                    'file_size_mb': file_size_mb
                }
            else:
                logger.error(f"Downloaded file not found - batch_id={batch_id}, resource_id={resource_id}, path={output_path}")
                raise FileNotFoundError(f"Downloaded file not found: {output_path}")
                
        except Exception as e:
            # Ghi lại trace đầy đủ của lỗi
            error_trace = traceback.format_exc()
            logger.error(f"Download video failed - batch_id={batch_id}, resource_id={resource_id}, duration={duration}, error={str(e)}, error_type={type(e).__name__}")
            logger.error(f"Download traceback - batch_id={batch_id}, resource_id={resource_id}, traceback={error_trace}")
            
            # Đánh dấu task là failed trong DB
            task_tracker.mark_failed(resource_id, batch_id)
            
            # Trả về lỗi để caller xử lý
            raise
    
    async def _process_single_video_concurrent(self, file_info: Dict, collection_name: str, batch_id: str):
        """Process a single video through steps 4-8 with concurrent control"""
        async with self.processing_semaphore:
            return await self._process_single_video(file_info, collection_name, batch_id)
        
    async def _batch_cleanup_temp_folder(self, batch_id: str):
        """Clean up any remaining files from previous batches"""
        try:
            temp_path = Path(TEMP_VIDEOS_DIR)
            if not temp_path.exists():
                return
                
            # Find all video files that might be stuck
            old_files = []
            current_time = datetime.now().timestamp()
            
            for file_path in temp_path.glob("*.mp4"):
                if file_path.is_file():
                    # Consider files older than 1 hour as stuck
                    file_age = current_time - file_path.stat().st_mtime
                    if file_age > 3600:  # 1 hour
                        old_files.append(file_path)
            
            if old_files:
                logger.warning(f"Found {len(old_files)} old temp files, cleaning up - batch_id={batch_id}")
                for file_path in old_files:
                    try:
                        file_path.unlink()
                        logger.info(f"Cleaned old file: {file_path}")
                    except Exception as e:
                        logger.error(f"Failed to clean old file {file_path}: {e}")
                        
        except Exception as e:
            logger.error(f"Batch temp cleanup failed - batch_id={batch_id}, error={e}")
    
    async def _process_single_video(self, file_info: Dict, collection_name: str, batch_id: str):
        """Process a single video through steps 4-8 (sequential within single video)"""
        resource_id = file_info['resource_id']
        duration = file_info['duration']
        original_path = file_info['original_path']
        file_size_mb = file_info.get('file_size_mb', 0)
        
        processed_path = None
        start_time = asyncio.get_event_loop().time()
        
        # Track all files that need cleanup
        cleanup_files = []
        if original_path:
            cleanup_files.append(('original', original_path))
        
        try:
            logger.info(f"Starting video processing pipeline - batch_id={batch_id}, resource_id={resource_id}, duration={duration}, file_size={file_size_mb:.2f}MB")
            
            # Step 4: Shrink video
            logger.info(f"Step 4: Shrinking video - batch_id={batch_id}, resource_id={resource_id}")
            processed_path = f"{TEMP_VIDEOS_DIR}/{resource_id}_processed.mp4"
            cleanup_files.append(('processed', processed_path))  # Add to cleanup list immediately
            
            await shrink_video(input_path=original_path, output_path=processed_path)
            logger.info(f"Step 4 completed - batch_id={batch_id}, resource_id={resource_id}")
            
            # Step 5: Video tagging
            logger.info(f"Step 5: Video tagging - batch_id={batch_id}, resource_id={resource_id}")
            tagging_result = await video_tagging(input_path=processed_path)
            tags_data = tagging_result.get('data', {})
            logger.info(f"Step 5 completed - batch_id={batch_id}, resource_id={resource_id}, tags_count={len(tags_data) if isinstance(tags_data, dict) else 'unknown'}")
            
            # Steps 6 & 7: Parallel execution
            logger.info(f"Steps 6&7: Parallel upsert and send - batch_id={batch_id}, resource_id={resource_id}")
            
            upsert_task = self._upsert_points_safe(collection_name, tags_data, resource_id, batch_id)
            send_task = self._send_tags_safe(resource_id, tags_data, batch_id)
            
            # Execute steps 6 & 7 concurrently
            step67_results = await asyncio.gather(upsert_task, send_task, return_exceptions=True)
            
            # Check results
            upsert_success = not isinstance(step67_results[0], Exception)
            send_success = not isinstance(step67_results[1], Exception)
            
            if not upsert_success:
                logger.error(f"Step 6 (upsert) failed - batch_id={batch_id}, resource_id={resource_id}, error={step67_results[0]}")
            else:
                logger.info(f"Step 6 (upsert) completed - batch_id={batch_id}, resource_id={resource_id}")
                
            if not send_success:
                logger.error(f"Step 7 (send tags) failed - batch_id={batch_id}, resource_id={resource_id}, error={step67_results[1]}")
            else:
                logger.info(f"Step 7 (send tags) completed - batch_id={batch_id}, resource_id={resource_id}")
            
            # Calculate processing time
            end_time = asyncio.get_event_loop().time()
            processing_time = round(end_time - start_time, 2)
            
            logger.info(f"Video processing pipeline completed - batch_id={batch_id}, resource_id={resource_id}, duration={duration}, processing_time={processing_time}s, upsert_success={upsert_success}, send_success={send_success}")
            
            # Mark task as successful
            task_tracker.mark_success(resource_id, batch_id)
            
            return {
                'resource_id': resource_id,
                'duration': duration,
                'success': True,
                'processing_time': processing_time,
                'upsert_success': upsert_success,
                'send_success': send_success
            }
            
        except Exception as e:
            end_time = asyncio.get_event_loop().time()
            processing_time = round(end_time - start_time, 2)
            
            # Get the full stack trace
            error_trace = traceback.format_exc()
            logger.error(f"Video processing pipeline failed - batch_id={batch_id}, resource_id={resource_id}, duration={duration}, processing_time={processing_time}s, error={str(e)}\n{error_trace}")
            
            # Mark task as failed
            task_tracker.mark_failed(resource_id, batch_id)
            
            raise
        
        finally:
            # Step 8: ALWAYS delete local files - this block MUST execute
            await self._cleanup_video_files(cleanup_files, resource_id, batch_id)

    async def _cleanup_video_files(self, cleanup_files: List[Tuple[str, str]], resource_id: str, batch_id: str):
        """Robust cleanup function that ensures all files are deleted"""
        logger.info(f"Step 8: Cleaning up files - batch_id={batch_id}, resource_id={resource_id}")
        
        cleanup_tasks = []
        files_to_clean = []
        
        for file_type, file_path in cleanup_files:
            if file_path and Path(file_path).exists():
                files_to_clean.append((file_type, file_path))
                cleanup_tasks.append(self._delete_video_safe(file_path, resource_id, batch_id, file_type))
        
        if not cleanup_tasks:
            logger.info(f"Step 8 completed - batch_id={batch_id}, resource_id={resource_id}, no_files_to_clean=True")
            return
        
        try:
            # First attempt: Try normal cleanup
            cleanup_results = await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            
            failed_cleanups = []
            success_count = 0
            
            for i, result in enumerate(cleanup_results):
                file_type, file_path = files_to_clean[i]
                if isinstance(result, Exception):
                    logger.warning(f"First cleanup attempt failed - batch_id={batch_id}, resource_id={resource_id}, file_type={file_type}, error={result}")
                    failed_cleanups.append((file_type, file_path))
                else:
                    success_count += 1
            
            # Second attempt: Retry failed cleanups with force
            if failed_cleanups:
                logger.warning(f"Retrying cleanup for {len(failed_cleanups)} files - batch_id={batch_id}, resource_id={resource_id}")
                
                retry_tasks = []
                for file_type, file_path in failed_cleanups:
                    if Path(file_path).exists():
                        retry_tasks.append(self._force_delete_video(file_path, resource_id, batch_id, file_type))
                
                if retry_tasks:
                    retry_results = await asyncio.gather(*retry_tasks, return_exceptions=True)
                    
                    for i, result in enumerate(retry_results):
                        file_type, file_path = failed_cleanups[i]
                        if not isinstance(result, Exception):
                            success_count += 1
                        else:
                            logger.error(f"Force cleanup also failed - batch_id={batch_id}, resource_id={resource_id}, file_type={file_type}, file_path={file_path}, error={result}")
            
            logger.info(f"Step 8 completed - batch_id={batch_id}, resource_id={resource_id}, files_cleaned={success_count}/{len(cleanup_tasks)}")
            
        except Exception as e:
            logger.error(f"Critical: Cleanup process failed entirely - batch_id={batch_id}, resource_id={resource_id}, error={str(e)}")
            
            # Last resort: Try to delete files synchronously
            try:
                import os
                sync_success = 0
                for file_type, file_path in files_to_clean:
                    try:
                        if Path(file_path).exists():
                            os.remove(file_path)
                            sync_success += 1
                            logger.info(f"Sync cleanup succeeded - batch_id={batch_id}, resource_id={resource_id}, file_type={file_type}")
                    except Exception as sync_error:
                        logger.error(f"Sync cleanup failed - batch_id={batch_id}, resource_id={resource_id}, file_type={file_type}, error={sync_error}")
                
                logger.info(f"Emergency sync cleanup completed - batch_id={batch_id}, resource_id={resource_id}, files_cleaned={sync_success}/{len(files_to_clean)}")
            except Exception as emergency_error:
                logger.critical(f"Emergency cleanup failed - batch_id={batch_id}, resource_id={resource_id}, error={emergency_error}")

    async def _force_delete_video(self, file_path: str, resource_id: str, batch_id: str, file_type: str):
        """Force delete with additional retry logic and different approaches"""
        try:
            # Try async delete with timeout
            await asyncio.wait_for(
                self._delete_video_safe(file_path, resource_id, batch_id, file_type),
                timeout=5.0
            )
            logger.info(f"Force delete succeeded - batch_id={batch_id}, resource_id={resource_id}, file_type={file_type}")
            
        except asyncio.TimeoutError:
            logger.warning(f"Delete timeout, trying sync method - batch_id={batch_id}, resource_id={resource_id}, file_type={file_type}")
            
            # Fallback to sync delete
            import os
            if Path(file_path).exists():
                os.remove(file_path)
                logger.info(f"Sync force delete succeeded - batch_id={batch_id}, resource_id={resource_id}, file_type={file_type}")
        
        except Exception as e:
            logger.error(f"Force delete failed - batch_id={batch_id}, resource_id={resource_id}, file_type={file_type}, error={str(e)}")
            raise

    async def _upsert_points_safe(self, collection_name: str, tags_data: Dict, resource_id: str, batch_id: str):
        """Safely upsert points with error handling"""
        try:
            await upsert_points(
                collection_name=collection_name,
                points=[tags_data],
                ids=[resource_id]
            )
            return True
        except Exception as e:
            logger.error(f"Upsert points failed - batch_id={batch_id}, resource_id={resource_id}, error={str(e)}")
            raise

    async def _send_tags_safe(self, resource_id: str, tags_data: Dict, batch_id: str):
        """Safely send tags with error handling"""
        try:
            await send_tags(
                id=resource_id,
                tags=tags_data,
                tag_version=TAG_VERSION
            )
            return True
        except Exception as e:
            logger.error(f"Send tags failed - batch_id={batch_id}, resource_id={resource_id}, error={str(e)}")
            raise

    async def _delete_video_safe(self, file_path: str, resource_id: str, batch_id: str, file_type: str):
        """Safely delete video file with error handling"""
        try:
            await delete_video(input_path=file_path)
            logger.info(f"File deleted successfully - batch_id={batch_id}, resource_id={resource_id}, file_type={file_type}, path={file_path}")
            return True
        except Exception as e:
            logger.error(f"File deletion failed - batch_id={batch_id}, resource_id={resource_id}, file_type={file_type}, path={file_path}, error={str(e)}")
            raise