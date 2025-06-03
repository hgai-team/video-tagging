import asyncio
import logging
import seqlog
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

from api.router.stock_router import get_resources_old_version, get_download_url, send_tags
from api.router.video_router import shrink_video, video_tagging, delete_video
from api.router.tags_router import upsert_points
from core import VideoProcessor

# Configure seqlog
seqlog.log_to_seq(
    server_url="http://localhost:5341",  # Change if needed
    level=logging.INFO,
    batch_size=10,
    auto_flush_timeout=2
)
logger = logging.getLogger(__name__)



class OldVersionPipelineProcessor:
    def __init__(self, max_concurrent_downloads: int = 10, max_concurrent_processing: int = 10):
        self.max_concurrent_downloads = max_concurrent_downloads
        self.max_concurrent_processing = max_concurrent_processing
        
        # Separate semaphores for different operations
        self.download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self.processing_semaphore = asyncio.Semaphore(max_concurrent_processing)
        
        logger.info(f"OldVersionPipelineProcessor initialized - max_downloads={max_concurrent_downloads}, max_processing={max_concurrent_processing}")
        
    async def process_old_version_batch(self, current_tag_version: str, media_type: int, collection_name: str):
        """Main pipeline process for old version resources with batching"""
        batch_id = f"old_version_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting old version batch - batch_id={batch_id}, current_tag_version={current_tag_version}, media_type={media_type}")
        
        try:
            # Step 1: Get old version resources
            logger.info(f"Step 1: Getting old version resources - batch_id={batch_id}")
            old_version_data = await get_resources_old_version(
                current_tag_version=current_tag_version,
                media_type=media_type
            )
            
            if not old_version_data or not isinstance(old_version_data, list):
                logger.warning(f"No old version resources found - batch_id={batch_id}")
                return
                
            resource_ids = [item.get('id') for item in old_version_data if item.get('id')]
            logger.info(f"Found old version resources - batch_id={batch_id}, count={len(resource_ids)}, ids={resource_ids}")
            
            # Step 2: Get download URLs concurrently
            logger.info(f"Step 2: Getting download URLs - batch_id={batch_id}")
            download_tasks = [self._get_download_url_safe(resource_id, batch_id) for resource_id in resource_ids]
            url_results = await asyncio.gather(*download_tasks, return_exceptions=True)
            
            # Filter successful URLs
            valid_downloads = []
            for i, result in enumerate(url_results):
                if isinstance(result, Exception):
                    logger.error(f"Failed to get download URL - batch_id={batch_id}, resource_id={resource_ids[i]}, error={str(result)}")
                else:
                    valid_downloads.append((resource_ids[i], result))
            
            logger.info(f"Valid download URLs - batch_id={batch_id}, count={len(valid_downloads)}")
            
            # Process in batches of max_concurrent_downloads
            await self._process_download_batches(valid_downloads, collection_name, batch_id)
                
            logger.info(f"Old version batch processing completed - batch_id={batch_id}")
            
        except Exception as e:
            logger.error(f"Old version pipeline batch failed - batch_id={batch_id}, error={str(e)}")
            raise
    
    async def _process_download_batches(self, valid_downloads: List, collection_name: str, batch_id: str):
        """Process downloads in batches of max_concurrent_downloads"""
        
        for i in range(0, len(valid_downloads), self.max_concurrent_downloads):
            current_batch = valid_downloads[i:i + self.max_concurrent_downloads]
            batch_num = (i // self.max_concurrent_downloads) + 1
            
            logger.info(f"Processing download batch {batch_num} - batch_id={batch_id}, count={len(current_batch)}")
            
            # Step 3: Download current batch
            logger.info(f"Step 3: Downloading videos batch {batch_num} - batch_id={batch_id}")
            download_tasks = [self._download_video_safe(resource_id, url_data, batch_id) 
                            for resource_id, url_data in current_batch]
            downloaded_results = await asyncio.gather(*download_tasks, return_exceptions=True)
            
            # Filter successful downloads
            valid_files = []
            for j, result in enumerate(downloaded_results):
                if isinstance(result, Exception):
                    resource_id = current_batch[j][0]
                    logger.error(f"Failed to download video - batch_id={batch_id}, resource_id={resource_id}, error={str(result)}")
                else:
                    valid_files.append(result)
            
            logger.info(f"Successfully downloaded batch {batch_num} - batch_id={batch_id}, count={len(valid_files)}")
            
            # 🚀 PARALLEL PROCESSING: Process all videos in current batch concurrently
            if valid_files:
                logger.info(f"Starting parallel video processing - batch_id={batch_id}, video_count={len(valid_files)}, max_concurrent={self.max_concurrent_processing}")
                
                # Create tasks for all videos in this batch
                video_processing_tasks = [
                    self._process_single_video_concurrent(file_info, collection_name, batch_id) 
                    for file_info in valid_files
                ]
                
                # Process all videos concurrently with semaphore control
                processing_results = await asyncio.gather(*video_processing_tasks, return_exceptions=True)
                
                # Log results
                successful_count = 0
                failed_count = 0
                for k, result in enumerate(processing_results):
                    resource_id = valid_files[k]['resource_id']
                    if isinstance(result, Exception):
                        failed_count += 1
                        logger.error(f"Video processing failed - batch_id={batch_id}, resource_id={resource_id}, error={str(result)}")
                    else:
                        successful_count += 1
                        logger.info(f"Video processing completed - batch_id={batch_id}, resource_id={resource_id}")
                
                logger.info(f"Batch {batch_num} processing summary - batch_id={batch_id}, successful={successful_count}, failed={failed_count}, total={len(valid_files)}")
            
            logger.info(f"Completed processing batch {batch_num} - batch_id={batch_id}")
    
    async def _get_download_url_safe(self, resource_id: str, batch_id: str):
        """Safely get download URL for a resource"""
        try:
            result = await get_download_url(id=resource_id)
            logger.info(f"Get download URL success - batch_id={batch_id}, resource_id={resource_id}")
            return result
        except Exception as e:
            logger.error(f"Get download URL failed - batch_id={batch_id}, resource_id={resource_id}, step=get_download_url, error={str(e)}")
            raise
    
    async def _download_video_safe(self, resource_id: str, url_data: Dict, batch_id: str):
        """Safely download video with semaphore"""
        async with self.download_semaphore:
            try:
                logger.info(f"Starting download process - batch_id={batch_id}, resource_id={resource_id}")
                
                # Check various possible URL fields
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
                    raise ValueError(f"No download URL found. Available fields: {available_fields}")
                
                output_path = f"./temp_videos/{resource_id}.mp4"
                Path("./temp_videos").mkdir(exist_ok=True)
                
                logger.info(f"Attempting to download - batch_id={batch_id}, resource_id={resource_id}, url={download_url}, output_path={output_path}")
                
                async with VideoProcessor() as processor:
                    result = await processor.download_video(url=download_url, output_path=output_path)
                    
                logger.info(f"Download completed successfully - batch_id={batch_id}, resource_id={resource_id}, output_path={output_path}, result={result}")
                
                return {
                    'resource_id': resource_id,
                    'original_path': output_path,
                    'url_data': url_data
                }
                
            except Exception as e:
                logger.error(f"Download video failed - batch_id={batch_id}, resource_id={resource_id}, step=download_video, error={str(e)}, error_type={type(e).__name__}")
                import traceback
                logger.error(f"Download traceback - batch_id={batch_id}, resource_id={resource_id}, traceback={traceback.format_exc()}")
                raise
    
    async def _process_single_video_concurrent(self, file_info: Dict, collection_name: str, batch_id: str):
        """Process a single video through steps 4-8 with concurrent control"""
        # Use semaphore to limit concurrent video processing
        async with self.processing_semaphore:
            return await self._process_single_video(file_info, collection_name, batch_id)
    
    async def _process_single_video(self, file_info: Dict, collection_name: str, batch_id: str):
        """Process a single video through steps 4-8 (sequential within single video)"""
        resource_id = file_info['resource_id']
        original_path = file_info['original_path']
        
        processed_path = None
        start_time = asyncio.get_event_loop().time()
        
        try:
            logger.info(f"Starting video processing pipeline - batch_id={batch_id}, resource_id={resource_id}")
            
            # Step 4: Shrink video (sequential - required)
            logger.info(f"Step 4: Shrinking video - batch_id={batch_id}, resource_id={resource_id}")
            processed_path = f"./temp_videos/{resource_id}_processed.mp4"
            await shrink_video(input_path=original_path, output_path=processed_path)
            logger.info(f"Step 4 completed - batch_id={batch_id}, resource_id={resource_id}")
            
            # Step 5: Video tagging (sequential - depends on step 4)
            logger.info(f"Step 5: Video tagging - batch_id={batch_id}, resource_id={resource_id}")
            tagging_result = await video_tagging(input_path=processed_path)
            tags_data = tagging_result.get('data', {})
            logger.info(f"Step 5 completed - batch_id={batch_id}, resource_id={resource_id}, tags_count={len(tags_data) if isinstance(tags_data, dict) else 'unknown'}")
            
            # Steps 6 & 7: Parallel execution (both depend on step 5 results)
            logger.info(f"Steps 6&7: Parallel upsert and send - batch_id={batch_id}, resource_id={resource_id}")
            
            upsert_task = self._upsert_points_safe(collection_name, tags_data, resource_id, batch_id)
            send_task = self._send_tags_safe(resource_id, tags_data, batch_id)
            
            # Execute steps 6 & 7 concurrently
            step67_results = await asyncio.gather(upsert_task, send_task, return_exceptions=True)
            
            # Check results of parallel steps
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
            
            logger.info(f"Video processing pipeline completed - batch_id={batch_id}, resource_id={resource_id}, processing_time={processing_time}s, upsert_success={upsert_success}, send_success={send_success}")
            
            return {
                'resource_id': resource_id,
                'success': True,
                'processing_time': processing_time,
                'upsert_success': upsert_success,
                'send_success': send_success
            }
            
        except Exception as e:
            end_time = asyncio.get_event_loop().time()
            processing_time = round(end_time - start_time, 2)
            logger.error(f"Video processing pipeline failed - batch_id={batch_id}, resource_id={resource_id}, processing_time={processing_time}s, error={str(e)}")
            raise
        
        finally:
            # Step 8: Delete local files (can be parallel)
            try:
                logger.info(f"Step 8: Cleaning up files - batch_id={batch_id}, resource_id={resource_id}")
                
                cleanup_tasks = []
                if original_path and Path(original_path).exists():
                    cleanup_tasks.append(self._delete_video_safe(original_path, resource_id, batch_id, "original"))
                    
                if processed_path and Path(processed_path).exists():
                    cleanup_tasks.append(self._delete_video_safe(processed_path, resource_id, batch_id, "processed"))
                
                if cleanup_tasks:
                    cleanup_results = await asyncio.gather(*cleanup_tasks, return_exceptions=True)
                    
                    cleanup_success = sum(1 for result in cleanup_results if not isinstance(result, Exception))
                    logger.info(f"Step 8 completed - batch_id={batch_id}, resource_id={resource_id}, files_cleaned={cleanup_success}/{len(cleanup_tasks)}")
                else:
                    logger.info(f"Step 8 completed - batch_id={batch_id}, resource_id={resource_id}, no_files_to_clean=True")
                    
            except Exception as e:
                logger.error(f"File cleanup failed - batch_id={batch_id}, resource_id={resource_id}, step=delete_video, error={str(e)}")
    
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
                tag_version="v3"
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