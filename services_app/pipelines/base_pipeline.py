import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Tuple, Any, Optional
import requests

from api.router.stock_router import get_download_url
from config.settings import get_settings
from .data_sources import BaseDataSource
from config.config import PipelineConfig
from utils.logger_utils import setup_pipeline_logger
from utils.error_logger import log_download_error

class BasePipelineProcessor(ABC):
    
    def __init__(self, config: PipelineConfig, data_source: BaseDataSource):
        self.config = config
        self.data_source = data_source
        self.processing_semaphore = asyncio.Semaphore(config.max_concurrent_processing)
        self.config.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup dedicated logger for this pipeline
        self.logger = setup_pipeline_logger(
            logger_name=f"{self.__class__.__name__}_{config.name}",
            log_file_name=config.log_file_name
        )
        
        # Add sets to track processed and failed IDs for the current run
        self.processed_ids = set()
        self.failed_download_ids = set()
        
        self.logger.info(
            f"{self.__class__.__name__} initialized with: "
            f"max_downloads={config.max_concurrent_downloads},"
            f"max_processing={config.max_concurrent_processing},"
            f"temp_dir='{config.temp_dir}'"
        )

    async def run(self, **kwargs: Any):
        batch_id = f"{self.config.name}_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(f"Starting batch - batch_id={batch_id}")
        
        # Clear the processed and failed IDs sets at the beginning of each run
        self.processed_ids.clear()
        self.failed_download_ids.clear()

        try:
            await self._batch_cleanup_temp_folder(batch_id)

            batch_size = kwargs.get('batch_size', 200)
            offset = 0
            total_processed = 0
            
            while True:
                self.logger.info(f"Step 1: Getting resources batch at offset={offset} with size={batch_size} - batch_id={batch_id}")
                context = {
                    "media_type": self.config.media_type,
                    "collection_name": self.config.collection_name,
                    "current_tag_version": self.config.tag_version,
                    "batch_size": batch_size,
                    "offset": offset,
                    **kwargs
                }
                raw_resources = await self.data_source.get_resources(**context)

                if not raw_resources or not isinstance(raw_resources, list) or len(raw_resources) == 0:
                    self.logger.info(f"No more resources found at offset={offset} - batch_id={batch_id}")
                    break

                # Filter out resources that have already been processed in this run
                resources_to_process = [res for res in raw_resources if res.get('id') not in self.processed_ids]
                
                if not resources_to_process:
                    self.logger.warning(f"No new valid resources to process at offset={offset} after filtering processed IDs - batch_id={batch_id}")
                    offset += len(raw_resources) # Adjust offset by original count
                    continue

                resources_with_duration = self._prepare_resources_by_duration(resources_to_process, batch_id)
                if not resources_with_duration:
                    self.logger.warning(f"No valid resources after duration filtering at offset={offset} - batch_id={batch_id}")
                    offset += len(raw_resources)
                    continue

                self.logger.info(f"Found {len(resources_with_duration)} valid resources to process at offset={offset}.")

                # Mark all IDs in this batch as processed to prevent reprocessing
                for resource_id, _ in resources_with_duration:
                    self.processed_ids.add(resource_id)
                
                self.logger.info(f"Step 2: Getting download URLs - batch_id={batch_id}")
                resources_with_urls = await self._get_download_urls_batch(resources_with_duration, batch_id)

                await self._process_with_file_management(resources_with_urls, batch_id)
                
                total_processed += len(resources_with_duration)
                offset += len(raw_resources) # Always advance offset by the number of items fetched
                
                self.logger.info(f"Processed batch at offset={offset-len(raw_resources)}, total processed so far: {total_processed} - batch_id={batch_id}")
                
                if len(raw_resources) < batch_size:
                    self.logger.info(f"Fetched {len(raw_resources)} resources < batch_size {batch_size}, reached end of collection - batch_id={batch_id}")
                    break

            self.logger.info(f"Batch processing completed successfully, total processed: {total_processed} - batch_id={batch_id}")

        except Exception as e:
            self.logger.error(f"Pipeline batch failed critically - batch_id={batch_id}, error={e}", exc_info=True)
            raise

    def _prepare_resources_by_duration(self, resource_data: List[Dict], batch_id: str) -> List[Tuple[str, int]]:
        resources = []
        for item in resource_data:
            resource_id = item.get('id')
            duration = item.get('duration', 0)
            if resource_id and duration > 0:
                resources.append((resource_id, duration))
            elif resource_id:
                self.logger.debug(f"Skipping resource with zero duration - batch_id={batch_id}, resource_id={resource_id}")

        resources.sort(key=lambda x: x[1])
        return resources

    async def _get_download_urls_batch(self, resources: List[Tuple[str, int]], batch_id: str) -> List[Dict]:
        self.logger.info(f"Starting to fetch {len(resources)} URLs concurrently")
        tasks = [self._get_download_url_safe(res_id, dur, batch_id) for res_id, dur in resources]
        results = await asyncio.gather(*tasks)

        valid_resources = []
        failed_resources_ids = []
        
        for i, result in enumerate(results):
            resource_id, duration = resources[i]
            if result is not None:
                valid_resources.append({'resource_id': resource_id, 'duration': duration, 'url_data': result})
            else:
                failed_resources_ids.append(resource_id)
        
        if failed_resources_ids:
            # Add failed IDs to the failed set for logging and CSV export
            for res_id in failed_resources_ids:
                self.failed_download_ids.add(res_id)
            self.logger.warning(f"Skipped {len(failed_resources_ids)} resources due to download URL errors: {failed_resources_ids[:5]}{'...' if len(failed_resources_ids) > 5 else ''}")
            
        self.logger.info(f"Got {len(valid_resources)}/{len(resources)} valid download URLs - batch_id={batch_id}")
        return valid_resources

    async def _process_with_file_management(self, resources: List[Dict], batch_id: str):
        remaining_resources = resources.copy()
        while remaining_resources:
            current_files = self._count_temp_files()
            available_slots = self.config.max_concurrent_downloads - current_files

            if available_slots <= 0:
                self.logger.warning(f"Proceeding to clear all files to free up space.")
                
                temp_files = self._get_temp_files_list()
                
                if temp_files:
                    await self._cleanup_media_files(temp_files, f"batch_{batch_id}")
                
                current_files = self._count_temp_files()
                available_slots = self.config.max_concurrent_downloads - current_files
                self.logger.info(f"After cleanup, temp folder has {current_files} files, {available_slots} slots available.")
                
                if available_slots <= 0:
                    self.logger.error(f"Temp folder is still full after cleanup. {len(remaining_resources)} resources remaining unprocessed.")
                    break

            to_download = remaining_resources[:available_slots]
            remaining_resources = remaining_resources[available_slots:]

            self.logger.info(f"Downloading batch of {len(to_download)} files. Remaining: {len(remaining_resources)}.")
            downloaded_files = await self._download_batch(to_download, batch_id)

            if downloaded_files:
                await self._process_media_files(downloaded_files, batch_id)
    
    async def _download_batch(self, resources: List[Dict], batch_id: str) -> List[Dict]:
        self.logger.info(f"Starting to download batch with {len(resources)} resources")
        
        resource_ids = [res['resource_id'] for res in resources]
        self.logger.info(f"Resources to download: {resource_ids}")
        
        tasks = [
            self._download_media_safe(
                res['resource_id'], res['url_data'], res['duration'], batch_id
            ) for res in resources
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid_files = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                resource_id = resources[i]['resource_id']
                # Add failed ID to the failed set for logging and CSV export
                self.failed_download_ids.add(resource_id)
                self.logger.error(f"Download failed for {resource_id}: {str(result)}")
            else:
                valid_files.append(result)
        
        self.logger.info(f"Successfully downloaded {len(valid_files)}/{len(resources)} files for batch - batch_id={batch_id}")
        
        return valid_files
        
    async def _get_download_url_safe(self, resource_id: str, duration: int, batch_id: str) -> Optional[Dict]:
        """Gets the download URL for a resource, without pre-validation."""
        try:
            result = await get_download_url(id=resource_id)
            if not result:
                error_msg = "URL fetch returned None"
                log_download_error(resource_id, "N/A", error_msg)
                self.logger.warning(f"URL fetch returned None for resource_id={resource_id}, skipping.")
                return None
            
            return result
        except Exception as e:
            error_msg = f"Get download URL failed: {str(e)}"
            log_download_error(resource_id, "N/A", error_msg)
            self.logger.warning(f"Get download URL failed - resource_id={resource_id}, error={e}")
            return None
    
    def _extract_download_url_from_result(self, result: Dict) -> Optional[str]:
        if not result or not isinstance(result, dict):
            return None
            
        url_fields = ['url', 'download_url', 'downloadUrl', 'link', 'videoUrl', 'audioUrl']
        
        url = next((result.get('data', {}).get(f) for f in url_fields if f in result.get('data', {})), None)
        if not url:
            url = next((result.get(f) for f in url_fields if f in result), None)
            
        return url

    async def _process_media_files(self, downloaded_files: List[Dict], batch_id: str):
        self.logger.info(f"Processing {len(downloaded_files)} downloaded files - batch_id={batch_id}")
        tasks = [self._process_single_media_concurrent(file_info, batch_id) for file_info in downloaded_files]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_single_media_concurrent(self, file_info: Dict, batch_id: str):
        resource_id = file_info['resource_id']
        async with self.processing_semaphore:
            self.logger.info(f"Acquired semaphore for processing - resource_id={resource_id}")
            await self._process_media(file_info, batch_id)
            self.logger.info(f"Released semaphore for processing - resource_id={resource_id}")
            
    async def _upsert_stock_vector(self, points: Dict, resource_id: str, media_type: str) -> Dict:
        settings = get_settings()
        url = f"{settings.TAG_DOMAIN}af/collections/{self.config.collection_name}/points"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
            
        data = {
            "points": [points], 
            "ids": [resource_id],
            "media_type": media_type
        }
        max_retries = 5

        for attempt in range(max_retries):
            try:
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: requests.put(url=url, headers=headers, json=data, timeout=60, verify=False)
                )
                response.raise_for_status()
                self.logger.info(f"Upsert stock vector success - resource_id={resource_id}, media_type={media_type}")
                return response.json()
            except requests.RequestException as e:
                self.logger.warning(f"Upsert stock vector attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)

        raise RuntimeError(f"Upsert stock vector failed after {max_retries} retries for resource {resource_id}")
    
    async def _batch_cleanup_temp_folder(self, batch_id: str):
        try:
            old_files = []
            current_time = datetime.now().timestamp()
            
            file_patterns = self._get_file_patterns()
            
            for pattern in file_patterns:
                for file_path in self.config.temp_dir.glob(pattern):
                    if file_path.is_file() and (current_time - file_path.stat().st_mtime > 3600):
                        old_files.append(file_path)

            if old_files:
                self.logger.warning(f"Found {len(old_files)} old temp files, cleaning up - batch_id={batch_id}")
                for file_path in old_files:
                    try:
                        file_path.unlink()
                        self.logger.info(f"Cleaned old file: {file_path}")
                    except OSError as e:
                        self.logger.error(f"Failed to clean old file {file_path}: {e}")
        except Exception as e:
            self.logger.error(f"Batch temp cleanup failed - batch_id={batch_id}, error={e}")

    @abstractmethod
    def _count_temp_files(self) -> int:
        pass
        
    @abstractmethod
    def _get_file_patterns(self) -> List[str]:
        pass
        
    @abstractmethod
    def _get_temp_files_list(self) -> List[Tuple[str, str]]:
        pass
    
    @abstractmethod
    async def _download_media_safe(self, resource_id: str, url_data: Dict, duration: int, batch_id: str) -> Dict:
        pass
        
    @abstractmethod
    async def _process_media(self, file_info: Dict, batch_id: str):
        pass
        
    @abstractmethod
    async def _cleanup_media_files(self, cleanup_files: List[Tuple[str, str]], resource_id: str, batch_id: str = None):
        pass