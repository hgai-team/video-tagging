import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Tuple, Any
import requests

from api.router.stock_router import get_download_url
from config.settings import get_settings
from .data_sources import BaseDataSource
from config.config import PipelineConfig 

logger = logging.getLogger(__name__)

class BasePipelineProcessor(ABC):
    
    def __init__(self, config: PipelineConfig, data_source: BaseDataSource):
        self.config = config
        self.data_source = data_source
        self.processing_semaphore = asyncio.Semaphore(config.max_concurrent_processing)
        self.config.temp_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(
            f"{self.__class__.__name__} initialized with: "
            f"max_downloads={config.max_concurrent_downloads},"
            f"max_processing={config.max_concurrent_processing},"
            f"temp_dir='{config.temp_dir}'"
        )

    async def run(self, **kwargs: Any):
        batch_id = f"{self.config.name}_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting batch - batch_id={batch_id}")

        try:
            await self._batch_cleanup_temp_folder(batch_id)

            # Step 1: Lấy danh sách resource
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

            resources_with_duration = self._prepare_resources_by_duration(raw_resources, batch_id)
            if not resources_with_duration:
                logger.warning(f"No valid resources after filtering - batch_id={batch_id}")
                return

            logger.info(f"Found {len(resources_with_duration)} valid resources to process.")

            # Step 2: Lấy URL download
            logger.info(f"Step 2: Getting download URLs - batch_id={batch_id}")
            resources_with_urls = await self._get_download_urls_batch(resources_with_duration, batch_id)

            # Other steps: Tải và xử lý media
            await self._process_with_file_management(resources_with_urls, batch_id)

            logger.info(f"Batch processing completed successfully - batch_id={batch_id}")

        except Exception as e:
            logger.error(f"Pipeline batch failed critically - batch_id={batch_id}, error={e}", exc_info=True)
            raise

    def _prepare_resources_by_duration(self, resource_data: List[Dict], batch_id: str) -> List[Tuple[str, int]]:
        resources = []
        for item in resource_data:
            resource_id = item.get('id')
            duration = item.get('duration', 0)
            if resource_id and duration > 0:
                resources.append((resource_id, duration))
            elif resource_id:
                logger.debug(f"Skipping resource with zero duration - batch_id={batch_id}, resource_id={resource_id}")

        resources.sort(key=lambda x: x[1])
        return resources

    async def _get_download_urls_batch(self, resources: List[Tuple[str, int]], batch_id: str) -> List[Dict]:
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
        """Quản lý việc tải và xử lý theo từng đợt."""
        remaining_resources = resources.copy()
        while remaining_resources:
            current_files = self._count_temp_files()
            available_slots = self.config.max_concurrent_downloads - current_files

            if available_slots <= 0:
                logger.warning(f"Tiến hành xóa toàn bộ file để giải phóng không gian.")
                
                temp_files = self._get_temp_files_list()
                
                if temp_files:
                    await self._cleanup_media_files(temp_files, f"batch_{batch_id}")
                
                current_files = self._count_temp_files()
                available_slots = self.config.max_concurrent_downloads - current_files
                logger.info(f"Sau khi dọn dẹp, thư mục tạm còn {current_files} files, {available_slots} slots trống.")
                
                if available_slots <= 0:
                    logger.error(f"Thư mục tạm vẫn đầy sau khi dọn dẹp. Còn {len(remaining_resources)} resources chưa xử lý.")
                    break

            to_download = remaining_resources[:available_slots]
            remaining_resources = remaining_resources[available_slots:]

            logger.info(f"Downloading batch of {len(to_download)} files. Remaining: {len(remaining_resources)}.")
            downloaded_files = await self._download_batch(to_download, batch_id)

            if downloaded_files:
                await self._process_media_files(downloaded_files, batch_id)
    
    async def _download_batch(self, resources: List[Dict], batch_id: str) -> List[Dict]:
        """Tải một loạt media."""
        tasks = [
            self._download_media_safe(
                res['resource_id'], res['url_data'], res['duration'], batch_id
            ) for res in resources
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_files = [res for res in results if not isinstance(res, Exception)]
        logger.info(f"Successfully downloaded {len(valid_files)}/{len(resources)} files for batch - batch_id={batch_id}")
        return valid_files
        
    async def _get_download_url_safe(self, resource_id: str, duration: int, batch_id: str) -> Dict:
        """Lấy URL download một cách an toàn."""
        try:
            result = await get_download_url(id=resource_id)
            logger.info(f"Successfully fetched URL for resource_id: {resource_id}")
            return result
        except Exception as e:
            logger.error(f"Get download URL failed - batch_id={batch_id}, resource_id={resource_id}, error={e}")
            raise

    async def _process_media_files(self, downloaded_files: List[Dict], batch_id: str):
        """Xử lý đồng thời một danh sách các file đã được tải xuống."""
        logger.info(f"Processing {len(downloaded_files)} downloaded files - batch_id={batch_id}")
        tasks = [self._process_single_media_concurrent(file_info, batch_id) for file_info in downloaded_files]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_single_media_concurrent(self, file_info: Dict, batch_id: str):
        """Wrapper để kiểm soát số lượng xử lý đồng thời bằng semaphore."""
        resource_id = file_info['resource_id']
        async with self.processing_semaphore:
            logger.info(f"Acquired semaphore for processing - resource_id={resource_id}")
            await self._process_media(file_info, batch_id)
            logger.info(f"Released semaphore for processing - resource_id={resource_id}")
            
    async def _upsert_stock_vector(self, points: Dict, resource_id: str, media_type: str) -> Dict:
        """Gửi vector với logic retry."""
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
                logger.info(f"Upsert stock vector success - resource_id={resource_id}, media_type={media_type}")
                return response.json()
            except requests.RequestException as e:
                logger.warning(f"Upsert stock vector attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)

        raise RuntimeError(f"Upsert stock vector failed after {max_retries} retries for resource {resource_id}")
    
    async def _batch_cleanup_temp_folder(self, batch_id: str):
        """Dọn dẹp các file cũ hơn 1 giờ trong thư mục tạm."""
        try:
            old_files = []
            current_time = datetime.now().timestamp()
            
            file_patterns = self._get_file_patterns()
            
            for pattern in file_patterns:
                for file_path in self.config.temp_dir.glob(pattern):
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

    # Các phương thức trừu tượng mà lớp con phải triển khai
    
    @abstractmethod
    def _count_temp_files(self) -> int:
        """Đếm số file media trong thư mục tạm."""
        pass
        
    @abstractmethod
    def _get_file_patterns(self) -> List[str]:
        """Lấy danh sách các pattern để tìm files (*.mp4, *.mp3, etc.)"""
        pass
        
    @abstractmethod
    def _get_temp_files_list(self) -> List[Tuple[str, str]]:
        """Lấy danh sách các file tạm để dọn dẹp."""
        pass
    
    @abstractmethod
    async def _download_media_safe(self, resource_id: str, url_data: Dict, duration: int, batch_id: str) -> Dict:
        """Tải media an toàn."""
        pass
        
    @abstractmethod
    async def _process_media(self, file_info: Dict, batch_id: str):
        """Xử lý một file media."""
        pass
        
    @abstractmethod
    async def _cleanup_media_files(self, cleanup_files: List[Tuple[str, str]], resource_id: str, batch_id: str = None):
        """Dọn dẹp files."""
        pass