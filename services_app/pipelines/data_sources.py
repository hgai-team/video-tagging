#data_sources.py
import asyncio
import abc
import logging
import requests
from typing import List, Dict
from config.settings import get_settings
from api.router.stock_router import get_unlabel_resources, get_resources_old_version

logger = logging.getLogger(__name__)

class BaseDataSource(abc.ABC):
    """Lớp trừu tượng cho các nguồn dữ liệu."""
    @abc.abstractmethod
    async def get_resources(self, **kwargs) -> List[Dict]:
        """Lấy danh sách resource để xử lý. Mỗi resource là một dict có 'id' và 'duration'."""
        pass

class UnlabeledDataSource(BaseDataSource):
    async def get_resources(self, start_date: str, end_date: str, media_type: int, **kwargs) -> List[Dict]:
        logger.info(f"Fetching unlabeled resources from {start_date} to {end_date}")
        return await get_unlabel_resources(
            media_type=media_type,
            start_date=start_date,
            end_date=end_date
        )

class OldVersionDataSource(BaseDataSource):
    async def get_resources(self, current_tag_version: str, media_type: int, **kwargs) -> List[Dict]:
        logger.info(f"Fetching resources with tag version older than {current_tag_version}")
        return await get_resources_old_version(
            current_tag_version=current_tag_version,
            media_type=media_type
        )

class MissUpsertDataSource(BaseDataSource):
    async def get_resources(self, start_date: str, end_date: str, media_type: int, collection_name: str, **kwargs) -> List[Dict]:
        logger.info(f"Fetching miss-upsert resources from {start_date} to {end_date}")
        settings = get_settings()

        # Step 1: Get all resources in time range with their tag versions
        payload_info = {"startDate": start_date, "endDate": end_date, "mediaType": media_type}
        headers_info = {"Accept": "text/plain", "X-Time-Zone": "Asia/Bangkok"}

        try:
            response_info = requests.post(url=settings.FILE_INFO_TAG_VERSION, headers=headers_info, json=payload_info)
            response_info.raise_for_status()
            all_resources = response_info.json()
        except requests.RequestException as e:
            logger.error(f"Failed to get file info by tag version: {e}")
            return []

        # Filter for IDs with the correct tag version
        target_ids = [res['id'] for res in all_resources if res.get('tagVersion') == kwargs.get('tag_version')]

        if not target_ids:
            logger.info("No resources with the target tag version found in the date range.")
            return []

        # Step 2: Check which of these IDs are missing from the vector collection
        headers_retrieve = {"Accept": "application/json", "Content-Type": "application/json"}
        url_retrieve = f"{settings.TAG_DOMAIN}af/collections/{collection_name}/points/retrieve"

        try:
            # Note: This API seems to return IDs that are *missing*. The original code's logic was a bit unclear.
            # Assuming the API correctly returns the list of IDs that need to be upserted.
            response_retrieve = requests.post(url=url_retrieve, headers=headers_retrieve, json=target_ids, timeout=360)
            response_retrieve.raise_for_status()
            missing_ids = response_retrieve.json()
        except requests.RequestException as e:
            logger.error(f"Failed to retrieve missing points from collection: {e}")
            return []

        logger.info(f"Found {len(missing_ids)} missing points to process.")

        # Return in the expected format, using a default duration as it's not provided
        return [{'id': point_id, 'duration': 20} for point_id in missing_ids]

class TaggedUnclassifiedDataSource(BaseDataSource):
    """Nguồn dữ liệu cho các video đã tagged nhưng chưa phân loại real/AI."""
    async def get_resources(self, collection_name: str, **kwargs) -> List[Dict]:
        logger.info(f"Fetching tagged but unclassified videos from collection {collection_name}")
        try:
            settings = get_settings()
            batch_size = kwargs.get('batch_size', 100)
            offset = kwargs.get('offset', 0)
            
            # Sử dụng URL API chính xác
            url = f"{settings.TAG_DOMAIN}af/collections/{collection_name}/points/tagged"
            
            # Tham số query
            params = {
                "is_real": "untagged",
                "limit": batch_size,
                "offset": offset
            }
            
            headers = {'Accept': 'application/json'}
            
            # Gọi API bất đồng bộ
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.get(url=url, headers=headers, params=params, timeout=60)
            )
            
            # Kiểm tra response
            if response.status_code != 200:
                logger.error(f"API call failed: {response.status_code} - {response.text}")
                return []
                
            # Parse response
            response_data = response.json()
            if not response_data.get('success', False):
                logger.error(f"API returned error: {response_data.get('error')}")
                return []
                
            video_ids = response_data.get('data', {}).get('video_ids', [])
            
            # Chuyển đổi thành định dạng phù hợp với pipeline
            resources = [{'id': video_id, 'duration': 0} for video_id in video_ids]
            
            logger.info(f"Found {len(resources)} tagged but unclassified videos")
            return resources
            
        except Exception as e:
            logger.error(f"Error in TaggedUnclassifiedDataSource: {str(e)}")
            return []