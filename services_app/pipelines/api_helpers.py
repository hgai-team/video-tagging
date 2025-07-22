import asyncio
import logging
import requests
from typing import Dict
from config.settings import get_settings

logger = logging.getLogger(__name__)

async def update_point_metadata_via_api(collection_name: str, point_id: str, is_real: int = None, time: int = None, max_retries: int = 5) -> Dict:
    settings = get_settings()
    
    payload = {}
    if is_real is not None:
        payload["is_real"] = str(is_real)  
    if time is not None:
        payload["time"] = time
        
    if not payload:
        logger.error("No update parameters provided")
        return {"success": False, "error": "No update parameters provided"}
    
    request_data = {
        "points": [payload],  # List các point payload
        "ids": [point_id]     # List các point ID tương ứng
    }
    
    # Tạo URL API
    url = f"{settings.TAG_DOMAIN}af/collections/{collection_name}/points"
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    
    # Log request để debug
    logger.debug(f"Update request data: {request_data}")
    
    # Retry logic
    for attempt in range(max_retries):
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.put(url=url, headers=headers, json=request_data, timeout=60)
            )
            
            # Kiểm tra response
            if response.status_code == 200:
                logger.info(f"Successfully updated metadata for point {point_id}")
                return {"success": True, "data": {"message": f"Đã cập nhật metadata cho điểm {point_id}"}}
            
            # Log lỗi chi tiết
            logger.error(f"Failed to update metadata: {response.status_code} - {response.text}")
            
            # Nếu còn lần thử, chờ một chút rồi thử lại
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff: 1, 2, 4, 8 seconds
                
        except Exception as e:
            logger.error(f"Error updating metadata: {str(e)}")
            
            # Nếu còn lần thử, chờ một chút rồi thử lại
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
    
    # Nếu đã thử hết số lần mà vẫn thất bại
    return {"success": False, "error": f"Failed to update metadata after {max_retries} attempts"}