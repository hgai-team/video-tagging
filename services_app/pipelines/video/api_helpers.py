import asyncio
import logging
import requests
from typing import Dict, List, Optional

from config.settings import get_settings

logger = logging.getLogger(__name__)

async def update_point_metadata_via_payload_api(
    collection_name: str, 
    point_id: str, 
    is_real: Optional[int] = None, 
    time: Optional[int] = None,
    bpm: Optional[int] = None,
    tone: Optional[str] = None,
    scale: Optional[str] = None,
    intro_chords: Optional[List] = None,
    outro_chords: Optional[List] = None,
    max_retries: int = 3
) -> Dict:
    
    settings = get_settings()
    
    payload = {}
    if is_real is not None:
        payload["is_real"] = str(is_real)  
    if time is not None:
        payload["time"] = time
    if bpm is not None:
        payload["bpm"] = bpm
    if tone is not None:
        payload["tone"] = tone
    if scale is not None:
        payload["scale"] = scale
    if intro_chords is not None:
        payload["intro_chords"] = intro_chords
    if outro_chords is not None:
        payload["outro_chords"] = outro_chords
        
    if not payload:
        logger.error("No update parameters provided")
        return {"success": False, "error": "No update parameters provided"}
    
    request_data = {
        "payload": payload,    
        "points": [point_id]   
    }
    
    url = f"{settings.TAG_DOMAIN}af/collections/{collection_name}/points/payload"
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    
    logger.debug(f"Update payload request: {request_data}")
    
    for attempt in range(max_retries):
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.post(url=url, headers=headers, json=request_data, timeout=60)
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully updated metadata for point {point_id}")
                return {"success": True, "data": {"message": f"Đã cập nhật metadata cho điểm {point_id}"}}
            
            logger.error(f"Failed to update metadata: {response.status_code} - {response.text}")

            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt) 
                
        except Exception as e:
            logger.error(f"Error updating metadata: {str(e)}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
    
    return {"success": False, "error": f"Failed to update metadata after {max_retries} attempts"}

async def update_point_partial_payload_local(
    collection_name: str,
    point_id: str, 
    partial_payload: Dict
) -> Dict:
    """
    Cập nhật partial payload cho point trong local Qdrant (chỉ update các field trong partial_payload)
    """
    try:
        settings = get_settings()
        
        from qdrant_client import QdrantClient
        client = QdrantClient(
            host=settings.QDRANT_HOST,
            port=settings.QDRANT_PORT
        )
        
        # Sử dụng set_payload để update partial
        client.set_payload(
            collection_name=collection_name,
            payload=partial_payload,
            points=[point_id]
        )
        
        logger.info(f"Successfully updated local partial payload for point {point_id} with fields: {list(partial_payload.keys())}")
        return {"success": True, "data": {"message": f"Đã cập nhật local payload cho điểm {point_id}"}}
        
    except Exception as e:
        logger.error(f"Error updating local partial payload: {str(e)}")
        return {"success": False, "error": str(e)}

async def update_point_full_payload_api(
    collection_name: str,
    point_id: str,
    payload_data: Dict,
    max_retries: int = 3
) -> Dict:
    """
    Cập nhật payload đầy đủ cho một point với dữ liệu tùy ý
    """
    settings = get_settings()
    
    if not payload_data:
        logger.error("No payload data provided")
        return {"success": False, "error": "No payload data provided"}
    
    request_data = {
        "payload": payload_data,
        "points": [point_id]
    }
    
    url = f"{settings.TAG_DOMAIN}af/collections/{collection_name}/points/payload"
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    
    logger.debug(f"Full payload update request: {request_data}")
    
    for attempt in range(max_retries):
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.post(url=url, headers=headers, json=request_data, timeout=60)
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully updated full payload for point {point_id}")
                return {"success": True, "data": {"message": f"Đã cập nhật payload cho điểm {point_id}"}}
            
            logger.error(f"Failed to update payload: {response.status_code} - {response.text}")

            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
                
        except Exception as e:
            logger.error(f"Error updating payload: {str(e)}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
    
    return {"success": False, "error": f"Failed to update payload after {max_retries} attempts"}

async def update_multiple_points_metadata(
    collection_name: str,
    point_ids: List[str],
    is_real: Optional[int] = None,
    time: Optional[int] = None,
    max_retries: int = 3
) -> Dict:
    """
    Cập nhật metadata (is_real, time) cho nhiều điểm cùng lúc
    sử dụng API /points/payload.
    
    Args:
        collection_name: Tên collection
        point_ids: Danh sách ID của các điểm cần cập nhật
        is_real: 1 cho video thật, 0 cho video AI-generated
        time: Thời lượng của video (seconds)
        max_retries: Số lần thử lại tối đa
    
    Returns:
        Dict: Kết quả cập nhật
    """
    settings = get_settings()
    
    if not point_ids:
        return {"success": True, "data": {"message": "Không có điểm nào để cập nhật"}}

    payload = {}
    if is_real is not None:
        payload["is_real"] = str(is_real)
    if time is not None:
        payload["time"] = time
        
    if not payload:
        logger.error("No update parameters provided")
        return {"success": False, "error": "No update parameters provided"}

    request_data = {
        "payload": payload,  # Chỉ các trường cần cập nhật
        "points": point_ids  # Danh sách point IDs cần cập nhật
    }
    
    url = f"{settings.TAG_DOMAIN}af/collections/{collection_name}/points/payload"
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

    logger.debug(f"Batch update payload request: {len(point_ids)} points")

    for attempt in range(max_retries):
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.post(url=url, headers=headers, json=request_data, timeout=60)
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully updated metadata for {len(point_ids)} points")
                return {
                    "success": True, 
                    "data": {
                        "message": f"Đã cập nhật metadata cho {len(point_ids)} điểm",
                        "count": len(point_ids)
                    }
                }

            logger.error(f"Failed to update metadata: {response.status_code} - {response.text}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
                
        except Exception as e:
            logger.error(f"Error updating metadata: {str(e)}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
    
    return {"success": False, "error": f"Failed to update metadata after {max_retries} attempts"}