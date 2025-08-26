import logging
import random
from typing import Dict, Any, List
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from config.settings import get_settings

logger = logging.getLogger(__name__)

class CollectionCopier:
    def __init__(self):
        self.client = QdrantClient(
            host=get_settings().QDRANT_HOST,
            port=get_settings().QDRANT_PORT,
            timeout=10000
        )

    async def copy_collection(self, 
                             source_collection: str, 
                             target_collection: str, 
                             sample_size: int = 0, 
                             recreate_target: bool = False) -> Dict[str, Any]:
        result = {
            "success": False,
            "source_points": 0,
            "copied_points": 0,
            "error": None
        }
        
        try:
            # Kiểm tra collection nguồn
            if not self.client.collection_exists(source_collection):
                error_msg = f"Collection nguồn '{source_collection}' không tồn tại"
                logger.error(error_msg)
                result["error"] = error_msg
                return result
                
            collection_info = self.client.get_collection(collection_name=source_collection)
            vectors_config = self._get_vectors_config(collection_info)
            
            if recreate_target or not self.client.collection_exists(target_collection):
                if self.client.collection_exists(target_collection):
                    logger.info(f"Xóa collection đích '{target_collection}' cũ")
                    self.client.delete_collection(collection_name=target_collection)
                
                logger.info(f"Tạo collection đích '{target_collection}' mới")
                self.client.recreate_collection(
                    collection_name=target_collection,
                    vectors_config=vectors_config
                )
            
            # Lấy danh sách ID từ collection nguồn
            points_ids = self._get_all_point_ids(source_collection)
            result["source_points"] = len(points_ids)
            logger.info(f"Tìm thấy {len(points_ids)} điểm trong collection nguồn")
            
            # Chọn mẫu nếu cần
            if sample_size > 0 and sample_size < len(points_ids):
                selected_ids = random.sample(points_ids, sample_size)
                logger.info(f"Đã chọn ngẫu nhiên {len(selected_ids)}/{len(points_ids)} điểm để sao chép")
            else:
                selected_ids = points_ids
                logger.info(f"Sao chép tất cả {len(selected_ids)} điểm")
            
            # Xử lý theo batch để tránh quá tải
            batch_size = 1000  # Giảm kích thước batch
            total_copied = 0
            
            for i in range(0, len(selected_ids), batch_size):
                batch_ids = selected_ids[i:i + batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(selected_ids) + batch_size - 1) // batch_size
                
                logger.info(f"Xử lý batch {batch_num}/{total_batches} - {len(batch_ids)} điểm")
                
                try:
                    # Lấy dữ liệu từ batch hiện tại
                    points = self.client.retrieve(
                        collection_name=source_collection,
                        ids=batch_ids,
                        with_vectors=True,
                        with_payload=True
                    )
                    
                    # Chuyển đổi sang định dạng để upsert
                    points_to_insert = []
                    for point in points:
                        points_to_insert.append(
                            PointStruct(
                                id=point.id,
                                vector=point.vector,
                                payload=point.payload
                            )
                        )
                    
                    # Upsert batch hiện tại vào collection đích
                    if points_to_insert:
                        self.client.upsert(
                            collection_name=target_collection,
                            points=points_to_insert
                        )
                        total_copied += len(points_to_insert)
                        logger.info(f"✅ Đã sao chép batch {batch_num}/{total_batches} - {len(points_to_insert)} điểm (Tổng: {total_copied}/{len(selected_ids)})")
                
                except Exception as e:
                    logger.error(f"❌ Lỗi khi xử lý batch {batch_num}: {str(e)}")
                    # Tiếp tục với batch tiếp theo thay vì dừng hoàn toàn
                    continue
            
            result["copied_points"] = total_copied
            result["success"] = True
            logger.info(f"🎉 Hoàn thành sao chép: {total_copied}/{len(selected_ids)} điểm")
            
        except Exception as e:
            error_msg = f"Lỗi khi sao chép collection: {str(e)}"
            logger.error(error_msg)
            result["error"] = error_msg
            
        return result
    
    def _get_vectors_config(self, collection_info) -> Dict:
        """Lấy cấu hình vector từ thông tin collection."""
        try:
            # Thử các cách khác nhau để truy cập cấu hình vector
            if hasattr(collection_info.config.params, 'vectors_config'):
                return collection_info.config.params.vectors_config
            elif hasattr(collection_info.config.params, 'vectors'):
                return collection_info.config.params.vectors
            elif isinstance(collection_info.config, dict):
                if 'params' in collection_info.config:
                    params = collection_info.config['params']
                    if 'vectors_config' in params:
                        return params['vectors_config']
                    elif 'vectors' in params:
                        return params['vectors']
        except (AttributeError, TypeError, KeyError) as e:
            logger.warning(f"Không thể xác định cấu hình vector: {e}")
        
        # Sử dụng cấu hình mặc định
        logger.warning("Sử dụng cấu hình vector mặc định")
        return {
            "description": {"size": 1536, "distance": "Cosine"},
            "keywords": {"size": 1536, "distance": "Cosine"}
        }
    
    def _get_all_point_ids(self, collection_name: str) -> List[str]:
        """Lấy tất cả ID điểm từ một collection."""
        points_ids = []
        offset = None
        limit = 1000
        
        while True:
            batch, offset = self.client.scroll(
                collection_name=collection_name,
                limit=limit,
                offset=offset,
                with_payload=False,
                with_vectors=False
            )
            
            if not batch:
                break
                
            points_ids.extend([point.id for point in batch])
            
            if offset is None:
                break
                
        return points_ids