import logging
from qdrant_client import QdrantClient
from config.settings import get_settings

logger = logging.getLogger(__name__)

class SchemaUpdater:
    def __init__(self, collection_name: str):
        self.collection_name = collection_name
        self.client = QdrantClient(
            host=get_settings().QDRANT_HOST,
            port=get_settings().QDRANT_PORT,
            check_compatibility=False
        )

    async def update_all_points(self):
        result = {
            "success": False,
            "total_points": 0,
            "updated_count": 0,
            "error": None
        }

        try:
            if not self.client.collection_exists(self.collection_name):
                error_msg = f"Collection '{self.collection_name}' không tồn tại"
                logger.error(error_msg)
                result["error"] = error_msg
                return result

            count_res = self.client.count(collection_name=self.collection_name, exact=True)
            total_points = count_res.count
            result["total_points"] = total_points
            logger.info(f"Tổng số điểm trong collection: {total_points}")

            if total_points == 0:
                logger.warning("Không có điểm dữ liệu nào để cập nhật.")
                result["success"] = True
                return result

            batch_size = 100
            offset = None
            updated_count = 0

            while True:
                points_batch, offset = self.client.scroll(
                    collection_name=self.collection_name,
                    with_payload=True,
                    limit=batch_size,
                    offset=offset  
                )

                if not points_batch:
                    break

                for point in points_batch:
                    pid = point.id
                    payload = point.payload or {}

                    changed = False
                    if "is_real" not in payload:
                        payload["is_real"] = "untagged"
                        changed = True
                    if "time" not in payload:
                        payload["time"] = 0
                        changed = True

                    if changed:
                        try:
                            self.client.set_payload(
                                collection_name=self.collection_name,
                                payload=payload,
                                points=[pid]
                            )
                            updated_count += 1
                        except Exception as e:
                            logger.error(f"Lỗi khi cập nhật điểm {pid}: {e}")

                logger.info(f"Đã cập nhật {updated_count}/{total_points} điểm "
                            f"({updated_count/total_points*100:.1f}%).")

                if offset is None:
                    break

            result["updated_count"] = updated_count
            result["success"] = True
            logger.info(f"Hoàn thành cập nhật schema: {updated_count}/{total_points} điểm.")
            
        except Exception as e:
            error_msg = f"Lỗi khi cập nhật schema: {str(e)}"
            logger.error(error_msg)
            result["error"] = error_msg
            
        return result