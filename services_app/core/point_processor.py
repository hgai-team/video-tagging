import os
import torch
import logging

from qdrant_client import QdrantClient, models
from typing import List, Dict,Any

from sentence_transformers import SentenceTransformer

from config.settings import get_settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("./logs/point_processor.log"), logging.StreamHandler()]
)

class PointProcessor:
    def __init__(self):
        self.model = SentenceTransformer(os.path.join(os.getcwd(), get_settings().MODEL_PATH), device='cuda' if torch.cuda.is_available() else 'cpu')
        self.client = QdrantClient(
            host=get_settings().QDRANT_HOST,
            port=get_settings().QDRANT_PORT
        )

    async def embedding(
        self,
        text: str
    ):
        try:
            return self.model.encode(text).tolist()
        except Exception as e:
            logger.exception(f"[embedding] Lỗi khi encode text: {e} | text: {text}")
            raise RuntimeError(f"Không thể tạo embedding: {e}")
        
    def _detect_media_type(self, point: Dict[str, Any]) -> str:
        
        if "audio_description" in point:
            return "audio"
        
        elif "video_description" in point:
            return "video"

    async def handle_point(self, point: Dict[str, Any], id: str, media_type: str = None):

        if media_type is None:
            media_type = self._detect_media_type(point)
        
        try:
            if media_type == "video":
                if "is_real" not in point:
                    point["is_real"] = "untagged"
                
            if "time" not in point:
                point["time"] = 0

            if media_type == "audio":
                point_struct = await self._handle_audio_point(point, id)
            else:  
                point_struct = await self._handle_video_point(point, id)
            
            return point_struct
            
        except RuntimeError as re:
            logger.error(f"[handle_point] RuntimeError id={id}: {re}")
            raise
        except Exception as e:
            logger.exception(f"[handle_point] Unexpected error với id={id}: {e}")
            raise RuntimeError(f"Lỗi khi xử lý point id={id}: {e}")

    async def _handle_video_point(self, point: Dict[str, Any], id: str):

        def extract_list_strings(obj):
            strings = []
            if isinstance(obj, dict):
                for v in obj.values():
                    strings.extend(extract_list_strings(v))
            elif isinstance(obj, list):
                for item in obj:
                    if isinstance(item, str):
                        strings.append(item)
                    else:
                        strings.extend(extract_list_strings(item))
            return strings
        
        str_keys = ",".join(extract_list_strings(point))
        des = point.get("video_description", "")
        
        point_struct = models.PointStruct(
            id=id,
            vector={
                'description': await self.embedding(text=des),
                'keywords': await self.embedding(text=str_keys)
            },
            payload=point
        )
        
        return point_struct
        
    async def _handle_audio_point(self, point: Dict[str, Any], id: str):
        """Xử lý point cho audio - logic mới với 3 vector."""
        audio_description = point.get("audio_description", "")
        
        # 2. Lấy thông tin project
        project_info = ""
        if "project" in point and point["project"]:
            if isinstance(point["project"], list):
                project_info = ", ".join([str(p) for p in point["project"] if p])
            else:
                project_info = str(point["project"])
        
        # 3. Trích xuất tất cả tags còn lại
        tags = []
        for key, value in point.items():
            if key not in ["audio_description", "project", "is_real", "time"]:
                if isinstance(value, list):
                    tags.extend([str(item) for item in value if item])
                elif value:
                    tags.append(str(value))
        tags_text = ", ".join(tags)

        point_struct = models.PointStruct(
            id=id,
            vector={
                'description': await self.embedding(text=audio_description),
                'project': await self.embedding(text=project_info),
                'tags': await self.embedding(text=tags_text)
            },
            payload=point
        )
        
        return point_struct

    async def create_collection(self, collection_name: str, media_type: str):

        if media_type not in ["video", "audio"]:
            raise ValueError('media_type phải là "video" hoặc "audio"')
        
        optimizers_config = models.OptimizersConfigDiff(
            memmap_threshold=1000000
        )
        
        hnsw_config = models.HnswConfigDiff(
            m=256,
            ef_construct=1024,
            on_disk=True
        )
        
        if media_type == "audio":
            vectors_config = {
                'description': models.VectorParams(
                    size=get_settings().VECTOR_SIZE,
                    distance=models.Distance.COSINE,
                    on_disk=True,
                    datatype=models.Datatype.FLOAT32,
                ),
                'project': models.VectorParams(
                    size=get_settings().VECTOR_SIZE,
                    distance=models.Distance.COSINE,
                    on_disk=True,
                    datatype=models.Datatype.FLOAT32,
                ),
                'tags': models.VectorParams(
                    size=get_settings().VECTOR_SIZE,
                    distance=models.Distance.COSINE,
                    on_disk=True,
                    datatype=models.Datatype.FLOAT32,
                )
            }
            logger.info(f"Tạo collection audio '{collection_name}' với 3 vector: description, project, tags")
        else: 
            vectors_config = {
                'description': models.VectorParams(
                    size=get_settings().VECTOR_SIZE,
                    distance=models.Distance.COSINE,
                    on_disk=True,
                    datatype=models.Datatype.FLOAT32,
                ),
                'keywords': models.VectorParams(
                    size=get_settings().VECTOR_SIZE,
                    distance=models.Distance.COSINE,
                    on_disk=True,
                    datatype=models.Datatype.FLOAT32,
                )
            }
            logger.info(f"Tạo collection video '{collection_name}' với 2 vector: description, keywords")
        
        self.client.create_collection(
            collection_name=collection_name,
            vectors_config=vectors_config,
            optimizers_config=optimizers_config,
            hnsw_config=hnsw_config,
        )
        
        logger.info(f"Đã tạo collection {collection_name} thành công với loại media: {media_type}")

async def upsert_points(
    self,
    collection_name: str,
    points: List[Dict[str, Any]],
    ids: List[str],
    media_type: str = None
):

    if not self.client.collection_exists(collection_name):
        # Xác định loại media nếu chưa được cung cấp
        detected_media_type = media_type or self._detect_media_type(points[0])
        await self.create_collection(collection_name=collection_name, media_type=detected_media_type)

    try:
        point_structs = []
        detected_media_type = media_type or self._detect_media_type(points[0])
        
        for point, id in zip(points, ids):
            point_structs.append(await self.handle_point(point, id, media_type=detected_media_type))

        self.client.upsert(
            collection_name=collection_name,
            points=point_structs
        )
        return point_structs
    except RuntimeError:
        raise
    except Exception as e:
        logger.exception(f"[upsert_points] Lỗi không xác định khi upsert vào '{collection_name}': {e}")
        raise RuntimeError(f"Lỗi khi upsert points vào '{collection_name}': {e}")