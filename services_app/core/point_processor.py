import os
import torch
import asyncio
import logging

from qdrant_client import QdrantClient, models
from typing import List, Dict,Any

from sentence_transformers import SentenceTransformer

from settings import get_settings

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

    async def handle_point(
        self,
        point: Dict[str, Any],
        id: str
    ):
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
        """
        Example point structure:
        {
            "overview": [],
            "entities": {
                "person": [],
                "animal": [],
                "object": []
            },
            "actions_events_festivals": [],
            "scene_context": {
                "type": [],
                "human_made_area": [],
                "natural_environment": []
            },
            "temporal_context": {
                "part_of_day": [],
                "season": [],
                "weather": [],
                "lighting": []
            },
            "affective_mood_vibe": [],
            "color": [],
            "visual_attributes": [],
            "camera_techniques": [],
            "video_description": ""
        }
        """
        try:
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
        except RuntimeError as re:
            logger.error(f"[handle_point] RuntimeError id={id}: {re}")
            raise

        except Exception as e:
            logger.exception(f"[handle_point] Unexpected error với id={id}: {e}")
            raise RuntimeError(f"Lỗi khi xử lý point id={id}: {e}")

    async def create_collection(
        self,
        collection_name: str
    ):
        self.client.create_collection(
            collection_name=collection_name,
            vectors_config={
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
            },
            optimizers_config=models.OptimizersConfigDiff(
                memmap_threshold=1000000
            ),
            hnsw_config=models.HnswConfigDiff(
                m=256,
                ef_construct=1024,
                on_disk=True
            ),
        )

    async def upsert_points(
        self,
        collection_name: str,
        points: List[Dict[str, Any]],
        ids: List[str]
    ):
        if not self.client.collection_exists(collection_name):
            await self.create_collection(collection_name=collection_name)

        try:
            point_structs = []
            for point, id in zip(points, ids):
                point_structs.append(await self.handle_point(point, id))

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

    async def delete_points(
        self,
        collection_name: str,
        ids: List[str]
    ):

        try:
            if not self.client.collection_exists(collection_name):
                msg = f"Collection '{collection_name}' không tồn tại để xóa points."
                logger.error(f"[delete_points] {msg}")
                raise RuntimeError(msg)

            self.client.delete(
                collection_name=collection_name,
                points_selector=models.PointIdsList(
                    points=ids,
                ),
            )

            return ids

        except RuntimeError:
            raise
        except Exception as e:
            logger.exception(f"[delete_points] Lỗi không xác định khi delete points từ '{collection_name}': {e}")
            raise RuntimeError(f"Lỗi khi delete points từ '{collection_name}': {e}")