import os
import torch
from qdrant_client import QdrantClient, models
from typing import List, Dict, Any
from sentence_transformers import SentenceTransformer

from config.settings import get_settings


class VectorStorage:
    """Handles vector database operations and embedding generation."""
    
    def __init__(self):
        self.model = SentenceTransformer(
            os.path.join(os.getcwd(), get_settings().MODEL_PATH), 
            device='cuda' if torch.cuda.is_available() else 'cpu'
        )
        self.client = QdrantClient(
            host=get_settings().QDRANT_HOST,
            port=get_settings().QDRANT_PORT
        )

    async def embedding(self, text: str):
        """Generate embedding for text."""
        try:
            return self.model.encode(text).tolist()
        except Exception as e:
            raise RuntimeError(f"Không thể tạo embedding: {e}")
        
    def detect_media_type(self, point: Dict[str, Any]) -> str:
        """Detect media type from point data."""
        if "audio_description" in point:
            return "audio"
        elif "video_description" in point:
            return "video"

    async def handle_point(self, point: Dict[str, Any], id: str, media_type: str = None):
        """Process point data and create PointStruct for vector storage."""
        if media_type is None:
            media_type = self.detect_media_type(point)
        
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
            
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"Lỗi khi xử lý point id={id}: {e}")

    async def _handle_video_point(self, point: Dict[str, Any], id: str):
        """Handle video point processing."""
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
        """Handle audio point processing."""
        audio_description = point.get("audio_description", "")
        
        tags = []
        for key, value in point.items():
            if key not in ["audio_description", "is_real", "time"]:
                if isinstance(value, list):
                    tags.extend([str(item) for item in value if item])
                elif value:
                    tags.append(str(value))
        
        tags_text = ", ".join(tags)

        point_struct = models.PointStruct(
            id=id,
            vector={
                'description': await self.embedding(text=audio_description),
                'tags': await self.embedding(text=tags_text)
            },
            payload=point
        )
        
        return point_struct

    async def create_collection(self, collection_name: str, media_type: str):
        """Create a new collection for the specified media type."""
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
                'tags': models.VectorParams(
                    size=get_settings().VECTOR_SIZE,
                    distance=models.Distance.COSINE,
                    on_disk=True,
                    datatype=models.Datatype.FLOAT32,
                )
            }
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
        
        self.client.create_collection(
            collection_name=collection_name,
            vectors_config=vectors_config,
            optimizers_config=optimizers_config,
            hnsw_config=hnsw_config,
        )

    async def upsert_points(
        self,
        collection_name: str,
        points: List[Dict[str, Any]],
        ids: List[str],
        media_type: str = None 
    ):
        """Add or update points in collection."""
        if not self.client.collection_exists(collection_name):
            # Determine media type if not provided
            detected_media_type = media_type if media_type else self.detect_media_type(points[0])
            await self.create_collection(collection_name=collection_name, media_type=detected_media_type)

        try:
            point_structs = []
            # Prioritize provided media_type, otherwise auto-detect
            detected_media_type = media_type if media_type else self.detect_media_type(points[0])
            
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
            raise RuntimeError(f"Lỗi khi upsert points vào '{collection_name}': {e}")
        
    async def delete_points(
        self,
        collection_name: str,
        ids: List[str]
    ):
        """Delete points from collection."""
        try:
            if not self.client.collection_exists(collection_name):
                raise RuntimeError(f"Collection '{collection_name}' không tồn tại để xóa points.")

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
            raise RuntimeError(f"Lỗi khi delete points từ '{collection_name}': {e}")