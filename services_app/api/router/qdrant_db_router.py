import logging
from fastapi import APIRouter, HTTPException, Query

from utils.schema_updater import SchemaUpdater
from utils.collection_copier import CollectionCopier

logger = logging.getLogger(__name__)

app = APIRouter(
    prefix='/QdrantDB',
    tags=['QdrantDB']
)

@app.post('/schema-update')
async def update_schema(
    collection_name: str = Query(...)
):
    try:
        updater = SchemaUpdater(collection_name=collection_name)
        result = await updater.update_all_points()
        
        if result["success"]:
            return {
                "success": True,
                "data": {
                    "total_points": result["total_points"],
                    "updated_count": result["updated_count"],
                    "message": f"Đã cập nhật {result['updated_count']}/{result['total_points']} điểm."
                },
                "error": None
            }
        else:
            return {
                "success": False,
                "data": None,
                "error": result["error"] or "Cập nhật schema thất bại."
            }
    except Exception as e:
        logger.error(f"Lỗi khi cập nhật schema: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Lỗi khi cập nhật schema: {str(e)}")
    
@app.post('/copy-colelctions',)
async def copy_collection(
    source_collection: str = Query(...),
    target_collection: str = Query(...),
    sample_size: int = Query(0),
    recreate_target: bool = Query(False, description="Xóa và tạo lại collection đích nếu đã tồn tại")
):

    try:
        copier = CollectionCopier()
        result = await copier.copy_collection(
            source_collection=source_collection,
            target_collection=target_collection,
            sample_size=sample_size,
            recreate_target=recreate_target
        )
        
        if result["success"]:
            return {
                "success": True,
                "data": {
                    "source_points": result["source_points"],
                    "copied_points": result["copied_points"],
                    "message": f"Đã sao chép {result['copied_points']}/{result['source_points']} điểm từ '{source_collection}' sang '{target_collection}'."
                },
                "error": None
            }
        else:
            return {
                "success": False,
                "data": None,
                "error": result["error"] or "Sao chép collection thất bại."
            }
    except Exception as e:
        logger.error(f"Lỗi khi sao chép collection: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Lỗi khi sao chép collection: {str(e)}")