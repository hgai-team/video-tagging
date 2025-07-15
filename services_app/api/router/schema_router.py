import logging
from fastapi import APIRouter, HTTPException, Query

from utils.schema_updater import SchemaUpdater

logger = logging.getLogger(__name__)

app = APIRouter(
    prefix='/schema',
    tags=['Schema']
)

@app.post('/update')
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