from typing import List, Dict, Any

from fastapi import APIRouter, Path, HTTPException

from core import point_pro


app = APIRouter(
    prefix='/tags',
    tags=['Tags']
)

@app.put(
    '/collections/{collection_name}/points',
)
async def upsert_points(
    *,
    collection_name: str = Path(...),
    points: List[Dict[str, Any]],
    ids: List[str]
):
    try:
        result = await point_pro.upsert_points(
            collection_name=collection_name,
            points=points,
            ids=ids
        )
        return {
            "success": True,
            "data": {"message": "Upsert thành công"},
            "error": None
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post(
    '/collections/{collection_name}/points/delete',
)
async def delete_points(
    *,
    collection_name: str = Path(...),
    ids: List[str]
):
    try:
        deleted_ids = await point_pro.delete_points(
            collection_name=collection_name,
            ids=ids
        )
        return {
            "success": True,
            "data": {"deleted": deleted_ids},
            "error": None
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
