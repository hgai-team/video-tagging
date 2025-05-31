from fastapi import APIRouter

from .router.video_router import app as tagging_video_router
from .router.tags_router import app as tagging_tags_router
from .router.stock_router import app as tagging_stock_router

app = APIRouter(
    prefix='/api/v3'
)

app.include_router(tagging_video_router)
app.include_router(tagging_tags_router)
app.include_router(tagging_stock_router)
