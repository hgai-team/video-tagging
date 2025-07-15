from fastapi import APIRouter

from .router.video_router import app as tagging_video_router
from .router.tags_router import app as tagging_tags_router
from .router.stock_router import app as tagging_stock_router
from .router.task_router import app as tagging_task_router  
from .router.schema_router import app as schema_task_router

app = APIRouter(
    prefix='/api/v3'
)

app.include_router(tagging_video_router)
app.include_router(tagging_tags_router)
app.include_router(tagging_stock_router)
app.include_router(tagging_task_router)  
app.include_router(schema_task_router)