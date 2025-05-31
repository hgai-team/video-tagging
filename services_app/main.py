import uvicorn

from fastapi import FastAPI
from contextlib import asynccontextmanager

from api.endpoints import app as tagging_endpoints

@asynccontextmanager
async def lifespan(app: FastAPI):

    yield


app = FastAPI(
    title="Tagging Service",
    description="",
    version="3.0.1",
    lifespan=lifespan,
)

@app.get(
    '/api/v3/health-check'
)
async def health_check():
    return {"status": 200}

app.include_router(tagging_endpoints)

if __name__=="__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
    )
