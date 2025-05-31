import uvicorn

from fastapi import FastAPI
from contextlib import asynccontextmanager

from api.endpoints import app as manager_endpoints

@asynccontextmanager
async def lifespan(app: FastAPI):

    yield


app = FastAPI(
    title="Manager Service",
    description="",
    version="3.0.1",
    lifespan=lifespan,
)

@app.get(
    '/api/v1/health-check'
)
async def health_check():
    return {"status": 200}

app.include_router(manager_endpoints)

if __name__=="__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
    )
