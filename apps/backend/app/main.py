from __future__ import annotations
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette import status

from app.routers import health, files, deid, auth, policies
from app.core.settings import Settings

settings = Settings()

app = FastAPI(title="De-identification & Privacy Platform", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in settings.allowed_origins.split(",")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/health", tags=["health"])
app.include_router(auth.router, prefix="/auth", tags=["auth"])
app.include_router(files.router, prefix="/files", tags=["files"])
app.include_router(deid.router, prefix="/deid", tags=["deid"])
app.include_router(policies.router, prefix="/policies", tags=["policies"])

@app.get("/")
async def root():
    return JSONResponse({"status": "ok", "service": app.title, "version": app.version}, status_code=status.HTTP_200_OK)
