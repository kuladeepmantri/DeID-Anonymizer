from __future__ import annotations
from fastapi import APIRouter, UploadFile, File, HTTPException
from app.services.storage import StorageService

router = APIRouter()

@router.post("/upload")
async def upload(file: UploadFile = File(...)):
    content = await file.read()
    key = await StorageService().put_object(file.filename, content, content_type=file.content_type)
    return {"key": key}

@router.get("/list")
async def list_files():
    objs = await StorageService().list_objects()
    return {"objects": objs}