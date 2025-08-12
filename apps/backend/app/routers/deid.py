from __future__ import annotations
from fastapi import APIRouter
from pydantic import BaseModel
from app.services.deid import DeidService

router = APIRouter()

class DeidRequest(BaseModel):
    text: str
    policy: str | None = None

@router.post("/text")
async def deid_text(req: DeidRequest):
    result = await DeidService().deidentify_text(req.text, policy_name=req.policy)
    return {"result": result}