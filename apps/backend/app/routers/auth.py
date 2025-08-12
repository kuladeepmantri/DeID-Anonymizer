from __future__ import annotations
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.core.security import create_access_token

router = APIRouter()

class LoginRequest(BaseModel):
    username: str
    password: str

@router.post("/login")
async def login(req: LoginRequest):
    # Placeholder: accept any non-empty credentials for dev
    if not req.username or not req.password:
        raise HTTPException(status_code=400, detail="Invalid credentials")
    token = create_access_token(subject=req.username)
    return {"access_token": token, "token_type": "bearer"}
