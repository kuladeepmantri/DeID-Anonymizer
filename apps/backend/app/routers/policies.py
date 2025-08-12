from __future__ import annotations
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

_POLICIES: dict[str, dict] = {
    "default": {
        "name": "default",
        "rules": [
            {"type": "email", "strategy": "mask"},
            {"type": "phone", "strategy": "mask"},
        ],
    }
}

class Policy(BaseModel):
    name: str
    rules: list[dict]

@router.get("")
async def list_policies():
    return list(_POLICIES.values())

@router.post("")
async def upsert_policy(policy: Policy):
    _POLICIES[policy.name] = policy.model_dump()
    return _POLICIES[policy.name]

@router.get("/{name}")
async def get_policy(name: str):
    if name not in _POLICIES:
        raise HTTPException(status_code=404, detail="Not found")
    return _POLICIES[name]