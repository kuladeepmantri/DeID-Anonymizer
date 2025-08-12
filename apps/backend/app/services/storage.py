from __future__ import annotations
import aioboto3
from typing import List
from app.core.settings import Settings

class StorageService:
    def __init__(self) -> None:
        self.settings = Settings()

    async def _client(self):
        session = aioboto3.Session()
        return session.client(
            "s3",
            endpoint_url=self.settings.s3_endpoint_url,
            region_name=self.settings.s3_region,
            aws_access_key_id=self.settings.aws_access_key_id,
            aws_secret_access_key=self.settings.aws_secret_access_key,
        )

    async def put_object(self, key: str, content: bytes, content_type: str | None = None) -> str:
        async with await self._client() as s3:
            await s3.put_object(Bucket=self.settings.s3_bucket, Key=key, Body=content, ContentType=content_type)
        return key

    async def list_objects(self) -> List[str]:
        async with await self._client() as s3:
            resp = await s3.list_objects_v2(Bucket=self.settings.s3_bucket)
            contents = resp.get("Contents", [])
            return [c["Key"] for c in contents]
