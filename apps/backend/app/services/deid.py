from __future__ import annotations
import re
from app.core.settings import Settings

EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_PATTERN = re.compile(r"(?:(?:\+\d{1,3}[ -]?)?(?:\(\d{1,4}\)[ -]?)?\d{3,}[ -]?\d{2,}[ -]?\d{2,})")

class DeidService:
    def __init__(self) -> None:
        self.settings = Settings()

    async def deidentify_text(self, text: str, policy_name: str | None = None) -> str:
        masked = EMAIL_PATTERN.sub("***@***.***", text)
        masked = PHONE_PATTERN.sub("***-***-****", masked)
        return masked