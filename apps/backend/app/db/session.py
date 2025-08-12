from __future__ import annotations
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from app.core.settings import Settings

settings = Settings()

engine = create_async_engine(settings.database_url, echo=False, pool_pre_ping=True, future=True)

AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False, autoflush=False, autocommit=False)

async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session