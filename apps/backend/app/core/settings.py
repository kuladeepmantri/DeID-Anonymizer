from __future__ import annotations
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_env: str = "development"
    secret_key: str = "changeme"
    jwt_algorithm: str = "HS256"

    api_base_url: str = "http://localhost:8000"
    frontend_base_url: str = "http://localhost:5173"
    allowed_origins: str = "http://localhost:5173"

    database_url: str
    redis_url: str
    celery_broker_url: str
    celery_result_backend: str

    storage_backend: str = "s3"
    s3_endpoint_url: str = "http://minio:9000"
    s3_region: str = "us-east-1"
    s3_bucket: str = "deid-bucket"
    aws_access_key_id: str = "minioadmin"
    aws_secret_access_key: str = "minioadmin"

    enable_graphql: bool = True
    enable_audit_ledger: bool = True
    enable_ocr: bool = True
    enable_ai_detection: bool = True
    enable_metal_acceleration: bool = False

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False)
