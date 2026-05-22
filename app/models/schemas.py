from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class ResourceMetadata(BaseModel):
    dataset: str
    asset_name: str
    source_type: str
    source_url: str
    ingestion_date: datetime = Field(default_factory=datetime.now)
    status: str
    skipped: bool = False
    file_path: Optional[str] = None
    size_bytes: Optional[int] = None
    records_count: Optional[int] = None
    execution_time_ms: Optional[float] = None
    error_message: Optional[str] = None


class BronzeAssetResult(BaseModel):
    metadata: ResourceMetadata
