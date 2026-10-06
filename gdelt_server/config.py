from __future__ import annotations

import json
import os
from pathlib import Path
from pydantic import BaseModel, Field, model_validator


class Settings(BaseModel):
    data_dir: Path = Path("data")
    host: str = "127.0.0.1"
    port: int = Field(8800, ge=1, le=65535)
    monitor_enabled: bool = False
    poll_seconds: int = Field(900, ge=60, le=86400)
    initial_hours: int = Field(72, ge=1, le=17520)
    batch_files: int = Field(32, ge=1, le=256)
    download_workers: int = Field(16, ge=1, le=64)
    parser_workers: int = Field(4, ge=1, le=16)
    hour_retention_days: int = Field(60, ge=8, le=730)
    day_retention_days: int = Field(730, ge=365, le=3650)
    min_free_gb: float = Field(5, ge=0.1)
    max_database_gb: float = Field(250, ge=0.1)
    max_download_mb: int = Field(128, ge=1, le=1024)
    max_uncompressed_mb: int = Field(1024, ge=1, le=8192)
    request_timeout: int = Field(60, ge=5, le=300)
    api_token: str = ""
    snapshot_days: list[int] = Field(default_factory=lambda: [7, 30, 90, 365])

    @model_validator(mode="after")
    def validate_settings(self):
        if self.hour_retention_days > self.day_retention_days:
            raise ValueError("小时保留期不能大于日保留期")
        if not self.snapshot_days or any(d not in (7, 30, 90, 365) for d in self.snapshot_days):
            raise ValueError("snapshot_days 只支持7、30、90、365")
        if len(set(self.snapshot_days)) != len(self.snapshot_days):
            raise ValueError("snapshot_days 不能重复")
        if self.host not in ("127.0.0.1", "localhost", "::1") and len(self.api_token) < 24:
            raise ValueError("监听局域网/公网地址需要至少24字符的 api_token")
        return self

    @classmethod
    def load(cls, path: str | Path | None = None):
        path = path or os.environ.get("GDELT_CONFIG")
        values = json.loads(Path(path).read_text(encoding="utf-8-sig")) if path else {}
        if os.environ.get("GDELT_API_TOKEN"):
            values["api_token"] = os.environ["GDELT_API_TOKEN"]
        settings = cls(**values)
        if path and not settings.data_dir.is_absolute():
            settings.data_dir = Path(path).resolve().parent / settings.data_dir
        settings.data_dir = settings.data_dir.resolve()
        return settings
