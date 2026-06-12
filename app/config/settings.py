import os
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv


load_dotenv()


class Settings:
    def __init__(self):
        self.project_root = Path(__file__).resolve().parents[2]
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        config_path = Path(os.getenv("CONFIG_PATH", self.project_root / "config.yaml"))
        with open(config_path, 'r', encoding='utf-8') as file_handle:
            return yaml.safe_load(file_handle)

    def _resolve_project_path(self, configured_path: str, env_var: str) -> Path:
        override = os.getenv(env_var)
        if override:
            return Path(override)

        path = Path(configured_path)
        if path.is_absolute():
            return path

        return self.project_root / path

    @property
    def datasets(self) -> Dict[str, Any]:
        return self.config['datasets']

    @property
    def spark(self) -> Dict[str, Any]:
        return self.config.get('spark', {})

    @property
    def profiling(self) -> Dict[str, Any]:
        return self.config.get('profiling', {})

    @property
    def quality(self) -> Dict[str, Any]:
        return self.config.get('quality', {})

    @property
    def silver(self) -> Dict[str, Any]:
        return self.config.get('silver', {})

    @property
    def gold(self) -> Dict[str, Any]:
        return self.config.get('gold', {})

    @property
    def timeout(self) -> int:
        return int(os.getenv("HTTP_TIMEOUT", "30"))

    @property
    def max_retries(self) -> int:
        return int(os.getenv("HTTP_MAX_RETRIES", "3"))

    @property
    def mef_base_url(self) -> str:
        return os.getenv("MEF_BASE_URL", "https://api.datosabiertos.mef.gob.pe")

    @property
    def api_page_size(self) -> int:
        return int(os.getenv("MEF_PAGE_SIZE", "10000"))

    @property
    def bronze_path(self) -> Path:
        return self._resolve_project_path(self.config['paths']['bronze'], "BRONZE_PATH")

    @property
    def raw_path(self) -> Path:
        return self._resolve_project_path(self.config['paths']['raw'], "RAW_PATH")

    @property
    def silver_path(self) -> Path:
        return self._resolve_project_path(self.config['paths']['silver'], "SILVER_PATH")

    @property
    def gold_path(self) -> Path:
        return self._resolve_project_path(self.config['paths']['gold'], "GOLD_PATH")

    @property
    def reports_path(self) -> Path:
        return self._resolve_project_path(self.config['paths']['reports'], "REPORTS_PATH")

    @property
    def audit_path(self) -> Path:
        return self.bronze_path.parent / "audit"


settings = Settings()
