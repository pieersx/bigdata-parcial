from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from app.utils.logger import StructuredLogger


class BaseAPIClient(ABC):
    def __init__(self, base_url: str, timeout: int = 30, max_retries: int = 3):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.logger = StructuredLogger(self.__class__.__name__)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10)
    )
    def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        if endpoint.startswith(("http://", "https://")):
            url = endpoint
        else:
            url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            self.logger.info("API request initiated", url=url, method=method)
            response = requests.request(
                method=method,
                url=url,
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
            self.logger.info("API request successful", status_code=response.status_code)
            return response.json()
        except requests.exceptions.RequestException as error:
            self.logger.error("API request failed", error=str(error), url=url)
            raise

    @abstractmethod
    def fetch_data(self, **kwargs) -> Dict[str, Any]:
        pass
