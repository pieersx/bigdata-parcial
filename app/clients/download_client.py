from pathlib import Path

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from app.utils.logger import StructuredLogger


class DownloadClient:
    def __init__(self, timeout: int = 30, max_retries: int = 3, chunk_size: int = 1024 * 1024):
        self.timeout = timeout
        self.max_retries = max_retries
        self.chunk_size = chunk_size
        self.logger = StructuredLogger(self.__class__.__name__)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10)
    )
    def download_to_path(self, url: str, destination: Path) -> int:
        destination.parent.mkdir(parents=True, exist_ok=True)
        total_bytes = 0

        try:
            self.logger.info("Direct download initiated", url=url, destination=str(destination))

            with requests.get(url, stream=True, timeout=self.timeout) as response:
                response.raise_for_status()

                with open(destination, 'wb') as file_handle:
                    for chunk in response.iter_content(chunk_size=self.chunk_size):
                        if not chunk:
                            continue
                        file_handle.write(chunk)
                        total_bytes += len(chunk)
        except Exception:
            if destination.exists():
                destination.unlink()
            raise

        self.logger.info(
            "Direct download completed",
            url=url,
            destination=str(destination),
            size_bytes=total_bytes,
        )
        return total_bytes
