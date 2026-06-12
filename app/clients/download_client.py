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
        wait=wait_exponential(multiplier=2, min=2, max=10),
        reraise=True,
    )
    def download_to_path(self, url: str, destination: Path) -> int:
        destination.parent.mkdir(parents=True, exist_ok=True)
        total_bytes = 0

        try:
            self.logger.info("Direct download initiated", url=url, destination=str(destination))

            with requests.get(url, stream=True, timeout=self.timeout) as response:
                response.raise_for_status()
                expected_bytes = self._content_length(response.headers)

                with open(destination, 'wb') as file_handle:
                    for chunk in response.iter_content(chunk_size=self.chunk_size):
                        if not chunk:
                            continue
                        file_handle.write(chunk)
                        total_bytes += len(chunk)
                if expected_bytes is not None and total_bytes != expected_bytes:
                    raise ValueError(
                        f"Incomplete download for {url}: expected {expected_bytes} bytes, "
                        f"received {total_bytes}"
                    )
                self._write_metadata(destination, response.headers)
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

    def is_complete_download(self, url: str, destination: Path, require_last_modified_metadata: bool = False) -> bool:
        if not destination.exists() or destination.stat().st_size == 0:
            return False
        metadata = self.remote_metadata(url)
        expected_bytes = metadata.get("content_length")
        if expected_bytes is not None and destination.stat().st_size != expected_bytes:
            return False

        expected_last_modified = metadata.get("last_modified")
        if expected_last_modified:
            local_metadata = self.local_metadata(destination)
            local_last_modified = local_metadata.get("last_modified")
            if require_last_modified_metadata and not local_last_modified:
                return False
            if local_last_modified and local_last_modified != expected_last_modified:
                return False

        return True

    def remote_size(self, url: str) -> int | None:
        return self.remote_metadata(url).get("content_length")

    def remote_metadata(self, url: str) -> dict:
        response = requests.head(url, timeout=self.timeout, allow_redirects=True)
        response.raise_for_status()
        return {
            "content_length": self._content_length(response.headers),
            "last_modified": response.headers.get("Last-Modified"),
            "content_type": response.headers.get("Content-Type"),
        }

    def local_metadata(self, destination: Path) -> dict:
        metadata_path = self._metadata_path(destination)
        if not metadata_path.exists():
            return {}
        try:
            import json

            with open(metadata_path, "r", encoding="utf-8") as file_handle:
                return json.load(file_handle)
        except Exception:
            return {}

    def _write_metadata(self, destination: Path, headers) -> None:
        import json

        metadata = {
            "content_length": self._content_length(headers),
            "last_modified": headers.get("Last-Modified"),
            "content_type": headers.get("Content-Type"),
        }
        with open(self._metadata_path(destination), "w", encoding="utf-8") as file_handle:
            json.dump(metadata, file_handle, indent=2)

    @staticmethod
    def _metadata_path(destination: Path) -> Path:
        return destination.with_name(f"{destination.name}.metadata.json")

    @staticmethod
    def _content_length(headers) -> int | None:
        value = headers.get("Content-Length")
        return int(value) if value else None
