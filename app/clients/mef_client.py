from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import parse_qs, urlparse

from app.clients.base_client import BaseAPIClient


class MEFClient(BaseAPIClient):
    def __init__(self, base_url: str, timeout: int = 30, max_retries: int = 3, page_size: int = 10000):
        super().__init__(base_url=base_url, timeout=timeout, max_retries=max_retries)
        self.page_size = page_size

    def fetch_data(
        self,
        resource_url: str,
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        parsed = urlparse(resource_url)
        request_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        query_params = {
            key: values[0]
            for key, values in parse_qs(parsed.query).items()
        }
        query_params['offset'] = str(offset)
        query_params['limit'] = str(limit or self.page_size)

        return self._make_request(request_url, params=query_params)

    def iter_records(self, resource_url: str) -> Iterator[List[Dict[str, Any]]]:
        offset = 0

        while True:
            payload = self.fetch_data(resource_url=resource_url, offset=offset, limit=self.page_size)
            records = payload.get('records') or []

            if not records:
                if offset == 0:
                    raise ValueError(f"No records returned for resource URL: {resource_url}")
                break

            yield records

            next_url = payload.get('next')
            if not next_url:
                break

            next_offset = parse_qs(urlparse(next_url).query).get('offset', [None])[0]
            if next_offset is None:
                break

            next_offset = int(next_offset)
            if next_offset == offset:
                break

            offset = next_offset
