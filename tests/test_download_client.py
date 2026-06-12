import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from app.clients.download_client import DownloadClient


class DownloadClientTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.destination = Path(self.temporary_directory.name) / "download.csv"
        self.client = DownloadClient()

    def tearDown(self):
        self.temporary_directory.cleanup()

    @patch("app.clients.download_client.requests.head")
    def test_existing_download_is_complete_only_when_remote_size_matches(self, head):
        self.destination.write_bytes(b"complete")
        head.return_value = self._response({"Content-Length": "8"})

        self.assertTrue(self.client.is_complete_download("https://example.test/data.csv", self.destination))

        head.return_value = self._response({"Content-Length": "9"})
        self.assertFalse(self.client.is_complete_download("https://example.test/data.csv", self.destination))

    @patch("app.clients.download_client.requests.head")
    def test_existing_download_is_stale_when_last_modified_changes(self, head):
        self.destination.write_bytes(b"complete")
        self.destination.with_name("download.csv.metadata.json").write_text(
            '{"content_length": 8, "last_modified": "Mon, 01 Jun 2026 10:00:00 GMT"}',
            encoding="utf-8",
        )

        head.return_value = self._response({
            "Content-Length": "8",
            "Last-Modified": "Tue, 02 Jun 2026 10:00:00 GMT",
        })

        self.assertFalse(self.client.is_complete_download("https://example.test/data.csv", self.destination))

    @patch("app.clients.download_client.requests.head")
    def test_live_download_requires_local_last_modified_metadata(self, head):
        self.destination.write_bytes(b"complete")
        head.return_value = self._response({
            "Content-Length": "8",
            "Last-Modified": "Tue, 02 Jun 2026 10:00:00 GMT",
        })

        self.assertFalse(
            self.client.is_complete_download(
                "https://example.test/data.csv",
                self.destination,
                require_last_modified_metadata=True,
            )
        )

    @patch("app.clients.download_client.requests.get")
    def test_partial_response_is_deleted(self, get):
        response = self._response({"Content-Length": "10"})
        response.iter_content.return_value = [b"partial"]
        get.return_value.__enter__.return_value = response

        with self.assertRaisesRegex(ValueError, "Incomplete download"):
            self.client.download_to_path("https://example.test/data.csv", self.destination)

        self.assertFalse(self.destination.exists())

    @patch("app.clients.download_client.requests.get")
    def test_successful_download_writes_remote_metadata(self, get):
        response = self._response({
            "Content-Length": "8",
            "Last-Modified": "Tue, 02 Jun 2026 10:00:00 GMT",
            "Content-Type": "text/csv",
        })
        response.iter_content.return_value = [b"complete"]
        get.return_value.__enter__.return_value = response

        self.client.download_to_path("https://example.test/data.csv", self.destination)

        metadata = self.client.local_metadata(self.destination)
        self.assertEqual(8, metadata["content_length"])
        self.assertEqual("Tue, 02 Jun 2026 10:00:00 GMT", metadata["last_modified"])

    @staticmethod
    def _response(headers):
        response = Mock()
        response.headers = headers
        response.raise_for_status.return_value = None
        return response


if __name__ == "__main__":
    unittest.main()
