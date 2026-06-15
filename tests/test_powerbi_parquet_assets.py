from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


class PowerBIParquetAssetsTest(unittest.TestCase):
    def test_gold_pipeline_publishes_powerbi_parquet_tables(self):
        pipeline = _read("app/pipelines/gold/gold_pipeline.py")
        for index in range(1, 7):
            self.assertIn(f"pbi_dashboard_{index:02d}", pipeline)
            self.assertIn(f"build_pbi_dashboard_{index:02d}", pipeline)

    def test_compose_does_not_publish_hive_stack(self):
        compose = _read("compose.yml").lower()
        forbidden = ["hive-server", "hive-metastore", "namenode", "datanode", "hdfs_namenode"]
        for token in forbidden:
            self.assertNotIn(token, compose)

    def test_powerbi_documentation_points_to_gold_parquet(self):
        doc = _read("reports/powerbi_direct_parquet_connection.md")
        self.assertIn("data\\gold\\pbi_dashboard_01", doc)
        self.assertIn("Folder.Files", doc)
        self.assertNotIn("Odbc.Query", doc)
        self.assertNotIn("MunicipalHive", doc)
