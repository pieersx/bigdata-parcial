import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class HiveAssetsTest(unittest.TestCase):
    def test_hive_scripts_exist(self):
        for relative in [
            "scripts/hive_bootstrap.py",
            "scripts/hive_lab_queries.py",
            "scripts/export_powerbi_from_hive.py",
            "scripts/materialize_powerbi_hive_tables.py",
            "sql/hive/01_create_databases.sql",
            "sql/hive/03_dashboard_views.sql",
            "sql/hive/04_lab_queries.sql",
            "sql/hive/05_powerbi_tables.sql",
            "reports/hive_lab_municipal.md",
            "reports/dashboard_spec_hive_municipal.md",
            "notebooks/07_Hive_Lab_Municipal.ipynb",
        ]:
            self.assertTrue((ROOT / relative).exists(), relative)

    def test_six_dashboard_views_are_defined(self):
        sql = (ROOT / "sql/hive/03_dashboard_views.sql").read_text(encoding="utf-8")
        for index in range(1, 7):
            self.assertIn(f"vw_dashboard_0{index}_", sql)

    def test_six_powerbi_tables_are_defined(self):
        sql = (ROOT / "sql/hive/05_powerbi_tables.sql").read_text(encoding="utf-8")
        materializer = (ROOT / "scripts/materialize_powerbi_hive_tables.py").read_text(
            encoding="utf-8"
        )
        for index in range(1, 7):
            self.assertIn(f"pbi_dashboard_0{index}", sql)
            self.assertIn(f"pbi_dashboard_0{index}", materializer)

    def test_hive_lab_covers_required_topics(self):
        sql = (ROOT / "sql/hive/04_lab_queries.sql").read_text(encoding="utf-8").lower()
        for topic in [
            "count(*)",
            "group by",
            "order by",
            "coalesce",
            "rank() over",
            "with ingresos as",
        ]:
            self.assertIn(topic, sql)

    def test_dashboard_spec_uses_categories_and_no_external_maps(self):
        spec = (ROOT / "reports/dashboard_spec_hive_municipal.md").read_text(encoding="utf-8")
        self.assertIn("categoria_municipalidad", spec)
        self.assertIn("Sin mapas externos", spec)


if __name__ == "__main__":
    unittest.main()
