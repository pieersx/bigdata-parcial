import json
import tempfile
import unittest
from pathlib import Path

try:
    from pyspark.sql import SparkSession

    from app.services.gold_transform_service import GoldTransformService
    from app.storage.data_lake import DataLake
    from app.storage.gold_storage import GoldStorage

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class FakeQualityAnalyzer:
    def analyze(self, *args, **kwargs):
        return []


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required for Gold integration tests")
class GoldTransformServiceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("gold-transform-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.silver_path = root / "silver"
        self.audit_path = root / "audit"
        self.data_lake = DataLake(
            raw_path=root / "raw",
            bronze_path=root / "bronze",
            silver_path=self.silver_path,
            gold_path=root / "gold",
            reports_path=root / "reports",
        )
        self.service = GoldTransformService(
            spark=self.spark,
            storage=GoldStorage(self.data_lake),
            quality_analyzer=FakeQualityAnalyzer(),
            execution_id="gold-test",
            silver_path=str(self.silver_path),
            audit_path=str(self.audit_path),
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def write_silver(self, table_name, rows):
        self.spark.createDataFrame(rows).write.mode("overwrite").parquet(
            str(self.silver_path / table_name)
        )

    def test_master_keeps_siaf_only_municipality(self):
        self.write_silver(
            "fact_ingresos_municipales",
            [
                self.income_row("300001", "010101", "MUNI SISMEPRE", "10.00", "20.00", "5.00"),
                self.income_row("300002", "010102", "MUNI SOLO SIAF", "10.00", "20.00", "5.00"),
            ],
        )
        self.write_silver(
            "dim_municipalidad",
            [{
                "SEC_EJEC": "300001", "UBIGEO": "010101", "MUNICIPALIDAD_NOMBRE": "MUNI SISMEPRE",
                "DEPARTAMENTO_NOMBRE": "AMAZONAS", "PROVINCIA_NOMBRE": "CHACHAPOYAS",
                "DISTRITO_NOMBRE": "CHACHAPOYAS", "idmunici": "1", "Tipomuni": "PROVINCIAL",
                "RENAMU_DEPARTAMENTO": "AMAZONAS", "RENAMU_PROVINCIA": "CHACHAPOYAS",
                "RENAMU_DISTRITO": "CHACHAPOYAS", "renamu_match": True,
            }],
        )

        result = self.service.build_dim_municipalidad_gold()
        published = self.spark.read.parquet(result["storage"]["path"])

        self.assertEqual(2, result["records_published"])
        siaf_only = published.filter("SEC_EJEC = '300002'").first()
        self.assertFalse(siaf_only["has_sismepre"])
        self.assertEqual("010102", siaf_only["UBIGEO"])
        self.assertEqual("MUNI SOLO SIAF", siaf_only["MUNICIPALIDAD_NOMBRE"])
        self.assertTrue(siaf_only["in_scope_presentacion"])
        self.assertTrue((self.silver_path.parent / "reference" / "municipalidades_presentadas.csv").exists())

    def test_scope_file_filters_gold_facts_and_logs_metrics(self):
        reference_path = self.silver_path.parent / "reference"
        reference_path.mkdir(parents=True)
        (reference_path / "municipalidades_presentadas.csv").write_text(
            "SEC_EJEC,UBIGEO,MUNICIPALIDAD_NOMBRE\n"
            "300001,,MUNI SISMEPRE\n"
            "300001,,DUPLICATE\n"
            "999999,,NO EXISTE\n",
            encoding="utf-8",
        )
        self.write_silver(
            "fact_ingresos_municipales",
            [
                self.income_row("300001", "010101", "MUNI SISMEPRE", "10.00", "20.00", "5.00"),
                self.income_row("300002", "010102", "MUNI SOLO SIAF", "10.00", "20.00", "5.00"),
            ],
        )
        self.write_silver(
            "dim_municipalidad",
            [{
                "SEC_EJEC": "300001", "UBIGEO": "010101", "MUNICIPALIDAD_NOMBRE": "MUNI SISMEPRE",
                "DEPARTAMENTO_NOMBRE": "AMAZONAS", "PROVINCIA_NOMBRE": "CHACHAPOYAS",
                "DISTRITO_NOMBRE": "CHACHAPOYAS", "idmunici": "1", "Tipomuni": "PROVINCIAL",
                "RENAMU_DEPARTAMENTO": "AMAZONAS", "RENAMU_PROVINCIA": "CHACHAPOYAS",
                "RENAMU_DISTRITO": "CHACHAPOYAS", "renamu_match": True,
            }],
        )

        dim_result = self.service.build_dim_municipalidad_gold()
        dim = self.spark.read.parquet(dim_result["storage"]["path"])
        monthly_result = self.service.build_fact_ingresos_mensuales()
        monthly = self.spark.read.parquet(monthly_result["storage"]["path"])

        self.assertEqual(2, dim_result["records_published"])
        self.assertEqual(1, dim.filter("in_scope_presentacion").count())
        self.assertEqual(1, monthly_result["records_published"])
        self.assertEqual("300001", monthly.first()["SEC_EJEC"])

        metrics_files = list((self.audit_path / "metrics").rglob("scope_municipalidades_gold-test.json"))
        self.assertEqual(1, len(metrics_files))
        metrics = json.loads(metrics_files[0].read_text(encoding="utf-8"))
        self.assertEqual("aplicado", metrics["scope_status"])
        self.assertEqual(1, metrics["selected_count"])
        self.assertEqual(1, metrics["excluded_count"])
        self.assertEqual(1, metrics["duplicate_count"])
        self.assertEqual(1, metrics["not_found_count"])

    def test_category_enrichment_is_conservative(self):
        self.write_silver(
            "fact_ingresos_municipales",
            [
                self.income_row("300001", "010101", "MUNICIPALIDAD DISTRITAL DE CHETO", "10.00", "20.00", "5.00"),
                self.income_row("300002", "010102", "MUNICIPALIDAD DISTRITAL DE ACO", "10.00", "20.00", "5.00"),
                self.income_row("300003", "020102", "MUNICIPALIDAD DISTRITAL DE ACO", "10.00", "20.00", "5.00"),
                self.income_row("300004", "010103", "MUNICIPALIDAD DISTRITAL DE SIN CATEGORIA", "10.00", "20.00", "5.00"),
            ],
        )
        self.write_silver(
            "dim_municipalidad",
            [{
                "SEC_EJEC": "300001", "UBIGEO": "010101", "MUNICIPALIDAD_NOMBRE": "MUNICIPALIDAD DISTRITAL DE CHETO",
                "DEPARTAMENTO_NOMBRE": "AMAZONAS", "PROVINCIA_NOMBRE": "CHACHAPOYAS",
                "DISTRITO_NOMBRE": "CHETO", "idmunici": "1", "Tipomuni": "DISTRITAL",
                "RENAMU_DEPARTAMENTO": "AMAZONAS", "RENAMU_PROVINCIA": "CHACHAPOYAS",
                "RENAMU_DISTRITO": "CHETO", "renamu_match": True,
            }],
        )
        self.write_silver(
            "dim_categoria_municipalidad",
            [
                {"municipalidad_categoria_raw": "M. D. DE CHETO", "categoria_municipalidad": "F", "municipalidad_categoria_norm": "M D DE CHETO"},
                {"municipalidad_categoria_raw": "M. D. DE ACO", "categoria_municipalidad": "B", "municipalidad_categoria_norm": "M D DE ACO"},
            ],
        )

        result = self.service.build_dim_municipalidad_gold()
        published = self.spark.read.parquet(result["storage"]["path"])

        self.assertEqual(4, result["records_published"])
        self.assertEqual("F", published.filter("SEC_EJEC = '300001'").first()["categoria_municipalidad"])
        self.assertEqual("matched", published.filter("SEC_EJEC = '300001'").first()["categoria_match_status"])
        self.assertIsNone(published.filter("SEC_EJEC = '300002'").first()["categoria_municipalidad"])
        self.assertEqual("ambiguous", published.filter("SEC_EJEC = '300002'").first()["categoria_match_status"])
        self.assertEqual("ambiguous", published.filter("SEC_EJEC = '300003'").first()["categoria_match_status"])
        self.assertEqual("unmatched", published.filter("SEC_EJEC = '300004'").first()["categoria_match_status"])

    def test_missing_category_source_does_not_break_gold_master(self):
        self.write_silver(
            "fact_ingresos_municipales",
            [self.income_row("300001", "010101", "MUNICIPALIDAD DISTRITAL DE CHETO", "10.00", "20.00", "5.00")],
        )
        self.write_silver(
            "dim_municipalidad",
            [{
                "SEC_EJEC": "999999", "UBIGEO": "999999", "MUNICIPALIDAD_NOMBRE": "NO MATCH",
                "DEPARTAMENTO_NOMBRE": "SIN", "PROVINCIA_NOMBRE": "SIN",
                "DISTRITO_NOMBRE": "SIN", "idmunici": "9", "Tipomuni": "DISTRITAL",
                "RENAMU_DEPARTAMENTO": "SIN", "RENAMU_PROVINCIA": "SIN",
                "RENAMU_DISTRITO": "SIN", "renamu_match": False,
            }],
        )

        result = self.service.build_dim_municipalidad_gold()
        published = self.spark.read.parquet(result["storage"]["path"]).first()

        self.assertEqual(1, result["records_published"])
        self.assertIsNone(published["categoria_municipalidad"])
        self.assertEqual("missing_category_source", published["categoria_match_status"])

    def test_monthly_income_reconciles_negative_adjustments_and_null_execution(self):
        self.write_silver(
            "fact_ingresos_municipales",
            [
                self.income_row("300001", "010101", "MUNI", "10.00", "20.00", "5.00"),
                self.income_row("300001", "010101", "MUNI", "0.00", "0.00", "-2.00", rubro="09"),
                self.income_row("300001", "010101", "MUNI", "0.00", "0.00", "3.00", month=2),
            ],
        )

        result = self.service.build_fact_ingresos_mensuales()
        published = self.spark.read.parquet(result["storage"]["path"])
        january = published.filter("MES_DOC = 1").first()
        february = published.filter("MES_DOC = 2").first()

        self.assertEqual(2, result["records_published"])
        self.assertEqual("3.00", str(january["MONTO_RECAUDADO"]))
        self.assertEqual("15.0000", str(january["pct_ejecucion"]))
        self.assertIsNone(february["pct_ejecucion"])

    def test_missing_required_silver_table_is_controlled_error(self):
        with self.assertRaisesRegex(ValueError, "Required Silver table does not exist"):
            self.service.build_fact_ingresos_mensuales()

    def test_quality_fact_reads_pretty_printed_audit_json(self):
        quality_path = self.audit_path / "quality_checks" / "2026" / "06" / "01"
        quality_path.mkdir(parents=True)
        payload = {
            "check_id": "silver-check-1",
            "check_name": "silver_completitud_fact",
            "check_type": "completitud",
            "status": "passed",
            "timestamp": "2026-06-01T08:00:00",
            "dataset": "fact",
            "records_checked": 1,
            "records_passed": 1,
            "records_failed": 0,
            "failure_rate": 0.0,
        }
        (quality_path / "silver-check-1.json").write_text(
            json.dumps(payload, indent=2), encoding="utf-8"
        )

        result = self.service.build_fact_calidad_datos()
        published = self.spark.read.parquet(result["storage"]["path"]).first()

        self.assertEqual(1, result["records_published"])
        self.assertEqual("silver-check-1", published["check_id"])
        self.assertEqual("silver", published["layer"])

    @staticmethod
    def income_row(sec_ejec, ejecutora, name, pia, pim, recaudado, month=1, rubro="08"):
        return {
            "ANO_DOC": 2025, "MES_DOC": month, "SEC_EJEC": sec_ejec, "EJECUTORA": ejecutora,
            "EJECUTORA_NOMBRE": name, "DEPARTAMENTO_EJECUTORA": "01",
            "DEPARTAMENTO_EJECUTORA_NOMBRE": "AMAZONAS", "PROVINCIA_EJECUTORA": "01",
            "PROVINCIA_EJECUTORA_NOMBRE": "CHACHAPOYAS", "DISTRITO_EJECUTORA": "01",
            "DISTRITO_EJECUTORA_NOMBRE": "CHACHAPOYAS", "FUENTE_FINANCIAMIENTO": "1",
            "RUBRO": rubro, "TIPO_RECURSO": "0", "GENERICA": "1", "SUBGENERICA": "1",
            "SUBGENERICA_DET": "1", "ESPECIFICA": "1", "ESPECIFICA_DET": "1",
            "MONTO_PIA": pia, "MONTO_PIM": pim, "MONTO_RECAUDADO": recaudado,
            "_silver_source_row_count": 1,
        }


if __name__ == "__main__":
    unittest.main()
