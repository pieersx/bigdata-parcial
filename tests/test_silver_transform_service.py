import tempfile
import unittest
from pathlib import Path

try:
    from pyspark.sql import SparkSession

    from app.services.silver_transform_service import SilverTransformService
    from app.storage.data_lake import DataLake
    from app.storage.silver_storage import SilverStorage

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class FakeQualityAnalyzer:
    def analyze(self, *args, **kwargs):
        return []


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required for Silver integration tests")
class SilverTransformServiceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("silver-transform-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.bronze_path = root / "bronze"
        self.data_lake = DataLake(
            raw_path=root / "raw",
            bronze_path=self.bronze_path,
            silver_path=root / "silver",
            reports_path=root / "reports",
        )
        self.service = SilverTransformService(
            spark=self.spark,
            storage=SilverStorage(self.data_lake),
            quality_analyzer=FakeQualityAnalyzer(),
            execution_id="silver-test",
            bronze_path=str(self.bronze_path),
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def write_bronze(self, relative_path, rows):
        self.spark.createDataFrame(rows).write.mode("overwrite").parquet(
            str(self.bronze_path / relative_path)
        )

    def test_regional_income_is_blocked_and_not_published_as_municipal(self):
        self.write_bronze("ingresos", [{"NIVEL_GOBIERNO": "R"}])

        result = self.service.build_fact_ingresos_municipales()

        self.assertEqual("blocked", result["status"])
        self.assertFalse((self.data_lake.silver_path / "fact_ingresos_municipales").exists())

    def test_municipal_income_movements_are_aggregated_by_budget_key(self):
        common = {
            "ANO_DOC": "2025", "MES_DOC": "7", "NIVEL_GOBIERNO": "M",
            "SEC_EJEC": "300939", "EJECUTORA": "100803", "FUENTE_FINANCIAMIENTO": "4",
            "RUBRO": "13", "TIPO_RECURSO": "18", "GENERICA": "9", "SUBGENERICA": "1",
            "SUBGENERICA_DET": "1", "ESPECIFICA": "1", "ESPECIFICA_DET": "1",
            "MONTO_PIA": "0", "MONTO_PIM": "0",
        }
        self.write_bronze(
            "ingresos",
            [{**common, "MONTO_RECAUDADO": "-803.50"}, {**common, "MONTO_RECAUDADO": "-120.00"}],
        )

        result = self.service.build_fact_ingresos_municipales()
        published = self.spark.read.parquet(result["storage"]["path"])

        self.assertEqual(1, result["records_published"])
        self.assertEqual("-923.50", str(published.first()["MONTO_RECAUDADO"]))
        self.assertEqual(2, published.first()["_silver_source_row_count"])

    def test_dim_municipalidad_keeps_unmatched_renamu_rows(self):
        self.write_bronze(
            "sismepre/rentas_esat_estadistica_atm",
            [
                {"SEC_EJEC": "1", "UBIGEO": "010101", "DEPARTAMENTO_NOMBRE": " Amazonas ", "PROVINCIA_NOMBRE": "Chachapoyas", "DISTRITO_NOMBRE": "A", "MUNICIPALIDAD_NOMBRE": " muni a "},
                {"SEC_EJEC": "2", "UBIGEO": "010102", "DEPARTAMENTO_NOMBRE": "Amazonas", "PROVINCIA_NOMBRE": "Chachapoyas", "DISTRITO_NOMBRE": "B", "MUNICIPALIDAD_NOMBRE": "muni b"},
            ],
        )
        self.write_bronze(
            "renamu",
            [{"Ubigeo": "010101", "idmunici": "1", "Tipomuni": "Provincial", "Departamento": "Amazonas", "Provincia": "Chachapoyas", "Distrito": "A"}],
        )

        result = self.service.build_dim_municipalidad()
        published = self.spark.read.parquet(result["storage"]["path"])

        self.assertEqual(2, result["records_published"])
        self.assertEqual(1, published.filter("renamu_match = false").count())
        self.assertEqual("MUNI A", published.filter("SEC_EJEC = '000001'").first()["MUNICIPALIDAD_NOMBRE"])

    def test_predial_invalid_metric_goes_to_quarantine_with_raw_value(self):
        common = {
            "SEC_EJEC": "1", "UBIGEO": "010101", "ANO_APLICACION": "2024", "PERIODO": "1",
            "ANO_ESTADISTICA": "2024", "MES_ESTADISTICA": "1", "FORMULARIO_ID": "7",
            "DEPARTAMENTO_NOMBRE": "Amazonas", "PROVINCIA_NOMBRE": "Chachapoyas",
            "DISTRITO_NOMBRE": "A", "MUNICIPALIDAD_NOMBRE": "Muni A",
        }
        self.write_bronze(
            "sismepre/rentas_esat_estadistica_atm",
            [{**common, "MON_RECAUDACION": "12.50"}, {**common, "MES_ESTADISTICA": "2", "MON_RECAUDACION": "abc"}],
        )

        result = self.service.build_fact_predial_esat()
        quarantined = self.spark.read.parquet(result["quarantine"]["path"])

        self.assertEqual(1, result["records_published"])
        self.assertEqual(1, result["records_quarantined"])
        self.assertEqual("abc", quarantined.first()["_raw_MON_RECAUDACION"])

    def test_responses_explode_multivalue_and_quarantine_empty_row(self):
        common = {"SEC_EJEC": "1", "ANO_APLICACION": "2024", "PERIODO": "1", "FORMULARIO_ID": "7", "PREGUNTA_ID": "9"}
        self.write_bronze(
            "sismepre/rentas_respuestas",
            [
                {**common, "RESPUESTA_ID": "1", "RESPUESTA_TEXTO": "si", "RESPUESTA_DECIMAL": "0", "RESPUESTA_ENTERO": "2", "RESPUESTA_FECHA": ""},
                {**common, "RESPUESTA_ID": "2", "RESPUESTA_TEXTO": "", "RESPUESTA_DECIMAL": "0", "RESPUESTA_ENTERO": "0", "RESPUESTA_FECHA": ""},
                {**common, "RESPUESTA_ID": "3", "RESPUESTA_TEXTO": "", "RESPUESTA_DECIMAL": "", "RESPUESTA_ENTERO": "", "RESPUESTA_FECHA": "99/99/2024"},
                {**common, "RESPUESTA_ID": "4", "RESPUESTA_TEXTO": "0", "RESPUESTA_DECIMAL": "0", "RESPUESTA_ENTERO": "0", "RESPUESTA_FECHA": ""},
            ],
        )

        result = self.service.build_fact_sismepre_respuestas()
        published = self.spark.read.parquet(result["storage"]["path"])
        quarantined = self.spark.read.parquet(result["quarantine"]["path"])

        self.assertEqual(3, result["records_published"])
        self.assertEqual(2, result["records_quarantined"])
        self.assertEqual(2, published.filter("source_multivalue = true").count())
        reasons = {row["_quarantine_reason"] for row in quarantined.select("_quarantine_reason").collect()}
        self.assertEqual({"no_active_response", "unparseable_typed_response"}, reasons)
        invalid_date = quarantined.filter("_quarantine_reason = 'unparseable_typed_response'").first()
        self.assertEqual("99/99/2024", invalid_date["response_raw_value"])

        self.write_bronze(
            "sismepre/rentas_respuestas",
            [{**common, "RESPUESTA_ID": "5", "RESPUESTA_TEXTO": "si", "RESPUESTA_DECIMAL": "0", "RESPUESTA_ENTERO": "0", "RESPUESTA_FECHA": ""}],
        )
        clean_result = self.service.build_fact_sismepre_respuestas()

        self.assertEqual(0, clean_result["records_quarantined"])
        self.assertFalse((self.data_lake.silver_path / "_quarantine" / "fact_sismepre_respuestas").exists())

    def test_category_dimension_normalizes_and_quarantines_conflicts(self):
        self.write_bronze(
            "categorias_municipalidades",
            [
                {"Municipalidad": "M. D. DE CHETO", "Categoria": "F"},
                {"Municipalidad": "Municipalidad Distrital de Cheto", "Categoria": "F"},
                {"Municipalidad": "M. P. DE CHACHAPOYAS", "Categoria": "A"},
                {"Municipalidad": "M. D. DE CONFLICTO", "Categoria": "B"},
                {"Municipalidad": "M. D. DE CONFLICTO", "Categoria": "C"},
                {"Municipalidad": "", "Categoria": "A"},
                {"Municipalidad": "M. D. DE INVALIDA", "Categoria": "Z"},
            ],
        )

        result = self.service.build_dim_categoria_municipalidad()
        published = self.spark.read.parquet(result["storage"]["path"])
        quarantined = self.spark.read.parquet(result["quarantine"]["path"])

        self.assertEqual(2, result["records_published"])
        self.assertEqual(4, result["records_quarantined"])
        self.assertEqual("M D DE CHETO", published.filter("categoria_municipalidad = 'F'").first()["municipalidad_categoria_norm"])
        reasons = {row["_quarantine_reason"] for row in quarantined.select("_quarantine_reason").collect()}
        self.assertEqual({"invalid_category_or_name", "conflicting_duplicate_category"}, reasons)


if __name__ == "__main__":
    unittest.main()
