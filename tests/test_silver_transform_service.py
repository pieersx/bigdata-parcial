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

        result = self.service.build_ingresos_municipales_curated()

        self.assertEqual("blocked", result["status"])
        self.assertFalse((self.data_lake.silver_path / "ingresos_municipales_curated").exists())

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

        result = self.service.build_ingresos_municipales_curated()
        published = self.spark.read.parquet(result["storage"]["path"])

        self.assertEqual(1, result["records_published"])
        self.assertEqual("-923.50", str(published.first()["MONTO_RECAUDADO"]))
        self.assertEqual(2, published.first()["_silver_source_row_count"])

    def test_municipalidades_curated_keeps_unmatched_renamu_rows(self):
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

        result = self.service.build_municipalidades_curated()
        published = self.spark.read.parquet(result["storage"]["path"])

        self.assertEqual(2, result["records_published"])
        self.assertEqual(1, published.filter("renamu_match = false").count())
        self.assertEqual("MUNI A", published.filter("SEC_EJEC = '000001'").first()["MUNICIPALIDAD_NOMBRE"])

    def test_renamu_curated_publishes_clean_parquet_dataset(self):
        self.write_bronze(
            "renamu",
            [
                {
                    "Ubigeo": "10101", "Año": "2022", "idmunici": "1", "Tipomuni": " Provincial ",
                    "Departamento": " amazonas ", "Provincia": " chachapoyas ", "Distrito": " chachapoyas ",
                    "P19M_T": "12", "P22_AT2": "1", "P22_AT3": "0", "P22_C2": "1", "P22_C3": "",
                    "P16_5": "1", "P17_2": "0", "P17_3": "1",
                },
                {
                    "Ubigeo": "", "Año": "2022", "idmunici": "2", "Tipomuni": "Distrital",
                    "Departamento": "x", "Provincia": "x", "Distrito": "x",
                    "P19M_T": "x", "P22_AT2": "0", "P22_AT3": "0", "P22_C2": "0", "P22_C3": "0",
                    "P16_5": "0", "P17_2": "0", "P17_3": "0",
                },
            ],
        )

        result = self.service.build_renamu_curated()
        published = self.spark.read.parquet(result["storage"]["path"])
        quarantined = self.spark.read.parquet(result["quarantine"]["path"])
        row = published.first()

        self.assertEqual(1, result["records_published"])
        self.assertEqual(1, result["records_quarantined"])
        self.assertEqual("010101", row["UBIGEO"])
        self.assertEqual(2022, row["ANO_RENAMU"])
        self.assertEqual("AMAZONAS", row["DEPARTAMENTO_NOMBRE"])
        self.assertTrue(row["usa_srtm_estado"])
        self.assertTrue(row["usa_software_catastro"])
        self.assertTrue(row["usa_al_menos_un_software_at"])
        self.assertEqual("missing_renamu_key_or_year", quarantined.first()["_quarantine_reason"])

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

        result = self.service.build_predial_esat_curated()
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

        result = self.service.build_sismepre_respuestas_curated()
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
        clean_result = self.service.build_sismepre_respuestas_curated()

        self.assertEqual(0, clean_result["records_quarantined"])
        self.assertFalse((self.data_lake.silver_path / "_quarantine" / "sismepre_respuestas_curated").exists())

    def test_category_dataset_resolves_multiple_categories_with_business_rules(self):
        self.spark.createDataFrame(
            [
                {"SEC_EJEC": "300001", "UBIGEO": "010101", "MUNICIPALIDAD_NOMBRE": "MUNICIPALIDAD DISTRITAL DE CHETO", "DEPARTAMENTO_NOMBRE": "AMAZONAS", "PROVINCIA_NOMBRE": "CHACHAPOYAS", "DISTRITO_NOMBRE": "CHETO"},
                {"SEC_EJEC": "300002", "UBIGEO": "150101", "MUNICIPALIDAD_NOMBRE": "MUNICIPALIDAD DISTRITAL DE CONFLICTO", "DEPARTAMENTO_NOMBRE": "LIMA", "PROVINCIA_NOMBRE": "LIMA", "DISTRITO_NOMBRE": "CONFLICTO"},
                {"SEC_EJEC": "300003", "UBIGEO": "010102", "MUNICIPALIDAD_NOMBRE": "MUNICIPALIDAD DISTRITAL DE CONFLICTO", "DEPARTAMENTO_NOMBRE": "AMAZONAS", "PROVINCIA_NOMBRE": "CHACHAPOYAS", "DISTRITO_NOMBRE": "CONFLICTO"},
                {"SEC_EJEC": "300004", "UBIGEO": "010103", "MUNICIPALIDAD_NOMBRE": "MUNICIPALIDAD DISTRITAL DE SIN CATEGORIA", "DEPARTAMENTO_NOMBRE": "AMAZONAS", "PROVINCIA_NOMBRE": "CHACHAPOYAS", "DISTRITO_NOMBRE": "SIN CATEGORIA"},
            ]
        ).write.mode("overwrite").parquet(str(self.data_lake.silver_path / "municipalidades_curated"))
        self.write_bronze(
            "categorias_municipalidades",
            [
                {"Municipalidad": "M. D. DE CHETO", "Categoria": "F"},
                {"Municipalidad": "Municipalidad Distrital de Cheto", "Categoria": "F"},
                {"Municipalidad": "M. D. DE CONFLICTO", "Categoria": "B"},
                {"Municipalidad": "M. D. DE CONFLICTO", "Categoria": "C"},
                {"Municipalidad": "", "Categoria": "A"},
                {"Municipalidad": "M. D. DE INVALIDA", "Categoria": "Z"},
            ],
        )

        result = self.service.build_categorias_municipalidades_curated()
        published = self.spark.read.parquet(result["storage"]["path"])
        quarantined = self.spark.read.parquet(result["quarantine"]["path"])

        self.assertEqual(4, result["records_published"])
        self.assertEqual(2, result["records_quarantined"])
        self.assertEqual("F", published.filter("SEC_EJEC = '300001'").first()["categoria_municipalidad"])
        self.assertEqual("C", published.filter("SEC_EJEC = '300002'").first()["categoria_municipalidad"])
        self.assertEqual("multiple_master_lima_to_c", published.filter("SEC_EJEC = '300002'").first()["categoria_rule_applied"])
        self.assertEqual("G", published.filter("SEC_EJEC = '300003'").first()["categoria_municipalidad"])
        self.assertEqual("multiple_master_non_lima_to_g", published.filter("SEC_EJEC = '300003'").first()["categoria_rule_applied"])
        self.assertTrue(published.filter("SEC_EJEC = '300004'").first()["exclude_from_gold_scope"])
        reasons = {row["_quarantine_reason"] for row in quarantined.select("_quarantine_reason").collect()}
        self.assertEqual({"invalid_category_or_name"}, reasons)


if __name__ == "__main__":
    unittest.main()

