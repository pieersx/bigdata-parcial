import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.quality.gold_quality_analyzer import GoldQualityAnalyzer
from app.services.municipal_scope import MunicipalScopeService
from app.storage.gold_storage import GoldStorage


CLASSIFIER_COLUMNS = [
    "FUENTE_FINANCIAMIENTO", "RUBRO", "TIPO_RECURSO", "GENERICA",
    "SUBGENERICA", "SUBGENERICA_DET", "ESPECIFICA", "ESPECIFICA_DET",
]
CLASSIFIER_NAME_COLUMNS = [f"{column}_NOMBRE" for column in CLASSIFIER_COLUMNS]
INCOME_AMOUNT_COLUMNS = ["MONTO_PIA", "MONTO_PIM", "MONTO_RECAUDADO"]
RENAMU_GESTION_COLUMNS = ["P19M_T", "P22_AT2", "P22_AT3", "P22_C2", "P22_C3"]
RENAMU_SOFTWARE_COLUMNS = ["P16_5", "P17_2", "P17_3"]


class GoldTransformService:
    def __init__(
        self,
        spark: SparkSession,
        storage: GoldStorage,
        quality_analyzer: GoldQualityAnalyzer,
        execution_id: str,
        silver_path: str,
        audit_path: str,
        reference_path: str | None = None,
    ):
        self.spark = spark
        self.storage = storage
        self.quality_analyzer = quality_analyzer
        self.execution_id = execution_id
        self.silver_path = silver_path
        self.audit_path = audit_path
        self.reference_path = reference_path or str(Path(silver_path).parent / "reference")
        self.scope_metrics: Dict[str, Any] | None = None

    def build_dim_municipalidad_gold(self) -> Dict[str, Any]:
        income = self._read("ingresos_municipales_curated")
        sismepre = self._read("municipalidades_curated")
        siaf = (
            income.select(
                "SEC_EJEC",
                F.col("EJECUTORA").alias("SIAF_UBIGEO"),
                F.col("EJECUTORA_NOMBRE").alias("SIAF_MUNICIPALIDAD_NOMBRE"),
                F.col("DEPARTAMENTO_EJECUTORA").alias("SIAF_DEPARTAMENTO"),
                F.col("DEPARTAMENTO_EJECUTORA_NOMBRE").alias("SIAF_DEPARTAMENTO_NOMBRE"),
                F.col("PROVINCIA_EJECUTORA").alias("SIAF_PROVINCIA"),
                F.col("PROVINCIA_EJECUTORA_NOMBRE").alias("SIAF_PROVINCIA_NOMBRE"),
                F.col("DISTRITO_EJECUTORA").alias("SIAF_DISTRITO"),
                F.col("DISTRITO_EJECUTORA_NOMBRE").alias("SIAF_DISTRITO_NOMBRE"),
                *self._lineage_columns(income),
            )
            .dropDuplicates(["SEC_EJEC"])
            .withColumn("has_siaf", F.lit(True))
        )
        sm = sismepre.select(
            "SEC_EJEC", "UBIGEO", "MUNICIPALIDAD_NOMBRE", "DEPARTAMENTO_NOMBRE",
            "PROVINCIA_NOMBRE", "DISTRITO_NOMBRE", "idmunici", "Tipomuni",
            "RENAMU_DEPARTAMENTO", "RENAMU_PROVINCIA", "RENAMU_DISTRITO", "renamu_match",
        ).withColumn("has_sismepre", F.lit(True))
        curated = (
            siaf.join(sm, "SEC_EJEC", "left")
            .withColumn("UBIGEO", F.coalesce("UBIGEO", "SIAF_UBIGEO"))
            .withColumn("MUNICIPALIDAD_NOMBRE", F.coalesce("MUNICIPALIDAD_NOMBRE", "SIAF_MUNICIPALIDAD_NOMBRE"))
            .withColumn("DEPARTAMENTO_NOMBRE", F.coalesce("DEPARTAMENTO_NOMBRE", "SIAF_DEPARTAMENTO_NOMBRE"))
            .withColumn("PROVINCIA_NOMBRE", F.coalesce("PROVINCIA_NOMBRE", "SIAF_PROVINCIA_NOMBRE"))
            .withColumn("DISTRITO_NOMBRE", F.coalesce("DISTRITO_NOMBRE", "SIAF_DISTRITO_NOMBRE"))
            .withColumn("has_sismepre", F.coalesce("has_sismepre", F.lit(False)))
            .withColumn("renamu_match", F.coalesce("renamu_match", F.lit(False)))
            .select(
                "SEC_EJEC", "UBIGEO", "MUNICIPALIDAD_NOMBRE",
                "DEPARTAMENTO_NOMBRE", "PROVINCIA_NOMBRE", "DISTRITO_NOMBRE",
                "idmunici", "Tipomuni", "RENAMU_DEPARTAMENTO", "RENAMU_PROVINCIA",
                "RENAMU_DISTRITO", "has_siaf", "has_sismepre", "renamu_match",
                *self._lineage_columns(siaf),
            )
        )
        curated = self._enrich_with_category(curated)
        scope_result = MunicipalScopeService(self.spark, self.reference_path).apply_to_master(curated)
        curated = scope_result.dataframe
        self.scope_metrics = scope_result.metrics
        self._write_scope_metrics(scope_result.metrics)
        return self._publish(
            "dim_municipalidad_gold",
            curated,
            required_columns=["SEC_EJEC", "UBIGEO", "MUNICIPALIDAD_NOMBRE", "DEPARTAMENTO_NOMBRE", "in_scope_presentacion"],
            unique_keys=["SEC_EJEC"],
            details={
                "siaf_only_count": curated.filter(~F.col("has_sismepre")).count(),
                "sismepre_count": curated.filter("has_sismepre").count(),
                "renamu_match_count": curated.filter("renamu_match").count(),
                "category_matched_count": curated.filter("categoria_match_status = 'matched'").count(),
                "category_resolved_lima_count": curated.filter("categoria_match_status = 'resolved_multiple_lima'").count(),
                "category_resolved_non_lima_count": curated.filter("categoria_match_status = 'resolved_multiple_non_lima'").count(),
                "category_excluded_count": curated.filter("exclude_from_gold_scope").count(),
                **scope_result.metrics,
            },
        )

    def build_dim_tiempo(self) -> Dict[str, Any]:
        income = self._read("ingresos_municipales_curated")
        bounds = income.select(
            F.min(F.make_date("ANO_DOC", "MES_DOC", F.lit(1))).alias("min_date"),
            F.max(F.make_date("ANO_DOC", "MES_DOC", F.lit(1))).alias("max_date"),
        ).first()
        if not bounds or not bounds["min_date"] or not bounds["max_date"]:
            raise ValueError("Silver ingresos_municipales_curated has no valid monthly dates")
        curated = (
            self.spark.sql(
                f"SELECT explode(sequence(to_date('{bounds['min_date']}'), "
                f"to_date('{bounds['max_date']}'), interval 1 month)) AS fecha_mes"
            )
            .withColumn("year", F.year("fecha_mes").cast("string"))
            .withColumn("mes", F.month("fecha_mes"))
            .withColumn("trimestre", F.quarter("fecha_mes"))
            .withColumn("nombre_mes", F.date_format("fecha_mes", "MMMM"))
            .withColumn("periodo_id", F.date_format("fecha_mes", "yyyyMM"))
        )
        return self._publish(
            "dim_tiempo", curated,
            required_columns=["periodo_id", "fecha_mes", "year", "mes", "trimestre", "nombre_mes"],
            unique_keys=["periodo_id"],
        )

    def build_dim_clasificador_ingreso(self) -> Dict[str, Any]:
        income = self._read("ingresos_municipales_curated")
        curated = (
            income.select(*CLASSIFIER_COLUMNS, *CLASSIFIER_NAME_COLUMNS)
            .dropDuplicates(CLASSIFIER_COLUMNS)
            .withColumn("clasificador_id", self._classifier_id())
        )
        return self._publish(
            "dim_clasificador_ingreso", curated,
            required_columns=["clasificador_id", *CLASSIFIER_COLUMNS],
            unique_keys=["clasificador_id"],
        )

    def build_dim_ubigeo(self) -> Dict[str, Any]:
        master = self.spark.read.parquet(str(self.storage.data_lake.gold_path / "dim_municipalidad_gold"))
        curated = (
            master.select(
                F.col("UBIGEO").alias("ubigeo_id"),
                "DEPARTAMENTO_NOMBRE",
                "PROVINCIA_NOMBRE",
                "DISTRITO_NOMBRE",
            )
            .where("ubigeo_id IS NOT NULL")
            .dropDuplicates(["ubigeo_id"])
        )
        return self._publish(
            "dim_ubigeo", curated,
            required_columns=["ubigeo_id", "DEPARTAMENTO_NOMBRE", "PROVINCIA_NOMBRE", "DISTRITO_NOMBRE"],
            unique_keys=["ubigeo_id"],
        )

    def build_dim_estado_sismepre(self) -> Dict[str, Any]:
        source = self._read("sismepre_entidad_estado_curated")
        curated = (
            source.select("ESTADO", "CLASIFICACION", "TIPO_META")
            .dropDuplicates(["ESTADO", "CLASIFICACION", "TIPO_META"])
            .withColumn("estado_sismepre_id", self._estado_sismepre_id())
        )
        return self._publish(
            "dim_estado_sismepre", curated,
            required_columns=["estado_sismepre_id", "ESTADO", "CLASIFICACION"],
            unique_keys=["estado_sismepre_id"],
        )

    def build_dim_formulario_sismepre(self) -> Dict[str, Any]:
        source = self._read("sismepre_formularios_curated")
        curated = source.select(
            "ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "TITULO", "SUB_TITULO",
            "TIPO_FORMULARIO", "CLASIFICACION", "ABREVIATURA", "ESTADO_REGISTRO",
            *self._lineage_columns(source),
        ).withColumn("year", F.col("ANO_APLICACION").cast("string"))
        return self._publish(
            "dim_formulario_sismepre", curated,
            required_columns=["ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "TITULO"],
            unique_keys=["ANO_APLICACION", "PERIODO", "FORMULARIO_ID"],
            partition_columns=["year"],
        )

    def build_dim_pregunta_sismepre(self) -> Dict[str, Any]:
        source = self._read("sismepre_preguntas_curated")
        curated = source.select(
            "ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID",
            "PREGUNTA_PADRE_ID", "ORDEN_PREGUNTA", "DESCRIPCION", "RESPUESTA",
            "RANGO_INI", "RANGO_FIN", "ESTADO_REGISTRO", *self._lineage_columns(source),
        ).withColumn("year", F.col("ANO_APLICACION").cast("string"))
        return self._publish(
            "dim_pregunta_sismepre", curated,
            required_columns=["ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID", "DESCRIPCION"],
            unique_keys=["ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID"],
            partition_columns=["year"],
        )

    def build_fact_ingresos_mensuales(self) -> Dict[str, Any]:
        income = self._filter_scope(self._read("ingresos_municipales_curated"))
        curated = (
            income.repartition(96, "ANO_DOC", "MES_DOC", "SEC_EJEC")
            .groupBy("SEC_EJEC", "ANO_DOC", "MES_DOC")
            .agg(
                *[F.sum(column).cast("decimal(24,2)").alias(column) for column in INCOME_AMOUNT_COLUMNS],
                self._source_row_count(income),
                *self._first_lineage(income),
            )
            .withColumn("year", F.col("ANO_DOC").cast("string"))
            .withColumn("periodo_id", F.format_string("%04d%02d", "ANO_DOC", "MES_DOC"))
            .withColumn("variacion_pim_pia", (F.col("MONTO_PIM") - F.col("MONTO_PIA")).cast("decimal(24,2)"))
            .withColumn(
                "pct_ejecucion",
                F.when(F.col("MONTO_PIM") != 0, (F.col("MONTO_RECAUDADO") / F.col("MONTO_PIM") * 100).cast("decimal(20,4)")),
            )
        )
        return self._publish(
            "fact_ingresos_mensuales", curated,
            required_columns=["SEC_EJEC", "ANO_DOC", "MES_DOC", *INCOME_AMOUNT_COLUMNS],
            unique_keys=["SEC_EJEC", "ANO_DOC", "MES_DOC"],
            partition_columns=["year"],
        )

    def build_fact_ingresos_clasificador(self) -> Dict[str, Any]:
        income = self._filter_scope(self._read("ingresos_municipales_curated"))
        keys = ["SEC_EJEC", "ANO_DOC", "MES_DOC", *CLASSIFIER_COLUMNS]
        curated = (
            income.repartition(96, *keys)
            .groupBy(*keys)
            .agg(
                *[F.sum(column).cast("decimal(24,2)").alias(column) for column in INCOME_AMOUNT_COLUMNS],
                self._source_row_count(income),
                *self._first_lineage(income),
            )
            .withColumn("clasificador_id", self._classifier_id())
            .withColumn("year", F.col("ANO_DOC").cast("string"))
            .withColumn("periodo_id", F.format_string("%04d%02d", "ANO_DOC", "MES_DOC"))
        )
        return self._publish(
            "fact_ingresos_clasificador", curated,
            required_columns=["SEC_EJEC", "ANO_DOC", "MES_DOC", "clasificador_id", *INCOME_AMOUNT_COLUMNS],
            unique_keys=keys,
            partition_columns=["year"],
        )

    def build_fact_predial_mensual(self) -> Dict[str, Any]:
        predial = self._filter_scope(self._read("predial_esat_curated"))
        metrics = [column for column in predial.columns if column.startswith("MON_") or column.startswith("NUM_")]
        keys = ["SEC_EJEC", "ANO_ESTADISTICA", "MES_ESTADISTICA"]
        curated = (
            predial.groupBy(*keys)
            .agg(
                *[F.sum(column).alias(column) for column in metrics],
                F.first("TIPO_META", ignorenulls=True).alias("TIPO_META"),
                F.count("*").alias("_gold_source_row_count"),
                *self._first_lineage(predial),
            )
            .withColumn("year", F.col("ANO_ESTADISTICA").cast("string"))
            .withColumn("periodo_id", F.format_string("%04d%02d", "ANO_ESTADISTICA", "MES_ESTADISTICA"))
            .withColumn("MON_RECAUDACION_TOTAL", F.coalesce("MON_RECAUDACTUAL_ORDIN", F.lit(0)) + F.coalesce("MON_RECAUDACTUAL_COAC", F.lit(0)))
            .withColumn("MON_SALDO_PREDIAL_TOTAL", F.coalesce("MON_SALDOPREDIAL_ORD", F.lit(0)) + F.coalesce("MON_SALDOPREDIAL_COAC", F.lit(0)))
        )
        return self._publish(
            "fact_predial_mensual", curated,
            required_columns=["SEC_EJEC", "ANO_ESTADISTICA", "MES_ESTADISTICA", "periodo_id"],
            unique_keys=keys,
            partition_columns=["year"],
        )

    def build_fact_sismepre_cumplimiento(self) -> Dict[str, Any]:
        source = self._filter_scope(self._read("sismepre_entidad_estado_curated"))
        curated = (
            source.select(
                "SEC_EJEC", "ANO_APLICACION", "PERIODO", "ESTADO", "CLASIFICACION",
                "TIPO_META", "ORIGEN_INFORMACION", "IND_RESOL_ALCAL_ADJUNTO",
                "FECHA_RESOL_ALCAL_ADJUNTO", *self._lineage_columns(source),
            )
            .withColumn("estado_sismepre_id", self._estado_sismepre_id())
            .withColumn("has_sismepre", F.lit(True))
            .withColumn("year", F.col("ANO_APLICACION").cast("string"))
        )
        return self._publish(
            "fact_sismepre_cumplimiento", curated,
            required_columns=["SEC_EJEC", "ANO_APLICACION", "PERIODO", "estado_sismepre_id"],
            unique_keys=["SEC_EJEC", "ANO_APLICACION", "PERIODO"],
            partition_columns=["year"],
        )

    def build_fact_sismepre_respuestas_resumen(self) -> Dict[str, Any]:
        source = self._filter_scope(self._read("sismepre_respuestas_curated"))
        keys = ["SEC_EJEC", "ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID", "response_type"]
        curated = (
            source.groupBy(*keys)
            .agg(
                F.count("*").alias("response_count"),
                F.sum(F.col("source_multivalue").cast("int")).alias("source_multivalue_count"),
                F.min("response_value_decimal").alias("response_decimal_min"),
                F.max("response_value_decimal").alias("response_decimal_max"),
                F.avg("response_value_decimal").alias("response_decimal_avg"),
                F.min("response_value_integer").alias("response_integer_min"),
                F.max("response_value_integer").alias("response_integer_max"),
                F.avg("response_value_integer").alias("response_integer_avg"),
                F.first("response_value_text", ignorenulls=True).alias("response_text_example"),
                F.min("response_value_date").alias("response_date_min"),
                F.max("response_value_date").alias("response_date_max"),
                *self._first_lineage(source),
            )
            .withColumn("year", F.col("ANO_APLICACION").cast("string"))
        )
        return self._publish(
            "fact_sismepre_respuestas_resumen", curated,
            required_columns=keys + ["response_count"],
            unique_keys=keys,
            partition_columns=["year"],
        )

    def build_fact_renamu_gestion_tributaria(self) -> Dict[str, Any]:
        source = self._read_bronze("renamu")
        master = self._renamu_master_lookup()
        curated = (
            source.select(
                self._code("Ubigeo", 6).alias("UBIGEO"),
                F.col("Año").cast("int").alias("ANO_RENAMU"),
                F.col("idmunici").cast("string").alias("idmunici"),
                F.trim(F.col("Tipomuni")).alias("tipo_municipalidad_renamu"),
                self._int_or_zero("P19M_T").alias("personal_municipal_total"),
                self._is_selected("P22_AT2").alias("requiere_asistencia_at"),
                self._is_selected("P22_AT3").alias("requiere_asistencia_catastro"),
                self._is_selected("P22_C2").alias("requiere_capacitacion_at"),
                self._is_selected("P22_C3").alias("requiere_capacitacion_catastro"),
                *self._existing_columns(source, RENAMU_GESTION_COLUMNS),
                *self._lineage_columns(source),
            )
            .join(master, "UBIGEO", "inner")
            .withColumn("year", F.col("ANO_RENAMU").cast("string"))
        )
        return self._publish(
            "fact_renamu_gestion_tributaria",
            curated,
            required_columns=["SEC_EJEC", "UBIGEO", "ANO_RENAMU", "personal_municipal_total"],
            unique_keys=["SEC_EJEC", "ANO_RENAMU"],
            partition_columns=["year"],
        )

    def build_fact_renamu_software_at(self) -> Dict[str, Any]:
        source = self._read_bronze("renamu")
        master = self._renamu_master_lookup()
        curated = (
            source.select(
                self._code("Ubigeo", 6).alias("UBIGEO"),
                F.col("Año").cast("int").alias("ANO_RENAMU"),
                F.col("idmunici").cast("string").alias("idmunici"),
                F.trim(F.col("Tipomuni")).alias("tipo_municipalidad_renamu"),
                self._is_selected("P16_5").alias("usa_srtm_estado"),
                self._is_selected("P17_2").alias("usa_software_rentas_at"),
                self._is_selected("P17_3").alias("usa_software_catastro"),
                *self._existing_columns(source, RENAMU_SOFTWARE_COLUMNS),
                *self._lineage_columns(source),
            )
            .join(master, "UBIGEO", "inner")
            .withColumn(
                "usa_al_menos_un_software_at",
                F.col("usa_srtm_estado") | F.col("usa_software_rentas_at") | F.col("usa_software_catastro"),
            )
            .withColumn("year", F.col("ANO_RENAMU").cast("string"))
        )
        return self._publish(
            "fact_renamu_software_at",
            curated,
            required_columns=["SEC_EJEC", "UBIGEO", "ANO_RENAMU", "usa_srtm_estado", "usa_software_rentas_at", "usa_software_catastro"],
            unique_keys=["SEC_EJEC", "ANO_RENAMU"],
            partition_columns=["year"],
        )

    def build_mart_dashboard_municipal(self) -> Dict[str, Any]:
        municipalities = self.spark.read.parquet(str(self.storage.data_lake.gold_path / "dim_municipalidad_gold"))
        income = self._drop_pipeline_metadata(self.spark.read.parquet(str(self.storage.data_lake.gold_path / "fact_ingresos_mensuales")))
        predial = self._drop_pipeline_metadata(self.spark.read.parquet(str(self.storage.data_lake.gold_path / "fact_predial_mensual")))
        cumplimiento = self._drop_pipeline_metadata(self.spark.read.parquet(str(self.storage.data_lake.gold_path / "fact_sismepre_cumplimiento")))
        gestion = self._drop_pipeline_metadata(self.spark.read.parquet(str(self.storage.data_lake.gold_path / "fact_renamu_gestion_tributaria")))
        software = self._drop_pipeline_metadata(self.spark.read.parquet(str(self.storage.data_lake.gold_path / "fact_renamu_software_at")))

        income_annual = (
            income.groupBy("SEC_EJEC", "ANO_DOC")
            .agg(
                F.sum("MONTO_PIA").cast("decimal(24,2)").alias("MONTO_PIA"),
                F.sum("MONTO_PIM").cast("decimal(24,2)").alias("MONTO_PIM"),
                F.sum("MONTO_RECAUDADO").cast("decimal(24,2)").alias("MONTO_RECAUDADO"),
            )
            .withColumn("year", F.col("ANO_DOC").cast("string"))
        )
        predial_annual = (
            predial.groupBy("SEC_EJEC", "ANO_ESTADISTICA")
            .agg(
                F.sum("MON_RECAUDACION_TOTAL").alias("MON_RECAUDACION_TOTAL"),
                F.sum("MON_SALDO_PREDIAL_TOTAL").alias("MON_SALDO_PREDIAL_TOTAL"),
                F.sum("MON_BASEIMPONIBLE_AFECTO").alias("MON_BASEIMPONIBLE_AFECTO"),
            )
            .withColumnRenamed("ANO_ESTADISTICA", "ANO_DOC")
        )
        cumplimiento_annual = (
            cumplimiento.groupBy("SEC_EJEC", F.col("ANO_APLICACION").alias("ANO_DOC"))
            .agg(
                F.first("ESTADO", ignorenulls=True).alias("ESTADO_SISMEPRE"),
                F.first("CLASIFICACION", ignorenulls=True).alias("CLASIFICACION_SISMEPRE"),
                F.first("TIPO_META", ignorenulls=True).alias("TIPO_META_SISMEPRE"),
            )
        )
        gestion_for_mart = gestion.drop("UBIGEO", "year")
        software_for_mart = software.drop(
            "UBIGEO",
            "year",
            "idmunici",
            "tipo_municipalidad_renamu",
            "ANO_RENAMU",
        )
        curated = (
            income_annual
            .join(predial_annual, ["SEC_EJEC", "ANO_DOC"], "left")
            .join(cumplimiento_annual, ["SEC_EJEC", "ANO_DOC"], "left")
            .join(gestion_for_mart, "SEC_EJEC", "left")
            .join(software_for_mart, "SEC_EJEC", "left")
            .join(
                municipalities.select(
                    "SEC_EJEC", "UBIGEO", "MUNICIPALIDAD_NOMBRE", "DEPARTAMENTO_NOMBRE",
                    "PROVINCIA_NOMBRE", "DISTRITO_NOMBRE", "categoria_municipalidad",
                    "categoria_match_status", "categoria_rule_applied",
                    "exclude_from_gold_scope", "in_scope_presentacion",
                ),
                "SEC_EJEC",
                "left",
            )
            .withColumn(
                "PCT_EJECUCION",
                F.when(F.col("MONTO_PIM") != 0, (F.col("MONTO_RECAUDADO") / F.col("MONTO_PIM") * 100).cast("decimal(20,4)")),
            )
            .withColumn(
                "PCT_RECUPERACION_PREDIAL",
                F.when(
                    F.coalesce(F.col("MON_RECAUDACION_TOTAL"), F.lit(0)) + F.coalesce(F.col("MON_SALDO_PREDIAL_TOTAL"), F.lit(0)) != 0,
                    (
                        F.coalesce(F.col("MON_RECAUDACION_TOTAL"), F.lit(0))
                        / (F.coalesce(F.col("MON_RECAUDACION_TOTAL"), F.lit(0)) + F.coalesce(F.col("MON_SALDO_PREDIAL_TOTAL"), F.lit(0)))
                        * 100
                    ).cast("decimal(20,4)"),
                ),
            )
            .withColumn("year", F.col("ANO_DOC").cast("string"))
        )
        return self._publish(
            "mart_dashboard_municipal",
            curated,
            required_columns=["SEC_EJEC", "ANO_DOC", "MONTO_RECAUDADO", "categoria_municipalidad"],
            unique_keys=["SEC_EJEC", "ANO_DOC"],
            partition_columns=["year"],
        )

    def build_fact_calidad_datos(self) -> Dict[str, Any]:
        quality_path = Path(self.audit_path) / "quality_checks"
        if not quality_path.exists():
            raise ValueError(f"Audit quality checks path does not exist: {quality_path}")
        source = (
            self.spark.read.option("recursiveFileLookup", "true")
            .option("multiLine", "true")
            .json(str(quality_path))
        )
        curated = (
            source.select(
                "check_id", "check_name", "check_type", "status", "timestamp", "dataset",
                "records_checked", "records_passed", "records_failed", "failure_rate",
            )
            .withColumn(
                "layer",
                F.when(F.col("check_name").startswith("gold_"), F.lit("gold"))
                .when(F.col("check_name").startswith("silver_"), F.lit("silver"))
                .otherwise(F.lit("bronze")),
            )
            .withColumn("year", F.year("timestamp").cast("string"))
            .dropDuplicates(["check_id"])
        )
        return self._publish(
            "fact_calidad_datos", curated,
            required_columns=["check_id", "check_name", "check_type", "status", "dataset", "layer"],
            unique_keys=["check_id"],
            partition_columns=["year"],
        )

    def _publish(
        self,
        table_name: str,
        df: DataFrame,
        required_columns: List[str],
        unique_keys: List[str],
        partition_columns: List[str] | None = None,
        details: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        curated = self._add_gold_metadata(df).persist(StorageLevel.DISK_ONLY)
        records_published = curated.count()
        if records_published == 0:
            curated.unpersist()
            raise ValueError(f"Gold table {table_name} is empty")
        self.storage.clear_table(table_name)
        storage_result = self.storage.write_table(curated, table_name, partition_columns)
        quality_checks = self.quality_analyzer.analyze(
            curated, table_name, required_columns, unique_keys, details=details
        )
        curated.unpersist()
        return {
            "table_name": table_name,
            "status": "published",
            "records_published": records_published,
            "storage": storage_result,
            "quality_checks": quality_checks,
        }

    def _read(self, table_name: str) -> DataFrame:
        path = Path(self.silver_path) / table_name
        if not path.exists():
            raise ValueError(f"Required Silver table does not exist: {path}")
        return self.spark.read.parquet(str(path))

    def _read_bronze(self, table_name: str) -> DataFrame:
        path = self.storage.data_lake.bronze_path / table_name
        if not path.exists():
            raise ValueError(f"Required Bronze table does not exist: {path}")
        return self.spark.read.parquet(str(path))

    def _renamu_master_lookup(self) -> DataFrame:
        master_path = self.storage.data_lake.gold_path / "dim_municipalidad_gold"
        if not master_path.exists() or not any(master_path.rglob("*.parquet")):
            raise ValueError("Gold dim_municipalidad_gold must be published before RENAMU facts")
        master = self.spark.read.parquet(str(master_path))
        if "in_scope_presentacion" in master.columns and (self.scope_metrics or {}).get("scope_status") == "aplicado":
            master = master.filter("in_scope_presentacion")
        if "exclude_from_gold_scope" in master.columns:
            master = master.filter(~F.col("exclude_from_gold_scope"))
        return master.select("SEC_EJEC", "UBIGEO").where("SEC_EJEC IS NOT NULL AND UBIGEO IS NOT NULL").dropDuplicates(["SEC_EJEC"])

    def _filter_scope(self, df: DataFrame) -> DataFrame:
        if "SEC_EJEC" not in df.columns:
            return df
        master_path = self.storage.data_lake.gold_path / "dim_municipalidad_gold"
        if not master_path.exists() or not any(master_path.rglob("*.parquet")):
            return df
        master_source = self.spark.read.parquet(str(master_path))
        if "in_scope_presentacion" not in master_source.columns:
            return df
        select_columns = ["SEC_EJEC", "in_scope_presentacion"]
        if "exclude_from_gold_scope" in master_source.columns:
            select_columns.append("exclude_from_gold_scope")
        master = master_source.select(*select_columns)
        if "exclude_from_gold_scope" in master.columns:
            master = master.filter(~F.col("exclude_from_gold_scope"))
        scope_state = self.scope_metrics or {}
        if scope_state.get("scope_status") != "aplicado":
            return df.join(master.select("SEC_EJEC"), "SEC_EJEC", "inner")
        return df.join(master.filter("in_scope_presentacion").select("SEC_EJEC"), "SEC_EJEC", "inner")

    def _write_scope_metrics(self, metrics: Dict[str, Any]) -> None:
        date_partition = datetime.now().strftime("%Y/%m/%d")
        output_dir = Path(self.audit_path) / "metrics" / date_partition
        output_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "execution_id": self.execution_id,
            "timestamp": datetime.now().isoformat(),
            **metrics,
        }
        with open(output_dir / f"scope_municipalidades_{self.execution_id}.json", "w", encoding="utf-8") as file_handle:
            json.dump(payload, file_handle, indent=2, ensure_ascii=False)

    def _enrich_with_category(self, municipalities: DataFrame) -> DataFrame:
        category_path = Path(self.silver_path) / "categorias_municipalidades_curated"
        if not category_path.exists() or not any(category_path.rglob("*.parquet")):
            result = (
                municipalities
                .withColumn("categoria_municipalidad", F.lit(None).cast("string"))
                .withColumn("categoria_match_status", F.lit("missing_category_source"))
                .withColumn("categoria_rule_applied", F.lit("missing_category_source"))
                .withColumn("exclude_from_gold_scope", F.lit(False))
            )
            self._write_category_metrics({
                "category_status": "missing_category_source",
                "total_municipalities": municipalities.count(),
                "category_source_count": 0,
                "matched_count": 0,
                "unmatched_count": 0,
                "resolved_multiple_lima_count": 0,
                "resolved_multiple_non_lima_count": 0,
                "excluded_count": 0,
            })
            return result

        category_source = self.spark.read.parquet(str(category_path))
        if "SEC_EJEC" in category_source.columns:
            category_columns = [
                "SEC_EJEC", "categoria_municipalidad", "categoria_match_status",
                "categoria_rule_applied", "exclude_from_gold_scope",
            ]
            categories = category_source.select(
                *[column for column in category_columns if column in category_source.columns]
            ).dropDuplicates(["SEC_EJEC"])
            enriched = (
                municipalities.join(categories, "SEC_EJEC", "left")
                .withColumn("categoria_match_status", F.coalesce(F.col("categoria_match_status"), F.lit("unmatched")))
                .withColumn("categoria_rule_applied", F.coalesce(F.col("categoria_rule_applied"), F.lit("no_master_match")))
                .withColumn("exclude_from_gold_scope", F.coalesce(F.col("exclude_from_gold_scope"), F.lit(True)))
            )
            self._write_category_metrics({
                "category_status": "aplicado_desde_silver",
                "total_municipalities": enriched.count(),
                "category_source_count": categories.count(),
                "matched_count": enriched.filter("categoria_match_status = 'matched'").count(),
                "resolved_multiple_lima_count": enriched.filter("categoria_match_status = 'resolved_multiple_lima'").count(),
                "resolved_multiple_non_lima_count": enriched.filter("categoria_match_status = 'resolved_multiple_non_lima'").count(),
                "unmatched_count": enriched.filter("categoria_match_status = 'unmatched'").count(),
                "excluded_count": enriched.filter("exclude_from_gold_scope").count(),
            })
            return enriched

        enriched = (
            municipalities
            .withColumn("categoria_municipalidad", F.lit(None).cast("string"))
            .withColumn("categoria_match_status", F.lit("category_source_without_sec_ejec"))
            .withColumn("categoria_rule_applied", F.lit("category_source_without_sec_ejec"))
            .withColumn("exclude_from_gold_scope", F.lit(True))
        )
        self._write_category_metrics({
            "category_status": "category_source_without_sec_ejec",
            "total_municipalities": enriched.count(),
            "category_source_count": category_source.count(),
            "matched_count": 0,
            "unmatched_count": enriched.count(),
            "excluded_count": enriched.count(),
        })
        return enriched

    def _write_category_metrics(self, metrics: Dict[str, Any]) -> None:
        date_partition = datetime.now().strftime("%Y/%m/%d")
        output_dir = Path(self.audit_path) / "metrics" / date_partition
        output_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "execution_id": self.execution_id,
            "timestamp": datetime.now().isoformat(),
            **metrics,
        }
        with open(output_dir / f"category_match_{self.execution_id}.json", "w", encoding="utf-8") as file_handle:
            json.dump(payload, file_handle, indent=2, ensure_ascii=False)

    def _normalize_municipality_name(self, value):
        normalized = F.upper(F.trim(value))
        normalized = F.translate(normalized, "ÁÉÍÓÚÜÑáéíóúüñ", "AEIOUUNAEIOUUN")
        normalized = F.regexp_replace(normalized, r"\bM\s*\.\s*D\s*\.\s*DE\b", "M D DE")
        normalized = F.regexp_replace(normalized, r"\bM\s*\.\s*P\s*\.\s*DE\b", "M P DE")
        normalized = F.regexp_replace(normalized, r"\bMUNICIPALIDAD\b", "M")
        normalized = F.regexp_replace(normalized, r"\bDISTRITAL\b", "D")
        normalized = F.regexp_replace(normalized, r"\bPROVINCIAL\b", "P")
        normalized = F.regexp_replace(normalized, r"[^A-Z0-9]+", " ")
        normalized = F.regexp_replace(normalized, r"\s+", " ")
        return F.trim(normalized)

    def _code(self, column: str, width: int):
        return F.lpad(F.regexp_replace(F.trim(F.col(column).cast("string")), r"\.0$", ""), width, "0")

    def _int_or_zero(self, column: str):
        value = F.trim(F.col(column).cast("string"))
        return F.when(value.rlike(r"^-?\d+$"), value.cast("int")).otherwise(F.lit(0))

    def _is_selected(self, column: str):
        value = F.trim(F.col(column).cast("string"))
        return F.when(value.rlike(r"^-?\d+$"), value.cast("int") > 0).otherwise(F.lit(False))

    def _existing_columns(self, df: DataFrame, columns: List[str]):
        return [F.col(column).cast("string").alias(f"{column}_raw") for column in columns if column in df.columns]

    def _drop_pipeline_metadata(self, df: DataFrame) -> DataFrame:
        metadata_columns = [
            column for column in df.columns
            if column.startswith("_bronze_") or column.startswith("_silver_") or column.startswith("_gold_")
        ]
        return df.drop(*metadata_columns)

    def _add_gold_metadata(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("_gold_execution_id", F.lit(self.execution_id))
            .withColumn("_gold_ingestion_ts", F.current_timestamp())
        )

    def _lineage_columns(self, df: DataFrame):
        return [
            F.col(column)
            for column in df.columns
            if column.startswith("_bronze_") or column.startswith("_silver_")
        ]

    def _first_lineage(self, df: DataFrame):
        return [
            F.first(column, ignorenulls=True).alias(column)
            for column in df.columns
            if column.startswith("_bronze_") or column.startswith("_silver_")
        ]

    def _classifier_id(self):
        return F.sha2(F.concat_ws("|", *[F.col(column).cast("string") for column in CLASSIFIER_COLUMNS]), 256)

    def _estado_sismepre_id(self):
        return F.sha2(
            F.concat_ws(
                "|",
                F.coalesce(F.col("ESTADO").cast("string"), F.lit("")),
                F.coalesce(F.col("CLASIFICACION").cast("string"), F.lit("")),
                F.coalesce(F.col("TIPO_META").cast("string"), F.lit("")),
            ),
            256,
        )

    def _source_row_count(self, df: DataFrame):
        if "_silver_source_row_count" in df.columns:
            return F.sum("_silver_source_row_count").alias("_gold_source_row_count")
        return F.count("*").alias("_gold_source_row_count")
