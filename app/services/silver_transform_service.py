from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel

from app.quality.silver_quality_analyzer import SilverQualityAnalyzer
from app.storage.silver_storage import SilverStorage


TRACE_COLUMNS = [
    "_bronze_source_path",
    "_bronze_source_url",
    "_bronze_source_checksum",
    "_bronze_execution_id",
    "_bronze_ingestion_ts",
    "_bronze_ingestion_date",
]
RENAMU_GESTION_COLUMNS = ["P19M_T", "P22_AT2", "P22_AT3", "P22_C2", "P22_C3"]
RENAMU_SOFTWARE_COLUMNS = ["P16_5", "P17_2", "P17_3"]


class SilverTransformService:
    def __init__(
        self,
        spark: SparkSession,
        storage: SilverStorage,
        quality_analyzer: SilverQualityAnalyzer,
        execution_id: str,
        bronze_path: str,
        required_government_level: str = "M",
        quarantine_enabled: bool = True,
    ):
        self.spark = spark
        self.storage = storage
        self.quality_analyzer = quality_analyzer
        self.execution_id = execution_id
        self.bronze_path = bronze_path
        self.required_government_level = required_government_level
        self.quarantine_enabled = quarantine_enabled

    def build_municipalidades_curated(self) -> Dict[str, Any]:
        esat = self._read("sismepre/rentas_esat_estadistica_atm")
        renamu = self._read("renamu")
        mappings = (
            esat.select(
                self._code("SEC_EJEC", 6).alias("SEC_EJEC"),
                self._code("UBIGEO", 6).alias("UBIGEO"),
                self._upper("DEPARTAMENTO_NOMBRE").alias("DEPARTAMENTO_NOMBRE"),
                self._upper("PROVINCIA_NOMBRE").alias("PROVINCIA_NOMBRE"),
                self._upper("DISTRITO_NOMBRE").alias("DISTRITO_NOMBRE"),
                self._upper("MUNICIPALIDAD_NOMBRE").alias("MUNICIPALIDAD_NOMBRE"),
                *self._existing_trace_columns(esat),
            )
            .where(F.col("SEC_EJEC").isNotNull() & F.col("UBIGEO").isNotNull())
            .dropDuplicates(["SEC_EJEC", "UBIGEO"])
        )
        renamu_dim = (
            renamu.select(
                self._code("Ubigeo", 6).alias("UBIGEO"),
                self._code("idmunici", 6).alias("idmunici"),
                F.trim(F.col("Tipomuni")).alias("Tipomuni"),
                self._upper("Departamento").alias("RENAMU_DEPARTAMENTO"),
                self._upper("Provincia").alias("RENAMU_PROVINCIA"),
                self._upper("Distrito").alias("RENAMU_DISTRITO"),
            )
            .dropDuplicates(["UBIGEO"])
        )
        curated = (
            mappings.join(renamu_dim, "UBIGEO", "left")
            .withColumn("renamu_match", F.col("idmunici").isNotNull())
        )
        return self._publish(
            "municipalidades_curated",
            curated,
            required_columns=["SEC_EJEC", "UBIGEO", "MUNICIPALIDAD_NOMBRE"],
            unique_keys=["SEC_EJEC", "UBIGEO"],
            details={"renamu_match_count": curated.filter("renamu_match").count()},
        )

    def build_renamu_curated(self) -> Dict[str, Any]:
        source = self._read("renamu")
        curated = (
            source.select(
                self._code("Ubigeo", 6).alias("UBIGEO"),
                F.col("Año").cast("int").alias("ANO_RENAMU"),
                F.col("idmunici").cast("string").alias("idmunici"),
                F.trim(F.col("Tipomuni")).alias("tipo_municipalidad_renamu"),
                self._upper("Departamento").alias("DEPARTAMENTO_NOMBRE"),
                self._upper("Provincia").alias("PROVINCIA_NOMBRE"),
                self._upper("Distrito").alias("DISTRITO_NOMBRE"),
                self._int_or_zero("P19M_T").alias("personal_municipal_total"),
                self._is_selected("P22_AT2").alias("requiere_asistencia_at"),
                self._is_selected("P22_AT3").alias("requiere_asistencia_catastro"),
                self._is_selected("P22_C2").alias("requiere_capacitacion_at"),
                self._is_selected("P22_C3").alias("requiere_capacitacion_catastro"),
                self._is_selected("P16_5").alias("usa_srtm_estado"),
                self._is_selected("P17_2").alias("usa_software_rentas_at"),
                self._is_selected("P17_3").alias("usa_software_catastro"),
                *self._existing_raw_columns(source, RENAMU_GESTION_COLUMNS + RENAMU_SOFTWARE_COLUMNS),
                *self._existing_trace_columns(source),
            )
            .withColumn(
                "usa_al_menos_un_software_at",
                F.col("usa_srtm_estado") | F.col("usa_software_rentas_at") | F.col("usa_software_catastro"),
            )
            .withColumn("year", F.col("ANO_RENAMU").cast("string"))
        )
        invalid = self._any_null(["UBIGEO", "ANO_RENAMU"])
        quarantine = curated.filter(invalid).withColumn("_quarantine_reason", F.lit("missing_renamu_key_or_year"))
        conformed = curated.filter(~invalid).dropDuplicates(["UBIGEO", "ANO_RENAMU"])
        return self._publish(
            "renamu_curated",
            conformed,
            required_columns=["UBIGEO", "ANO_RENAMU", "personal_municipal_total"],
            unique_keys=["UBIGEO", "ANO_RENAMU"],
            quarantine=quarantine,
            partition_columns=["year"],
        )

    def build_ingresos_municipales_curated(self) -> Dict[str, Any]:
        source = self._read("ingresos")
        municipal = source.filter(F.col("NIVEL_GOBIERNO") == self.required_government_level)
        source_count = source.count()
        municipal_count = municipal.count()
        if municipal_count == 0:
            self.storage.clear_table("ingresos_municipales_curated")
            self.storage.clear_quarantine("ingresos_municipales_curated")
            return {
                "table_name": "ingresos_municipales_curated",
                "status": "blocked",
                "reason": (
                    f"Bronze ingresos contains {source_count} rows but no rows with "
                    f"NIVEL_GOBIERNO={self.required_government_level!r}"
                ),
                "records_published": 0,
                "records_quarantined": 0,
            }

        decimal_columns = ["MONTO_PIA", "MONTO_PIM", "MONTO_RECAUDADO"]
        curated = municipal
        for column in decimal_columns:
            curated = curated.withColumn(f"_raw_{column}", F.col(column).cast("string"))
        for column in ["ANO_DOC", "MES_DOC"]:
            curated = curated.withColumn(column, F.col(column).cast("int"))
        for column in decimal_columns:
            curated = curated.withColumn(column, F.col(column).cast("decimal(20,2)"))
        curated = curated.withColumn("year", F.col("ANO_DOC").cast("string"))
        key_columns = [
            "ANO_DOC", "MES_DOC", "SEC_EJEC", "EJECUTORA", "FUENTE_FINANCIAMIENTO",
            "RUBRO", "TIPO_RECURSO", "GENERICA", "SUBGENERICA", "SUBGENERICA_DET",
            "ESPECIFICA", "ESPECIFICA_DET",
        ]
        invalid = self._any_null(key_columns + decimal_columns) | ~F.col("MES_DOC").between(1, 12)
        quarantine = curated.filter(invalid).withColumn(
            "_quarantine_reason", F.lit("invalid_key_month_or_amount")
        )
        valid = curated.filter(~invalid).drop(*[f"_raw_{column}" for column in decimal_columns])
        lineage_columns = [column for column in valid.columns if column.startswith("_bronze_")]
        attribute_columns = [
            column for column in valid.columns
            if column not in set(key_columns + decimal_columns + lineage_columns + ["year"])
        ]
        aggregations = [
            F.sum(column).cast("decimal(20,2)").alias(column) for column in decimal_columns
        ] + [
            F.first(column, ignorenulls=True).alias(column)
            for column in attribute_columns + lineage_columns
        ] + [
            F.count("*").alias("_silver_source_row_count")
        ]
        conformed = (
            valid.repartition(96, *key_columns)
            .groupBy(*key_columns)
            .agg(*aggregations)
            .withColumn("year", F.col("ANO_DOC").cast("string"))
            .repartition(96, "year", "SEC_EJEC")
        )
        self.storage.clear_table("ingresos_municipales_curated")
        return self._publish(
            "ingresos_municipales_curated",
            conformed,
            required_columns=key_columns + decimal_columns,
            unique_keys=key_columns,
            quarantine=quarantine,
            partition_columns=["year"],
        )

    def build_predial_esat_curated(self) -> Dict[str, Any]:
        source = self._read("sismepre/rentas_esat_estadistica_atm")
        integer_columns = ["ANO_APLICACION", "PERIODO", "ANO_ESTADISTICA", "MES_ESTADISTICA", "FORMULARIO_ID"]
        monetary_columns = [name for name in source.columns if name.startswith("MON_")]
        count_columns = [name for name in source.columns if name.startswith("NUM_")]
        typed_columns = [
            (column, "int") for column in integer_columns
        ] + [
            (column, "decimal(20,2)") for column in monetary_columns
        ] + [
            (column, "long") for column in count_columns
        ]
        invalid_metrics = [
            (F.trim(F.col(column).cast("string")) != "") & F.col(column).isNotNull() & F.col(column).cast(data_type).isNull()
            for column, data_type in typed_columns
        ]
        curated = source.withColumn("_invalid_metric", self._any(invalid_metrics))
        for column, _ in typed_columns:
            curated = curated.withColumn(f"_raw_{column}", F.col(column).cast("string"))
        for column in integer_columns:
            curated = curated.withColumn(column, F.col(column).cast("int"))
        for column in monetary_columns:
            curated = curated.withColumn(column, F.col(column).cast("decimal(20,2)"))
        for column in count_columns:
            curated = curated.withColumn(column, F.col(column).cast("long"))
        curated = (
            curated.withColumn("SEC_EJEC", self._code("SEC_EJEC", 6))
            .withColumn("UBIGEO", self._code("UBIGEO", 6))
            .withColumn("DEPARTAMENTO_NOMBRE", self._upper("DEPARTAMENTO_NOMBRE"))
            .withColumn("PROVINCIA_NOMBRE", self._upper("PROVINCIA_NOMBRE"))
            .withColumn("DISTRITO_NOMBRE", self._upper("DISTRITO_NOMBRE"))
            .withColumn("MUNICIPALIDAD_NOMBRE", self._upper("MUNICIPALIDAD_NOMBRE"))
            .withColumn("year", F.col("ANO_APLICACION").cast("string"))
        )
        keys = ["SEC_EJEC", "ANO_APLICACION", "PERIODO", "ANO_ESTADISTICA", "MES_ESTADISTICA", "FORMULARIO_ID"]
        invalid = self._any_null(keys) | F.col("_invalid_metric")
        quarantine = curated.filter(invalid).withColumn("_quarantine_reason", F.lit("invalid_predial_key_or_metric"))
        conformed = curated.filter(~invalid).drop(
            "_invalid_metric", *[f"_raw_{column}" for column, _ in typed_columns]
        ).dropDuplicates(keys)
        return self._publish(
            "predial_esat_curated",
            conformed,
            required_columns=keys + ["UBIGEO"],
            unique_keys=keys,
            quarantine=quarantine,
            partition_columns=["year"],
        )

    def build_sismepre_respuestas_curated(self) -> Dict[str, Any]:
        source = self._read("sismepre/rentas_respuestas")
        base = source.withColumn("SEC_EJEC", self._code("SEC_EJEC", 6))
        entries = [
            ("texto", "RESPUESTA_TEXTO", True),
            ("decimal", "RESPUESTA_DECIMAL", False),
            ("entero", "RESPUESTA_ENTERO", False),
            ("fecha", "RESPUESTA_FECHA", True),
        ]
        structs = [
            F.struct(
                F.lit(response_type).alias("response_type"),
                F.trim(F.col(column)).alias("raw_value"),
                (
                    F.trim(F.col(column)).isNotNull()
                    & (F.trim(F.col(column)) != "")
                    & (F.lit(allow_zero) | (F.trim(F.col(column)) != "0"))
                ).alias("active"),
            )
            for response_type, column, allow_zero in entries
        ]
        base = (
            base.withColumn("_responses", F.array(*structs))
            .withColumn(
                "_active_responses",
                F.filter(
                    "_responses",
                    lambda item: item["active"],
                ),
            )
            .withColumn("source_multivalue", F.size("_active_responses") > 1)
            .withColumn("year", F.col("ANO_APLICACION"))
        )
        empty_quarantine = (
            base.filter(F.size("_active_responses") == 0)
            .withColumn("response_type", F.lit(None).cast("string"))
            .withColumn("response_raw_value", F.lit(None).cast("string"))
            .withColumn("_quarantine_reason", F.lit("no_active_response"))
        )
        exploded = (
            base.filter(F.size("_active_responses") > 0)
            .withColumn("_response", F.explode("_active_responses"))
            .withColumn("response_type", F.col("_response.response_type"))
            .withColumn("response_raw_value", F.col("_response.raw_value"))
            .withColumn("response_value_text", F.when(F.col("response_type") == "texto", F.col("response_raw_value")))
            .withColumn("response_value_decimal", F.when(F.col("response_type") == "decimal", F.col("response_raw_value").cast("decimal(20,4)")))
            .withColumn("response_value_integer", F.when(F.col("response_type") == "entero", F.col("response_raw_value").cast("long")))
            .withColumn(
                "response_value_date",
                F.when(F.col("response_type") == "fecha", self._parse_date(F.col("response_raw_value"))),
            )
        )
        invalid_typed = (
            ((F.col("response_type") == "decimal") & F.col("response_value_decimal").isNull())
            | ((F.col("response_type") == "entero") & F.col("response_value_integer").isNull())
            | ((F.col("response_type") == "fecha") & F.col("response_value_date").isNull())
        )
        invalid_quarantine = exploded.filter(invalid_typed).withColumn(
            "_quarantine_reason", F.lit("unparseable_typed_response")
        )
        quarantine = empty_quarantine.unionByName(invalid_quarantine, allowMissingColumns=True).drop(
            "_responses", "_active_responses", "_response"
        )
        conformed = exploded.filter(~invalid_typed).drop("_responses", "_active_responses", "_response")
        keys = ["SEC_EJEC", "ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID", "RESPUESTA_ID", "response_type"]
        return self._publish(
            "sismepre_respuestas_curated",
            conformed,
            required_columns=keys + ["response_raw_value"],
            unique_keys=keys,
            quarantine=quarantine,
            partition_columns=["year"],
            details={"source_multivalue_rows": base.filter("source_multivalue").count()},
        )

    def build_categorias_municipalidades_curated(self) -> Dict[str, Any]:
        source = self._read("categorias_municipalidades")
        curated = (
            source.select(
                F.trim(F.col("Municipalidad")).alias("municipalidad_categoria_raw"),
                F.upper(F.trim(F.col("Categoria"))).alias("categoria_municipalidad"),
                *self._existing_trace_columns(source),
            )
            .withColumn(
                "municipalidad_categoria_norm",
                self._normalize_municipality_name(F.col("municipalidad_categoria_raw")),
            )
        )
        invalid = (
            F.col("municipalidad_categoria_raw").isNull()
            | (F.col("municipalidad_categoria_raw") == "")
            | F.col("categoria_municipalidad").isNull()
            | ~F.col("categoria_municipalidad").isin("A", "B", "C", "D", "E", "F", "G")
        )
        invalid_quarantine = curated.filter(invalid).withColumn(
            "_quarantine_reason", F.lit("invalid_category_or_name")
        )
        valid = curated.filter(~invalid)

        category_master = (
            valid.groupBy("municipalidad_categoria_norm")
            .agg(
                F.first("municipalidad_categoria_raw", ignorenulls=True).alias("municipalidad_categoria_raw"),
                F.array_sort(F.collect_set("categoria_municipalidad")).alias("categorias_maestro"),
                F.countDistinct("categoria_municipalidad").alias("categoria_distinct_count"),
                *[
                    F.first(column, ignorenulls=True).alias(column)
                    for column in TRACE_COLUMNS
                    if column in valid.columns
                ],
            )
        )

        municipal_path = self.storage.data_lake.silver_path / "municipalidades_curated"
        if municipal_path.exists() and any(municipal_path.rglob("*.parquet")):
            municipalities = (
                self.spark.read.parquet(str(municipal_path))
                .select(
                    "SEC_EJEC", "UBIGEO", "MUNICIPALIDAD_NOMBRE",
                    "DEPARTAMENTO_NOMBRE", "PROVINCIA_NOMBRE", "DISTRITO_NOMBRE",
                )
                .dropDuplicates(["SEC_EJEC"])
                .withColumn(
                    "municipalidad_categoria_norm",
                    self._normalize_municipality_name(F.col("MUNICIPALIDAD_NOMBRE")),
                )
            )
            conformed = (
                municipalities.join(category_master, "municipalidad_categoria_norm", "left")
                .withColumn("_has_master_category", F.col("categoria_distinct_count").isNotNull())
                .withColumn("_is_lima", self._is_lima_municipality())
                .withColumn(
                    "categoria_municipalidad",
                    F.when(F.col("categoria_distinct_count") == 1, F.element_at("categorias_maestro", 1))
                    .when(F.col("categoria_distinct_count") > 1, F.when(F.col("_is_lima"), F.lit("C")).otherwise(F.lit("G"))),
                )
                .withColumn(
                    "categoria_match_status",
                    F.when(~F.col("_has_master_category"), F.lit("unmatched"))
                    .when(F.col("categoria_distinct_count") == 1, F.lit("matched"))
                    .when(F.col("_is_lima"), F.lit("resolved_multiple_lima"))
                    .otherwise(F.lit("resolved_multiple_non_lima")),
                )
                .withColumn(
                    "categoria_rule_applied",
                    F.when(~F.col("_has_master_category"), F.lit("no_master_match"))
                    .when(F.col("categoria_distinct_count") == 1, F.lit("unique_master_category"))
                    .when(F.col("_is_lima"), F.lit("multiple_master_lima_to_c"))
                    .otherwise(F.lit("multiple_master_non_lima_to_g")),
                )
                .withColumn("exclude_from_gold_scope", ~F.col("_has_master_category"))
                .drop("_has_master_category", "_is_lima")
            )
        else:
            conformed = (
                category_master
                .withColumn(
                    "categoria_municipalidad",
                    F.when(F.col("categoria_distinct_count") == 1, F.element_at("categorias_maestro", 1)),
                )
                .withColumn(
                    "categoria_match_status",
                    F.when(F.col("categoria_distinct_count") == 1, F.lit("matched"))
                    .otherwise(F.lit("requires_municipal_context")),
                )
                .withColumn(
                    "categoria_rule_applied",
                    F.when(F.col("categoria_distinct_count") == 1, F.lit("unique_master_category"))
                    .otherwise(F.lit("multiple_master_requires_municipal_context")),
                )
                .withColumn("exclude_from_gold_scope", F.lit(False))
            )

        conformed = (
            conformed
            .withColumn("categoria_source", F.lit("CategoriasMunicipalidades.csv"))
            .dropDuplicates(["SEC_EJEC"] if "SEC_EJEC" in conformed.columns else ["municipalidad_categoria_norm"])
        )
        return self._publish(
            "categorias_municipalidades_curated",
            conformed,
            required_columns=["municipalidad_categoria_norm", "categoria_match_status", "categoria_rule_applied"],
            unique_keys=["SEC_EJEC"] if "SEC_EJEC" in conformed.columns else ["municipalidad_categoria_norm"],
            quarantine=invalid_quarantine,
            details={
                "category_master_keys": category_master.count(),
                "resolved_multiple_lima": conformed.filter("categoria_rule_applied = 'multiple_master_lima_to_c'").count(),
                "resolved_multiple_non_lima": conformed.filter("categoria_rule_applied = 'multiple_master_non_lima_to_g'").count(),
                "unmatched_municipalities": conformed.filter("categoria_match_status = 'unmatched'").count(),
            },
        )

    def build_categorias_municipalidades_lookup(self) -> Dict[str, Any]:
        source = self._read("categorias_municipalidades")
        curated = (
            source.select(
                F.trim(F.col("Municipalidad")).alias("municipalidad_categoria_raw"),
                F.upper(F.trim(F.col("Categoria"))).alias("categoria_municipalidad"),
                *self._existing_trace_columns(source),
            )
            .withColumn(
                "municipalidad_categoria_norm",
                self._normalize_municipality_name(F.col("municipalidad_categoria_raw")),
            )
        )
        valid = curated.filter(
            F.col("municipalidad_categoria_raw").isNotNull()
            & (F.col("municipalidad_categoria_raw") != "")
            & F.col("categoria_municipalidad").isin("A", "B", "C", "D", "E", "F", "G")
        )
        lookup = (
            valid.groupBy("municipalidad_categoria_norm")
            .agg(
                F.first("municipalidad_categoria_raw", ignorenulls=True).alias("municipalidad_categoria_raw"),
                F.array_sort(F.collect_set("categoria_municipalidad")).alias("categorias_maestro"),
                F.countDistinct("categoria_municipalidad").alias("categoria_distinct_count"),
                *[
                    F.first(column, ignorenulls=True).alias(column)
                    for column in TRACE_COLUMNS
                    if column in valid.columns
                ],
            )
            .withColumn("categoria_source", F.lit("CategoriasMunicipalidades.csv"))
        )
        return self._publish(
            "categorias_municipalidades_lookup",
            lookup,
            required_columns=["municipalidad_categoria_norm", "categorias_maestro", "categoria_distinct_count"],
            unique_keys=["municipalidad_categoria_norm"],
            details={
                "category_master_keys": lookup.count(),
                "single_category_keys": lookup.filter("categoria_distinct_count = 1").count(),
                "multiple_category_keys": lookup.filter("categoria_distinct_count > 1").count(),
            },
        )

    def build_curated_dataset(
        self,
        table_name: str,
        bronze_relative_path: str,
        required_columns: List[str],
        unique_keys: List[str],
    ) -> Dict[str, Any]:
        curated = self._read(bronze_relative_path)
        if "ANO_APLICACION" in curated.columns:
            curated = curated.withColumn("year", F.col("ANO_APLICACION"))
        if "SEC_EJEC" in curated.columns:
            curated = curated.withColumn("SEC_EJEC", self._code("SEC_EJEC", 6))
        invalid = self._any_null(required_columns)
        quarantine = curated.filter(invalid).withColumn("_quarantine_reason", F.lit("missing_required_dimension_value"))
        conformed = curated.filter(~invalid).dropDuplicates(unique_keys)
        return self._publish(
            table_name,
            conformed,
            required_columns=required_columns,
            unique_keys=unique_keys,
            quarantine=quarantine,
            partition_columns=["year"],
        )

    def _publish(
        self,
        table_name: str,
        df: DataFrame,
        required_columns: List[str],
        unique_keys: List[str],
        quarantine: DataFrame | None = None,
        partition_columns: List[str] | None = None,
        details: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        curated = self._add_silver_metadata(df).persist(StorageLevel.DISK_ONLY)
        records_published = curated.count()
        storage_result = self.storage.write_table(curated, table_name, partition_columns)
        quarantine_count = 0
        quarantine_result = None
        if quarantine is not None and self.quarantine_enabled:
            quarantine = self._add_silver_metadata(quarantine).persist(StorageLevel.DISK_ONLY)
            quarantine_count = quarantine.count()
            if quarantine_count:
                self.storage.clear_quarantine(table_name)
                quarantine_result = self.storage.write_quarantine(quarantine, table_name, partition_columns)
            else:
                self.storage.clear_quarantine(table_name)
            quarantine.unpersist()
        quality_checks = self.quality_analyzer.analyze(
            curated,
            table_name,
            required_columns,
            unique_keys,
            quarantine_count=quarantine_count,
            details=details,
        )
        curated.unpersist()
        return {
            "table_name": table_name,
            "status": "published",
            "records_published": records_published,
            "records_quarantined": quarantine_count,
            "storage": storage_result,
            "quarantine": quarantine_result,
            "quality_checks": quality_checks,
        }

    def _read(self, relative_path: str) -> DataFrame:
        return self.spark.read.parquet(f"{self.bronze_path}/{relative_path}")

    def _add_silver_metadata(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("_silver_execution_id", F.lit(self.execution_id))
            .withColumn("_silver_ingestion_ts", F.current_timestamp())
        )

    def _existing_trace_columns(self, df: DataFrame):
        return [F.col(column) for column in TRACE_COLUMNS if column in df.columns]

    def _any_null(self, columns: List[str]):
        expressions = [F.col(column).isNull() for column in columns]
        result = expressions[0]
        for expression in expressions[1:]:
            result = result | expression
        return result

    def _any(self, expressions):
        if not expressions:
            return F.lit(False)
        result = expressions[0]
        for expression in expressions[1:]:
            result = result | expression
        return result

    def _code(self, column: str, length: int):
        value = F.trim(F.col(column).cast("string"))
        return F.when(value == "", None).otherwise(F.lpad(value, length, "0"))

    def _upper(self, column: str):
        return F.upper(F.trim(F.col(column)))

    def _parse_date(self, value):
        return F.coalesce(
            F.to_date(value),
            F.to_date(value, "d/M/yyyy HH:mm:ss"),
            F.to_date(value, "d/M/yyyy"),
        )

    def _int_or_zero(self, column: str):
        value = F.trim(F.col(column).cast("string"))
        return F.when(value.rlike(r"^-?\d+$"), value.cast("int")).otherwise(F.lit(0))

    def _is_selected(self, column: str):
        value = F.trim(F.col(column).cast("string"))
        return F.when(value.rlike(r"^-?\d+$"), value.cast("int") > 0).otherwise(F.lit(False))

    def _existing_raw_columns(self, df: DataFrame, columns: List[str]):
        return [F.col(column).cast("string").alias(f"{column}_raw") for column in columns if column in df.columns]

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

    def _is_lima_municipality(self):
        return F.upper(F.trim(F.coalesce(F.col("DEPARTAMENTO_NOMBRE").cast("string"), F.lit("")))) == F.lit("LIMA")
