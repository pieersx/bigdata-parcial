DICTIONARY_RULE = {
    "required_columns": ["VARIABLE", "TIPO_DATO", "DESCRIPCION"],
    "unique_keys": ["VARIABLE"],
    "validity_expressions": [
        {"name": "tipo_dato_informado", "expr": "TRIM(TIPO_DATO) <> ''"},
    ],
    "consistency_expressions": [],
    "integrity_pairs": [],
    "integrity_references": [],
    "actuality_expressions": [],
    "accuracy_expressions": [
        {"name": "descripcion_informada", "expr": "TRIM(DESCRIPCION) <> ''"},
    ],
}


DATASET_RULES = {
    "rentas_respuestas": {
        "required_columns": [
            "SEC_EJEC",
            "ANO_APLICACION",
            "PERIODO",
            "FORMULARIO_ID",
            "PREGUNTA_ID",
            "RESPUESTA_ID",
            "ESTADO_REGISTRO",
        ],
        "unique_keys": [
            "SEC_EJEC",
            "ANO_APLICACION",
            "PERIODO",
            "FORMULARIO_ID",
            "PREGUNTA_ID",
            "RESPUESTA_ID",
        ],
        "validity_expressions": [
            {"name": "estado_registro_dominio", "expr": "ESTADO_REGISTRO IN ('A', 'I')"},
            {"name": "year_matches_partition", "expr": "ANO_APLICACION = year"},
            {"name": "periodo_numerico", "expr": "PERIODO RLIKE '^[0-9]+$'"},
        ],
        "consistency_expressions": [
            {
                "name": "una_sola_respuesta_activa",
                "expr": "(CASE WHEN TRIM(RESPUESTA_TEXTO) <> '' THEN 1 ELSE 0 END + "
                "CASE WHEN TRIM(RESPUESTA_DECIMAL) <> '' AND TRIM(RESPUESTA_DECIMAL) <> '0' THEN 1 ELSE 0 END + "
                "CASE WHEN TRIM(RESPUESTA_ENTERO) <> '' AND TRIM(RESPUESTA_ENTERO) <> '0' THEN 1 ELSE 0 END + "
                "CASE WHEN TRIM(RESPUESTA_FECHA) <> '' THEN 1 ELSE 0 END) <= 1",
            },
        ],
        "integrity_pairs": [],
        "integrity_references": [
            {
                "reference_table": "rentas_preguntas",
                "join_columns": [
                    ("ANO_APLICACION", "ANO_APLICACION"),
                    ("PERIODO", "PERIODO"),
                    ("FORMULARIO_ID", "FORMULARIO_ID"),
                    ("PREGUNTA_ID", "PREGUNTA_ID"),
                ],
            },
            {
                "reference_table": "rentas_formulario",
                "join_columns": [
                    ("ANO_APLICACION", "ANO_APLICACION"),
                    ("PERIODO", "PERIODO"),
                    ("FORMULARIO_ID", "FORMULARIO_ID"),
                ],
            },
        ],
        "actuality_expressions": [
            {"name": "year_valido", "expr": "year RLIKE '^[0-9]{4}$' AND CAST(year AS INT) <= YEAR(CURRENT_DATE()) + 1"},
        ],
        "accuracy_expressions": [
            {"name": "respuesta_decimal_numerica", "expr": "TRIM(RESPUESTA_DECIMAL) = '' OR RESPUESTA_DECIMAL RLIKE '^-?[0-9]+(\\.[0-9]+)?$'"},
            {"name": "respuesta_entero_numerico", "expr": "TRIM(RESPUESTA_ENTERO) = '' OR RESPUESTA_ENTERO RLIKE '^-?[0-9]+$'"},
        ],
    },
    "rentas_esat_estadistica_atm": {
        "required_columns": [
            "SEC_EJEC",
            "ANO_APLICACION",
            "PERIODO",
            "DEPARTAMENTO_NOMBRE",
            "PROVINCIA_NOMBRE",
            "DISTRITO_NOMBRE",
            "MUNICIPALIDAD_NOMBRE",
        ],
        "unique_keys": ["SEC_EJEC", "ANO_APLICACION", "PERIODO", "ANO_ESTADISTICA"],
        "validity_expressions": [
            {"name": "year_matches_partition", "expr": "ANO_APLICACION = year"},
            {"name": "periodo_numerico", "expr": "PERIODO RLIKE '^[0-9]+$'"},
            {"name": "ano_estadistica_numerico", "expr": "ANO_ESTADISTICA RLIKE '^[0-9]{4}$'"},
        ],
        "consistency_expressions": [
            {"name": "predios_total_no_vacio", "expr": "TRIM(NUM_PREDIOTOTAL) <> ''"},
        ],
        "integrity_pairs": [
            ("DEPARTAMENTO", "DEPARTAMENTO_NOMBRE"),
            ("PROVINCIA", "PROVINCIA_NOMBRE"),
            ("DISTRITO", "DISTRITO_NOMBRE"),
        ],
        "integrity_references": [],
        "actuality_expressions": [
            {"name": "year_valido", "expr": "year RLIKE '^[0-9]{4}$' AND CAST(year AS INT) <= YEAR(CURRENT_DATE()) + 1"},
        ],
        "accuracy_expressions": [
            {"name": "recaudacion_actual_ordinaria_numerica", "expr": "MON_RECAUDACTUAL_ORDIN RLIKE '^-?[0-9]+(\\.[0-9]+)?$'"},
            {"name": "recaudacion_actual_coactiva_numerica", "expr": "MON_RECAUDACTUAL_COAC RLIKE '^-?[0-9]+(\\.[0-9]+)?$'"},
        ],
    },
    "rentas_preguntas": {
        "required_columns": [
            "ANO_APLICACION",
            "PERIODO",
            "FORMULARIO_ID",
            "PREGUNTA_ID",
            "DESCRIPCION",
            "ESTADO_REGISTRO",
        ],
        "unique_keys": ["ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID"],
        "validity_expressions": [
            {"name": "estado_registro_dominio", "expr": "ESTADO_REGISTRO IN ('A', 'I')"},
            {"name": "year_matches_partition", "expr": "ANO_APLICACION = year"},
        ],
        "consistency_expressions": [
            {"name": "descripcion_informada", "expr": "TRIM(DESCRIPCION) <> ''"},
        ],
        "integrity_pairs": [],
        "integrity_references": [],
        "actuality_expressions": [
            {"name": "year_valido", "expr": "year RLIKE '^[0-9]{4}$'"},
        ],
        "accuracy_expressions": [
            {"name": "formulario_id_numerico", "expr": "FORMULARIO_ID RLIKE '^[0-9]+$'"},
        ],
    },
    "rentas_formulario": {
        "required_columns": ["ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "TITULO", "TIPO_FORMULARIO"],
        "unique_keys": ["ANO_APLICACION", "PERIODO", "FORMULARIO_ID"],
        "validity_expressions": [
            {"name": "tipo_formulario_dominio", "expr": "TIPO_FORMULARIO IN ('C', 'E')"},
            {"name": "year_matches_partition", "expr": "ANO_APLICACION = year"},
        ],
        "consistency_expressions": [
            {"name": "titulo_informado", "expr": "TRIM(TITULO) <> ''"},
        ],
        "integrity_pairs": [],
        "integrity_references": [],
        "actuality_expressions": [
            {"name": "year_valido", "expr": "year RLIKE '^[0-9]{4}$'"},
        ],
        "accuracy_expressions": [
            {"name": "orden_formulario_numerico", "expr": "ORDEN_FORMULARIO RLIKE '^[0-9]+$'"},
        ],
    },
    "rentas_estadistica": {
        "required_columns": ["ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "ANO_ESTADISTICA", "MES_ESTADISTICA"],
        "unique_keys": ["ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "ANO_ESTADISTICA", "MES_ESTADISTICA"],
        "validity_expressions": [
            {"name": "year_matches_partition", "expr": "ANO_APLICACION = year"},
            {"name": "mes_valido", "expr": "MES_ESTADISTICA RLIKE '^[0-9]{1,2}$' AND CAST(MES_ESTADISTICA AS INT) BETWEEN 1 AND 13"},
        ],
        "consistency_expressions": [],
        "integrity_pairs": [],
        "integrity_references": [],
        "actuality_expressions": [
            {"name": "year_valido", "expr": "year RLIKE '^[0-9]{4}$'"},
        ],
        "accuracy_expressions": [
            {"name": "ano_estadistica_numerico", "expr": "ANO_ESTADISTICA RLIKE '^[0-9]{4}$'"},
        ],
    },
    "rentas_ano_aplicacion": {
        "required_columns": ["ANO_APLICACION", "PERIODO", "ESTADO"],
        "unique_keys": ["ANO_APLICACION", "PERIODO", "ANO_APLICACION_FIN"],
        "validity_expressions": [
            {"name": "estado_dominio", "expr": "ESTADO IN ('A', 'I')"},
            {"name": "year_matches_partition", "expr": "ANO_APLICACION = year"},
        ],
        "consistency_expressions": [
            {"name": "anio_inicio_menor_igual_fin", "expr": "CAST(ANO_APLICACION_INICIO AS INT) <= CAST(ANO_APLICACION_FIN AS INT)"},
        ],
        "integrity_pairs": [],
        "integrity_references": [],
        "actuality_expressions": [
            {"name": "year_valido", "expr": "year RLIKE '^[0-9]{4}$'"},
        ],
        "accuracy_expressions": [
            {"name": "periodo_numerico", "expr": "PERIODO RLIKE '^[0-9]+$'"},
        ],
    },
    "rentas_entidad_estado": {
        "required_columns": ["SEC_EJEC", "ANO_APLICACION", "PERIODO", "ESTADO", "CLASIFICACION"],
        "unique_keys": ["SEC_EJEC", "ANO_APLICACION", "PERIODO"],
        "validity_expressions": [
            {"name": "estado_dominio", "expr": "ESTADO IN ('A', 'I')"},
            {"name": "year_matches_partition", "expr": "ANO_APLICACION = year"},
        ],
        "consistency_expressions": [
            {"name": "sec_ejec_presente", "expr": "TRIM(SEC_EJEC) <> '' OR TRIM(SEC_EJE) <> ''"},
        ],
        "integrity_pairs": [],
        "integrity_references": [],
        "actuality_expressions": [
            {"name": "year_valido", "expr": "year RLIKE '^[0-9]{4}$'"},
        ],
        "accuracy_expressions": [
            {"name": "periodo_numerico", "expr": "PERIODO RLIKE '^[0-9]+$'"},
        ],
    },
    "rentas_preguntas_diccionario": DICTIONARY_RULE,
    "rentas_estadistica_diccionario": DICTIONARY_RULE,
    "rentas_formulario_diccionario": DICTIONARY_RULE,
    "rentas_respuestas_diccionario": DICTIONARY_RULE,
    "rentas_ano_aplicacion_diccionario": DICTIONARY_RULE,
    "rentas_esat_estadistica_atm_diccionario": DICTIONARY_RULE,
    "rentas_entidad_estado_diccionario": DICTIONARY_RULE,
}
