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
    "ingresos": {
        "required_columns": [
            "ANO_DOC",
            "MES_DOC",
            "NIVEL_GOBIERNO",
            "NIVEL_GOBIERNO_NOMBRE",
            "SEC_EJEC",
            "EJECUTORA",
            "EJECUTORA_NOMBRE",
            "MONTO_RECAUDADO",
        ],
        "unique_keys": [
            "ANO_DOC",
            "MES_DOC",
            "SEC_EJEC",
            "EJECUTORA",
            "FUENTE_FINANCIAMIENTO",
            "RUBRO",
            "TIPO_RECURSO",
            "GENERICA",
            "SUBGENERICA",
            "SUBGENERICA_DET",
            "ESPECIFICA",
            "ESPECIFICA_DET",
        ],
        "validity_expressions": [
            {"name": "mes_doc_rango", "expr": "MES_DOC RLIKE '^[0-9]{1,2}$' AND CAST(MES_DOC AS INT) BETWEEN 1 AND 12"},
            {"name": "nivel_gobierno_dominio", "expr": "NIVEL_GOBIERNO IN ('E', 'R', 'M')"},
            {"name": "monto_pia_numerico", "expr": "MONTO_PIA RLIKE '^-?[0-9]+(\\.[0-9]+)?$'"},
            {"name": "monto_pim_numerico", "expr": "MONTO_PIM RLIKE '^-?[0-9]+(\\.[0-9]+)?$'"},
            {"name": "monto_recaudado_numerico", "expr": "MONTO_RECAUDADO RLIKE '^-?[0-9]+(\\.[0-9]+)?$'"},
        ],
        "consistency_expressions": [
            {"name": "year_matches_partition", "expr": "ANO_DOC = year"},
            {
                "name": "nivel_nombre_match",
                "expr": "CASE WHEN NIVEL_GOBIERNO = 'E' THEN NIVEL_GOBIERNO_NOMBRE = 'GOBIERNO NACIONAL' "
                "WHEN NIVEL_GOBIERNO = 'R' THEN NIVEL_GOBIERNO_NOMBRE = 'GOBIERNOS REGIONALES' "
                "WHEN NIVEL_GOBIERNO = 'M' THEN NIVEL_GOBIERNO_NOMBRE = 'GOBIERNOS LOCALES' "
                "ELSE false END",
            },
        ],
        "integrity_pairs": [
            ("DEPARTAMENTO_EJECUTORA", "DEPARTAMENTO_EJECUTORA_NOMBRE"),
            ("PROVINCIA_EJECUTORA", "PROVINCIA_EJECUTORA_NOMBRE"),
            ("DISTRITO_EJECUTORA", "DISTRITO_EJECUTORA_NOMBRE"),
        ],
        "integrity_references": [],
        "actuality_expressions": [
            {"name": "year_rango_esperado", "expr": "year RLIKE '^[0-9]{4}$' AND CAST(year AS INT) BETWEEN 2012 AND YEAR(CURRENT_DATE()) + 1"},
        ],
        "accuracy_expressions": [
            {"name": "pia_no_negativo", "expr": "CAST(MONTO_PIA AS DOUBLE) >= 0"},
            {"name": "pim_no_negativo", "expr": "CAST(MONTO_PIM AS DOUBLE) >= 0"},
            {"name": "recaudado_no_negativo", "expr": "CAST(MONTO_RECAUDADO AS DOUBLE) >= 0"},
        ],
    },
    "ingresos_diccionario": DICTIONARY_RULE,
}
