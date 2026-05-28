DATASET_RULES = {
    "renamu": {
        "required_columns": [
            "Año",
            "idmunici",
            "ccdd",
            "ccpp",
            "ccdi",
            "Ubigeo",
            "Departamento",
            "Provincia",
            "Distrito",
            "Tipomuni",
        ],
        "unique_keys": ["idmunici"],
        "validity_expressions": [
            {"name": "year_matches_partition", "expr": "`Año` = year"},
            {"name": "tipomuni_dominio", "expr": "Tipomuni IN ('1', '2')"},
            {"name": "ubigeo_formato", "expr": "Ubigeo RLIKE '^[0-9]{6}$'"},
        ],
        "consistency_expressions": [
            {"name": "ubigeo_consistente", "expr": "Ubigeo = concat(ccdd, ccpp, ccdi)"},
        ],
        "integrity_pairs": [
            ("ccdd", "Departamento"),
            ("ccpp", "Provincia"),
            ("ccdi", "Distrito"),
        ],
        "integrity_references": [],
        "actuality_expressions": [
            {"name": "year_renamu_2022", "expr": "year = '2022'"},
        ],
        "accuracy_expressions": [
            {"name": "idmunici_formato", "expr": "idmunici RLIKE '^[0-9]{6}$'"},
            {"name": "ccdd_formato", "expr": "ccdd RLIKE '^[0-9]{2}$'"},
            {"name": "ccpp_formato", "expr": "ccpp RLIKE '^[0-9]{2}$'"},
            {"name": "ccdi_formato", "expr": "ccdi RLIKE '^[0-9]{2}$'"},
        ],
    },
}
