DATASET_RULES = {
    "categorias_municipalidades": {
        "required_columns": ["Municipalidad", "Categoria"],
        "unique_keys": ["Municipalidad"],
        "validity_expressions": [
            {"name": "categoria_dominio", "expr": "Categoria IN ('A', 'B', 'C', 'D', 'E', 'F', 'G')"},
        ],
        "consistency_expressions": [
            {"name": "municipalidad_informada", "expr": "TRIM(Municipalidad) <> ''"},
        ],
        "integrity_pairs": [],
        "integrity_references": [],
        "actuality_expressions": [],
        "accuracy_expressions": [
            {"name": "categoria_informada", "expr": "TRIM(Categoria) <> ''"},
        ],
    },
}
