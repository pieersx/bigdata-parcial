# Resultados Laboratorio Hive Municipal

## 01_lectura_parquet

- Filas resultado: `1`
- Parquet: `/home/jovyan/work/data/hive/results/01_lectura_parquet`

```text
 filas
325090
```

## 02_agregacion_recaudacion

- Filas resultado: `15`
- Parquet: `/home/jovyan/work/data/hive/results/02_agregacion_recaudacion`

```text
 year            pia            pim      recaudado
 2012 14826201287.00 27217404109.00 26288353011.25
 2013 17559535020.00 28965553175.00 26579856350.37
 2014 16352951558.00 27088593492.00 25872639528.01
 2015 15230682622.00 24004510109.00 23399762001.90
 2016 14269909830.00 26468273662.00 25979054166.49
```

## 03_filtrado_ordenamiento

- Filas resultado: `20`
- Parquet: `/home/jovyan/work/data/hive/results/03_filtrado_ordenamiento`

```text
                DEPARTAMENTO_NOMBRE PROVINCIA_NOMBRE DISTRITO_NOMBRE     recaudado
                               LIMA             LIMA            LIMA 4320250573.04
                             ANCASH            HUARI      SAN MARCOS 1082599468.87
                              CUSCO    LA CONVENCION       MEGANTONI  566694551.54
PROVINCIA CONSTITUCIONAL DEL CALLAO     PROV. CALLAO          CALLAO  465247944.10
                             ANCASH            SANTA        CHIMBOTE  450150732.93
```

## 04_datos_faltantes

- Filas resultado: `8`
- Parquet: `/home/jovyan/work/data/hive/results/04_datos_faltantes`

```text
categoria  municipalidades
        A               59
        B               90
        C               31
        D               93
        E              299
```

## 05_window_ranking

- Filas resultado: `125`
- Parquet: `/home/jovyan/work/data/hive/results/05_window_ranking`

```text
DEPARTAMENTO_NOMBRE DISTRITO_NOMBRE   recaudado  ranking_departamento
           AMAZONAS    BAGUA GRANDE 40513004.64                     1
           AMAZONAS           NIEVA 38204678.62                     2
           AMAZONAS           BAGUA 26299293.85                     3
           AMAZONAS     CHACHAPOYAS 25067221.28                     4
           AMAZONAS           IMAZA 18159374.80                     5
```

## 06_cte_join_analytics

- Filas resultado: `2437`
- Parquet: `/home/jovyan/work/data/hive/results/06_cte_join_analytics`

```text
DEPARTAMENTO_NOMBRE categoria_municipalidad  year recaudado_siaf recaudado_predial efectividad_predial_pct
               LIMA                    None  2026  4902717242.08       62103860.00                    None
               LIMA                       C  2026  2098500575.77       84682966.14               45.486700
           AREQUIPA                       D  2026   751809994.11              None                    None
              CUSCO                       G  2026   509309072.42           5977.62                    None
             ANCASH                    None  2026   497765233.97              0.00                    None
```
