USE municipal_gold;

-- 01 Lectura de Parquet externo registrado en Hive.
SELECT COUNT(*) AS filas
FROM fact_ingresos_mensuales;

-- 02 Transformaciones y agregaciones.
SELECT
    year,
    SUM(MONTO_PIA) AS pia,
    SUM(MONTO_PIM) AS pim,
    SUM(MONTO_RECAUDADO) AS recaudado
FROM fact_ingresos_mensuales
GROUP BY year
ORDER BY year;

-- 03 Filtrado y ordenamiento.
SELECT
    m.DEPARTAMENTO_NOMBRE,
    m.PROVINCIA_NOMBRE,
    m.DISTRITO_NOMBRE,
    SUM(i.MONTO_RECAUDADO) AS recaudado
FROM fact_ingresos_mensuales i
JOIN dim_municipalidad_gold m ON i.SEC_EJEC = m.SEC_EJEC
WHERE i.year = 2024
GROUP BY m.DEPARTAMENTO_NOMBRE, m.PROVINCIA_NOMBRE, m.DISTRITO_NOMBRE
ORDER BY recaudado DESC
LIMIT 20;

-- 04 Manejo de datos faltantes.
SELECT
    COALESCE(categoria_municipalidad, 'SIN CATEGORIA') AS categoria,
    COUNT(*) AS municipalidades
FROM dim_municipalidad_gold
GROUP BY COALESCE(categoria_municipalidad, 'SIN CATEGORIA')
ORDER BY categoria;

-- 05 Window functions.
WITH ranking AS (
    SELECT
        m.DEPARTAMENTO_NOMBRE,
        m.DISTRITO_NOMBRE,
        SUM(i.MONTO_RECAUDADO) AS recaudado,
        RANK() OVER (
            PARTITION BY m.DEPARTAMENTO_NOMBRE
            ORDER BY SUM(i.MONTO_RECAUDADO) DESC
        ) AS ranking_departamento
    FROM fact_ingresos_mensuales i
    JOIN dim_municipalidad_gold m ON i.SEC_EJEC = m.SEC_EJEC
    WHERE i.year = 2024
    GROUP BY m.DEPARTAMENTO_NOMBRE, m.DISTRITO_NOMBRE
)
SELECT *
FROM ranking
WHERE ranking_departamento <= 5
ORDER BY DEPARTAMENTO_NOMBRE, ranking_departamento;

-- 06 Consulta compleja con CTEs y joins.
WITH ingresos AS (
    SELECT SEC_EJEC, year, SUM(MONTO_RECAUDADO) AS recaudado
    FROM fact_ingresos_mensuales
    GROUP BY SEC_EJEC, year
),
predial AS (
    SELECT
        SEC_EJEC,
        year,
        SUM(MON_RECAUDACION_TOTAL) AS reca_predial,
        SUM(MON_EMISIONPREDIAL_AFECTO) AS emision_predial
    FROM fact_predial_mensual
    GROUP BY SEC_EJEC, year
)
SELECT
    m.DEPARTAMENTO_NOMBRE,
    m.categoria_municipalidad,
    i.year,
    SUM(i.recaudado) AS recaudado_siaf,
    SUM(p.reca_predial) AS recaudado_predial,
    CASE
        WHEN SUM(p.emision_predial) = 0 THEN NULL
        ELSE SUM(p.reca_predial) / SUM(p.emision_predial) * 100
    END AS efectividad_predial_pct
FROM ingresos i
JOIN dim_municipalidad_gold m ON i.SEC_EJEC = m.SEC_EJEC
LEFT JOIN predial p ON i.SEC_EJEC = p.SEC_EJEC AND i.year = p.year
GROUP BY m.DEPARTAMENTO_NOMBRE, m.categoria_municipalidad, i.year
ORDER BY i.year DESC, recaudado_siaf DESC;
