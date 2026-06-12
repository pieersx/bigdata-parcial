USE municipal_gold;

CREATE OR REPLACE VIEW vw_dashboard_01_recaudacion_capacidad AS
SELECT
    i.year,
    m.SEC_EJEC,
    m.UBIGEO,
    m.MUNICIPALIDAD_NOMBRE,
    m.DEPARTAMENTO_NOMBRE,
    m.PROVINCIA_NOMBRE,
    m.DISTRITO_NOMBRE,
    m.categoria_municipalidad,
    SUM(i.MONTO_RECAUDADO) AS recaudacion_total,
    SUM(i.MONTO_PIM) AS pim_total,
    MAX(r.personal_municipal_total) AS personal_municipal_total,
    CASE
        WHEN MAX(CASE WHEN r.requiere_asistencia_at THEN 1 ELSE 0 END) = 1 THEN 'SI'
        WHEN COUNT(r.SEC_EJEC) = 0 THEN NULL
        ELSE 'NO'
    END AS requiere_asistencia_at,
    CASE
        WHEN MAX(r.personal_municipal_total) IS NULL OR MAX(r.personal_municipal_total) = 0 THEN NULL
        ELSE SUM(i.MONTO_RECAUDADO) / MAX(r.personal_municipal_total)
    END AS recaudacion_por_personal
FROM fact_ingresos_mensuales i
JOIN dim_municipalidad_gold m ON i.SEC_EJEC = m.SEC_EJEC
LEFT JOIN fact_renamu_gestion_tributaria r ON i.SEC_EJEC = r.SEC_EJEC
GROUP BY
    i.year,
    m.SEC_EJEC,
    m.UBIGEO,
    m.MUNICIPALIDAD_NOMBRE,
    m.DEPARTAMENTO_NOMBRE,
    m.PROVINCIA_NOMBRE,
    m.DISTRITO_NOMBRE,
    m.categoria_municipalidad;

CREATE OR REPLACE VIEW vw_dashboard_02_clasificador_ingreso AS
SELECT
    f.year,
    f.MES_DOC AS month,
    m.SEC_EJEC,
    m.DEPARTAMENTO_NOMBRE,
    m.PROVINCIA_NOMBRE,
    m.DISTRITO_NOMBRE,
    m.categoria_municipalidad,
    c.clasificador_id,
    c.RUBRO_NOMBRE,
    c.GENERICA_NOMBRE,
    c.SUBGENERICA_NOMBRE,
    c.ESPECIFICA_NOMBRE,
    SUM(f.MONTO_PIA) AS pia,
    SUM(f.MONTO_PIM) AS pim,
    SUM(f.MONTO_RECAUDADO) AS recaudado
FROM fact_ingresos_clasificador f
JOIN dim_municipalidad_gold m ON f.SEC_EJEC = m.SEC_EJEC
LEFT JOIN dim_clasificador_ingreso c ON f.clasificador_id = c.clasificador_id
GROUP BY
    f.year,
    f.MES_DOC,
    m.SEC_EJEC,
    m.DEPARTAMENTO_NOMBRE,
    m.PROVINCIA_NOMBRE,
    m.DISTRITO_NOMBRE,
    m.categoria_municipalidad,
    c.clasificador_id,
    c.RUBRO_NOMBRE,
    c.GENERICA_NOMBRE,
    c.SUBGENERICA_NOMBRE,
    c.ESPECIFICA_NOMBRE;

CREATE OR REPLACE VIEW vw_dashboard_03_predial_vs_efectividad AS
SELECT
    p.year,
    p.MES_ESTADISTICA AS month,
    m.SEC_EJEC,
    m.DEPARTAMENTO_NOMBRE,
    m.PROVINCIA_NOMBRE,
    m.DISTRITO_NOMBRE,
    m.categoria_municipalidad,
    SUM(p.MON_RECAUDACION_TOTAL) AS recaudacion_predial_total,
    SUM(p.MON_EMISIONPREDIAL_AFECTO) AS emision_predial_total,
    CASE
        WHEN SUM(p.MON_EMISIONPREDIAL_AFECTO) = 0 THEN NULL
        ELSE SUM(p.MON_RECAUDACION_TOTAL) / SUM(p.MON_EMISIONPREDIAL_AFECTO) * 100
    END AS efectividad_predial_pct,
    MAX(e.ESTADO) AS estado_sismepre,
    MAX(e.CLASIFICACION) AS clasificacion_sismepre,
    MAX(e.TIPO_META) AS tipo_meta
FROM fact_predial_mensual p
JOIN dim_municipalidad_gold m ON p.SEC_EJEC = m.SEC_EJEC
LEFT JOIN fact_sismepre_cumplimiento e
    ON p.SEC_EJEC = e.SEC_EJEC
    AND p.year = e.year
GROUP BY
    p.year,
    p.MES_ESTADISTICA,
    m.SEC_EJEC,
    m.DEPARTAMENTO_NOMBRE,
    m.PROVINCIA_NOMBRE,
    m.DISTRITO_NOMBRE,
    m.categoria_municipalidad;

CREATE OR REPLACE VIEW vw_dashboard_04_distribucion_efectividad AS
WITH municipal_predial AS (
    SELECT
        p.year,
        m.SEC_EJEC,
        m.DEPARTAMENTO_NOMBRE,
        m.PROVINCIA_NOMBRE,
        m.DISTRITO_NOMBRE,
        m.categoria_municipalidad,
        SUM(p.MON_RECAUDACION_TOTAL) AS recaudacion_predial_total,
        SUM(p.MON_EMISIONPREDIAL_AFECTO) AS emision_predial_total
    FROM fact_predial_mensual p
    JOIN dim_municipalidad_gold m ON p.SEC_EJEC = m.SEC_EJEC
    GROUP BY
        p.year,
        m.SEC_EJEC,
        m.DEPARTAMENTO_NOMBRE,
        m.PROVINCIA_NOMBRE,
        m.DISTRITO_NOMBRE,
        m.categoria_municipalidad
)
SELECT
    year,
    SEC_EJEC,
    DEPARTAMENTO_NOMBRE,
    PROVINCIA_NOMBRE,
    DISTRITO_NOMBRE,
    categoria_municipalidad,
    recaudacion_predial_total,
    emision_predial_total,
    CASE
        WHEN emision_predial_total = 0 THEN NULL
        ELSE recaudacion_predial_total / emision_predial_total * 100
    END AS efectividad_predial_pct,
    NTILE(10) OVER (
        PARTITION BY year
        ORDER BY
            CASE
                WHEN emision_predial_total = 0 THEN NULL
                ELSE recaudacion_predial_total / emision_predial_total
            END
    ) AS decil_efectividad,
    RANK() OVER (
        PARTITION BY year, DEPARTAMENTO_NOMBRE
        ORDER BY
            CASE
                WHEN emision_predial_total = 0 THEN NULL
                ELSE recaudacion_predial_total / emision_predial_total
            END DESC
    ) AS ranking_departamental
FROM municipal_predial;

CREATE OR REPLACE VIEW vw_dashboard_05_software_tributario AS
SELECT
    s.year,
    m.SEC_EJEC,
    m.DEPARTAMENTO_NOMBRE,
    m.PROVINCIA_NOMBRE,
    m.DISTRITO_NOMBRE,
    m.categoria_municipalidad,
    s.usa_srtm_estado,
    s.usa_software_rentas_at,
    s.usa_software_catastro,
    s.usa_al_menos_un_software_at
FROM fact_renamu_software_at s
JOIN dim_municipalidad_gold m ON s.SEC_EJEC = m.SEC_EJEC;

CREATE OR REPLACE VIEW vw_dashboard_06_priorizacion_municipal AS
SELECT
    mart.year,
    mart.SEC_EJEC,
    mart.UBIGEO,
    mart.MUNICIPALIDAD_NOMBRE,
    mart.DEPARTAMENTO_NOMBRE,
    mart.PROVINCIA_NOMBRE,
    mart.DISTRITO_NOMBRE,
    mart.categoria_municipalidad,
    mart.MONTO_RECAUDADO AS recaudacion_total,
    mart.MON_RECAUDACION_TOTAL AS recaudacion_predial_total,
    mart.MON_BASEIMPONIBLE_AFECTO AS base_imponible_predial,
    mart.MON_SALDO_PREDIAL_TOTAL AS saldo_predial_total,
    mart.PCT_RECUPERACION_PREDIAL AS efectividad_predial_pct,
    CASE
        WHEN mart.usa_al_menos_un_software_at THEN 'SI'
        WHEN mart.usa_al_menos_un_software_at IS NULL THEN NULL
        ELSE 'NO'
    END AS usa_al_menos_un_software_at,
    mart.ESTADO_SISMEPRE,
    mart.CLASIFICACION_SISMEPRE,
    mart.TIPO_META_SISMEPRE,
    CASE
        WHEN mart.PCT_RECUPERACION_PREDIAL IS NULL THEN 'SIN INDICADOR'
        WHEN mart.PCT_RECUPERACION_PREDIAL < 50 AND COALESCE(mart.usa_al_menos_un_software_at, false) = false THEN 'ALTA'
        WHEN mart.PCT_RECUPERACION_PREDIAL < 70 THEN 'MEDIA'
        ELSE 'BAJA'
    END AS prioridad_intervencion
FROM mart_dashboard_municipal mart;
