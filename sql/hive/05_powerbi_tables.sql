USE municipal_gold;

DROP TABLE IF EXISTS pbi_dashboard_01;
CREATE TABLE pbi_dashboard_01
STORED AS PARQUET AS
SELECT
    CAST(year AS INT) AS year,
    CAST(sec_ejec AS STRING) AS sec_ejec,
    CAST(ubigeo AS STRING) AS ubigeo,
    CAST(municipalidad_nombre AS STRING) AS municipalidad_nombre,
    CAST(departamento_nombre AS STRING) AS departamento_nombre,
    CAST(provincia_nombre AS STRING) AS provincia_nombre,
    CAST(distrito_nombre AS STRING) AS distrito_nombre,
    CAST(categoria_municipalidad AS STRING) AS categoria_municipalidad,
    CAST(recaudacion_total AS DOUBLE) AS recaudacion_total,
    CAST(pim_total AS DOUBLE) AS pim_total,
    CAST(personal_municipal_total AS INT) AS personal_municipal_total,
    CAST(requiere_asistencia_at AS STRING) AS requiere_asistencia_at,
    CAST(recaudacion_por_personal AS DOUBLE) AS recaudacion_por_personal
FROM vw_dashboard_01_recaudacion_capacidad;

DROP TABLE IF EXISTS pbi_dashboard_02;
CREATE TABLE pbi_dashboard_02
STORED AS PARQUET AS
SELECT
    CAST(year AS INT) AS year,
    CAST(month AS INT) AS month,
    CAST(sec_ejec AS STRING) AS sec_ejec,
    CAST(departamento_nombre AS STRING) AS departamento_nombre,
    CAST(provincia_nombre AS STRING) AS provincia_nombre,
    CAST(distrito_nombre AS STRING) AS distrito_nombre,
    CAST(categoria_municipalidad AS STRING) AS categoria_municipalidad,
    CAST(clasificador_id AS STRING) AS clasificador_id,
    CAST(rubro_nombre AS STRING) AS rubro_nombre,
    CAST(generica_nombre AS STRING) AS generica_nombre,
    CAST(subgenerica_nombre AS STRING) AS subgenerica_nombre,
    CAST(especifica_nombre AS STRING) AS especifica_nombre,
    CAST(pia AS DOUBLE) AS pia,
    CAST(pim AS DOUBLE) AS pim,
    CAST(recaudado AS DOUBLE) AS recaudado
FROM vw_dashboard_02_clasificador_ingreso;

DROP TABLE IF EXISTS pbi_dashboard_03;
CREATE TABLE pbi_dashboard_03
STORED AS PARQUET AS
SELECT
    CAST(year AS INT) AS year,
    CAST(month AS INT) AS month,
    CAST(sec_ejec AS STRING) AS sec_ejec,
    CAST(departamento_nombre AS STRING) AS departamento_nombre,
    CAST(provincia_nombre AS STRING) AS provincia_nombre,
    CAST(distrito_nombre AS STRING) AS distrito_nombre,
    CAST(categoria_municipalidad AS STRING) AS categoria_municipalidad,
    CAST(recaudacion_predial_total AS DOUBLE) AS recaudacion_predial_total,
    CAST(emision_predial_total AS DOUBLE) AS emision_predial_total,
    CAST(efectividad_predial_pct AS DOUBLE) AS efectividad_predial_pct,
    CAST(estado_sismepre AS STRING) AS estado_sismepre,
    CAST(clasificacion_sismepre AS STRING) AS clasificacion_sismepre,
    CAST(tipo_meta AS STRING) AS tipo_meta
FROM vw_dashboard_03_predial_vs_efectividad;

DROP TABLE IF EXISTS pbi_dashboard_04;
CREATE TABLE pbi_dashboard_04
STORED AS PARQUET AS
SELECT
    CAST(year AS INT) AS year,
    CAST(sec_ejec AS STRING) AS sec_ejec,
    CAST(departamento_nombre AS STRING) AS departamento_nombre,
    CAST(provincia_nombre AS STRING) AS provincia_nombre,
    CAST(distrito_nombre AS STRING) AS distrito_nombre,
    CAST(categoria_municipalidad AS STRING) AS categoria_municipalidad,
    CAST(recaudacion_predial_total AS DOUBLE) AS recaudacion_predial_total,
    CAST(emision_predial_total AS DOUBLE) AS emision_predial_total,
    CAST(efectividad_predial_pct AS DOUBLE) AS efectividad_predial_pct,
    CAST(decil_efectividad AS INT) AS decil_efectividad,
    CAST(ranking_departamental AS INT) AS ranking_departamental
FROM vw_dashboard_04_distribucion_efectividad;

DROP TABLE IF EXISTS pbi_dashboard_05;
CREATE TABLE pbi_dashboard_05
STORED AS PARQUET AS
SELECT
    CAST(year AS INT) AS year,
    CAST(sec_ejec AS STRING) AS sec_ejec,
    CAST(departamento_nombre AS STRING) AS departamento_nombre,
    CAST(provincia_nombre AS STRING) AS provincia_nombre,
    CAST(distrito_nombre AS STRING) AS distrito_nombre,
    CAST(categoria_municipalidad AS STRING) AS categoria_municipalidad,
    CAST(usa_srtm_estado AS STRING) AS usa_srtm_estado,
    CAST(usa_software_rentas_at AS STRING) AS usa_software_rentas_at,
    CAST(usa_software_catastro AS STRING) AS usa_software_catastro,
    CAST(usa_al_menos_un_software_at AS STRING) AS usa_al_menos_un_software_at
FROM vw_dashboard_05_software_tributario;

DROP TABLE IF EXISTS pbi_dashboard_06;
CREATE TABLE pbi_dashboard_06
STORED AS PARQUET AS
SELECT
    CAST(year AS INT) AS year,
    CAST(sec_ejec AS STRING) AS sec_ejec,
    CAST(ubigeo AS STRING) AS ubigeo,
    CAST(municipalidad_nombre AS STRING) AS municipalidad_nombre,
    CAST(departamento_nombre AS STRING) AS departamento_nombre,
    CAST(provincia_nombre AS STRING) AS provincia_nombre,
    CAST(distrito_nombre AS STRING) AS distrito_nombre,
    CAST(categoria_municipalidad AS STRING) AS categoria_municipalidad,
    CAST(recaudacion_total AS DOUBLE) AS recaudacion_total,
    CAST(recaudacion_predial_total AS DOUBLE) AS recaudacion_predial_total,
    CAST(base_imponible_predial AS DOUBLE) AS base_imponible_predial,
    CAST(saldo_predial_total AS DOUBLE) AS saldo_predial_total,
    CAST(efectividad_predial_pct AS DOUBLE) AS efectividad_predial_pct,
    CAST(usa_al_menos_un_software_at AS STRING) AS usa_al_menos_un_software_at,
    CAST(estado_sismepre AS STRING) AS estado_sismepre,
    CAST(clasificacion_sismepre AS STRING) AS clasificacion_sismepre,
    CAST(tipo_meta_sismepre AS STRING) AS tipo_meta_sismepre,
    CAST(prioridad_intervencion AS STRING) AS prioridad_intervencion
FROM vw_dashboard_06_priorizacion_municipal;
