-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- Equipo VSEC SERVICIOS CORPORATIVOS
-----------------------------------------------------------------------------
-- Fecha Creación: 20260320
-- Última Fecha Modificación: 20260320
-- Autores: daetobo
-- Últimos Autores: daetobo
-- Descripción: Depuración de las tablas temporales asociadas al paso
--              o a toda la rutina según aplique.
-----------------------------------------------------------------------------
---------------------------------- INSUMOS ----------------------------------
-- resultados.reporte_flujos_oozie
--------------------------------- RESULTADOS --------------------------------
-- proceso.temporal_ads_package_gen
-----------------------------------------------------------------------------
-------------------------------- Query Start --------------------------------

-- ESTO ES SOLO UNA CONSULTA DE MUESTRA, SE DEBEN COLOCAR TODAS LAS CONSULTAS
-- Y PARÁMETROS ASOCIADOS A LA RUTINA
DROP TABLE IF EXISTS {zona_procesamiento}.{prefijo}temporal_ads_package_gen PURGE;
CREATE TABLE IF NOT EXISTS {zona_procesamiento}.{prefijo}temporal_ads_package_gen STORED AS PARQUET TBLPROPERTIES ('transactional'='false') AS
    SELECT
        anio_finalizacion
        , mes_finalizacion
        , dia_finalizacion
        , nombre_flujo
    FROM
        {in_oozie}
    WHERE
        anio_finalizacion = {f_corte_y} AND
        mes_finalizacion  = {f_corte_m} AND
        dia_finalizacion  = {f_corte_d}
    LIMIT
        10;
COMPUTE STATS {zona_procesamiento}.{prefijo}temporal_ads_package_gen;
--------------------------------- Query End ---------------------------------