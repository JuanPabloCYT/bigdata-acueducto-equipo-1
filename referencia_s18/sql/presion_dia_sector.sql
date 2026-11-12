-- Producto de presion del tablero de operaciones: lecturas por dia, sector y nivel de presion.
--
-- Usa la misma convencion que el producto de consumo: cuenta TODAS las lecturas y separa las
-- confiables de las no confiables, en lugar de descartar. No suma consumo, de modo que solo necesita
-- leer tres columnas de la preparacion (sector, quality_flag y nivel_presion) mas la fecha.
-- Si una lectura no trae presion, el nivel queda como 'sin_dato' para que el grano no tenga nulos.
SELECT
    fecha,
    sector,
    COALESCE(nivel_presion, 'sin_dato') AS nivel_presion,
    COUNT(*) AS n_lecturas,
    SUM(CASE WHEN quality_flag = 'OK' THEN 1 ELSE 0 END) AS n_confiables,
    SUM(CASE WHEN quality_flag = 'OK' THEN 0 ELSE 1 END) AS n_no_confiables,
    anio,
    mes,
    dia
FROM prep_lecturas
GROUP BY anio, mes, dia, fecha, sector, COALESCE(nivel_presion, 'sin_dato')
