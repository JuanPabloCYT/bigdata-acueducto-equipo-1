-- Producto presion_dia_sector (equipo-1): lecturas por dia, sector y nivel de presion sobre prep_lecturas.
-- Usa la clase nivel_presion que ya calculo la preparacion (una sola copia de los umbrales 2,5 y 3,5 bar);
-- una presion nula queda como 'sin_dato', la misma etiqueta del tablero heredado. Cuenta todas las lecturas
-- y separa confiables (quality_flag = 'OK') de no confiables. Lee cuatro columnas de la preparacion (mas las de la ruta).
SELECT
    fecha,
    sector,
    COALESCE(nivel_presion, 'sin_dato')                 AS nivel_presion,
    COUNT(*)                                            AS n_lecturas,
    SUM(CASE WHEN quality_flag = 'OK' THEN 1 ELSE 0 END)  AS n_confiables,
    SUM(CASE WHEN quality_flag = 'OK' THEN 0 ELSE 1 END)  AS n_no_confiables,
    anio,
    mes,
    dia
FROM prep_lecturas
GROUP BY fecha, sector, COALESCE(nivel_presion, 'sin_dato'), anio, mes, dia
