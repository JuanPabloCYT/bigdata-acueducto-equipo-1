-- Producto consumo_dia_sector (equipo-1): consumo por dia y sector sobre la vista prep_lecturas.
-- Cuenta TODAS las lecturas de la preparacion y separa confiables de no confiables en lugar de descartarlas.
-- La regla de confiabilidad (quality_flag = 'OK') se aplica AQUI, al leer el producto: cambiarla es cambiar este
-- SQL y reprocesar el producto, sin reconstruir la preparacion. Las columnas de particion van al final.
SELECT
    fecha,
    sector,
    COUNT(*)                                            AS n_lecturas,
    SUM(CASE WHEN quality_flag = 'OK' THEN 1 ELSE 0 END)  AS n_confiables,
    SUM(CASE WHEN quality_flag = 'OK' THEN 0 ELSE 1 END)  AS n_no_confiables,
    SUM(consumo_m3)                                     AS consumo_total_m3,
    SUM(CASE WHEN quality_flag = 'OK' THEN consumo_m3 END) AS consumo_confiable_m3,
    anio,
    mes,
    dia
FROM prep_lecturas
GROUP BY fecha, sector, anio, mes, dia
