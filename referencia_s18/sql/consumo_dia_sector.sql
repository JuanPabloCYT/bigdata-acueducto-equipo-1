-- Producto de consumo del tablero financiero: una fila por dia y sector.
--
-- Lee la vista prep_lecturas (la capa de preparacion refinada_spark/, ya limitada a la ventana de
-- dias por las columnas de la ruta). La preparacion CONSERVA todas las lecturas con su bandera de
-- calidad; la regla de confiabilidad se aplica AQUI, al construir el producto, y no antes:
--   n_lecturas            todas las lecturas del dia y sector (sin filtro)
--   n_confiables          las de quality_flag = 'OK'
--   n_no_confiables       el resto: queda contado, no desaparece
--   consumo_total_m3      suma de consumo de todas las lecturas
--   consumo_confiable_m3  suma de consumo de las confiables (lo que el trabajo heredado entrega)
-- anio, mes y dia van al final porque son las columnas de particion de la capa de salida.
SELECT
    fecha,
    sector,
    COUNT(*) AS n_lecturas,
    SUM(CASE WHEN quality_flag = 'OK' THEN 1 ELSE 0 END) AS n_confiables,
    SUM(CASE WHEN quality_flag = 'OK' THEN 0 ELSE 1 END) AS n_no_confiables,
    SUM(consumo_m3) AS consumo_total_m3,
    SUM(CASE WHEN quality_flag = 'OK' THEN consumo_m3 END) AS consumo_confiable_m3,
    anio,
    mes,
    dia
FROM prep_lecturas
GROUP BY anio, mes, dia, fecha, sector
