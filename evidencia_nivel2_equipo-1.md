# Evidencia del Nivel 2 · equipo-1

1. **Encabezado.** Volumen ampliacion (datalake_equipo) · ventana 2024-01-09 a 2024-01-14 · Spark 3.5.3 · adaptativa apagada · 8 particiones de mezcla · metrica de bytes `bytes_columnas_leidas`.
2. **Hipótesis (con marca de tiempo).** {"elt_consumo": [1380000, "2026-11-12T22:28:36.648051Z"], "elt_presion": [400000, "2026-11-12T22:28:36.648058Z"], "reproceso": [6, "2026-11-12T22:28:36.648060Z"]}
3. **Métricas observadas.**

| producto | enfoque | archivos_leidos | bytes_leidos | bytes_escritos | filas_descartadas_visibles |
|---|---|---|---|---|---|
| ambos | ETL-una-pasada | 120 | 54371830 | 12996 | 0 |
| consumo | ELT | 120 | 1382744 | 275768 | 60209 |
| consumo | ETL | 120 | 54371830 | 6852 | 0 |
| consumo | ETL-parquet | 120 | 1382744 | 7007 | 0 |
| consumo | ETL-ventana | 6 | 2718973 | 8150 | 0 |
| presion | ELT | 120 | 401505 | 253897 | 60209 |
| presion | ETL | 120 | 54371830 | 6238 | 0 |
| presion | ETL-parquet | 120 | 2281385 | 6238 | 0 |

4. **Hipótesis frente a lo observado.** _(una frase por hipótesis)_
5. **Decisiones del nivel y la cifra que las cambiaría.** _(granularidad, invariantes, qué va por ETL)_
6. **Conclusión** que separa lo observado de lo interpretado. _(su texto)_
