# Evidencia del Nivel 2 · equipo-1

## Perfil de la ampliacion

- Comando: `python practica/generar_ampliacion_s15.py --equipo equipo-1` (3.5 s)
- Medidores: 348 · lecturas (total, no incremento): 1002240
- Sector dominante: S-SUR · fraccion de medidores: 0.3448 · disco: 56.0 MiB
- Reparto de la cruda: 5 particiones, archivos por particion [29, 29, 29, 29, 4], filas por particion [242208, 242208, 242208, 242208, 33408], lotes de Arrow esperados 104

## Capa reejecutable y reconciliacion (dos corridas por volumen)

| volumen | corrida | filas | archivos | hash | no_conformes | heredada | control | mib_leidos | tiempo_s |
|---|---|---|---|---|---|---|---|---|---|
| base | 1 | 576000 | 120 | 9cc455c0aff8 | 0 | OK | OK | 56.48 | 2.986 |
| base | 2 | 576000 | 120 | 9cc455c0aff8 | 0 | OK | OK | 56.48 | 2.93 |
| ampliacion | 1 | 1002240 | 120 | 7b0ac06a1ab1 | 0 | OK | OK | 103.71 | 3.717 |
| ampliacion | 2 | 1002240 | 120 | 7b0ac06a1ab1 | 0 | OK | OK | 103.71 | 3.657 |

## Lector independiente (pyarrow y pandas, sin Spark)

| volumen | grupos | solo_en_un_lado | lecturas_distintas | diferencia_max_consumo |
|---|---|---|---|---|
| base | 480 | 0 | 0 | 0.0 |
| ampliacion | 480 | 0 | 0 | 5.684341886080802e-14 |

Marca temporal de la ampliacion: {'fisico': 'INT64', 'logico': 'isAdjustedToUTC=true, unidad=microseconds', 'tipo_arrow': 'timestamp[us, tz=UTC]'}

## Reproceso de 6 dias sobre una copia (base)

| sobrescritura | filas_que_quedan | carpetas | intactas | reescritas | perdidas | hash | reconciliacion_heredada |
|---|---|---|---|---|---|---|---|
| estatica | 28800 | 6 | 0 | 6 | 114 | e635456bc71a | FALLA |
| dinamica | 576000 | 120 | 114 | 6 | 0 | 9cc455c0aff8 | OK |

Con mes y dia como entero: carpetas de mes ['mes=01', 'mes=02', 'mes=03', 'mes=04', 'mes=1'], 604800 filas al releer (28800 duplicadas).

- **Conclusion:** OBSERVADO: al reprocesar 6 dias (2024-01-01 a 2024-01-06) sobre una copia de la capa de la base, la sobrescritura estatica dejo 28 800 filas (6 x 4 800) y 6 carpetas: perdio 114 de 120 particiones y la reconciliacion con la heredada FALLA. La dinamica dejo 576 000 filas, 120 carpetas (114 intactas, 6 reescritas, 0 perdidas) y el mismo hash 9cc455c0aff8 que la capa original. Con mes y dia como entero aparecen mes=1 junto a mes=01 y al releer hay 604 800 filas (28 800 duplicadas). INTERPRETADO: la estatica interpreta 'overwrite' como 'reemplazar toda la tabla'; la dinamica, como 'reemplazar solo las particiones que traigo'. Adoptamos la dinamica con rutas de texto con ceros.

## Variantes de clasificacion

- **Hipotesis sobre el CSV (escrita antes de medir):** Esperamos que la nativa sea la mas rapida, pero con poca diferencia: cada corrida relee y convierte 120 CSV, y ese costo (varios segundos) es comun a las tres variantes. La vectorizada deberia quedar con un rango solapado con la nativa (un lote de Arrow por cada 10 000 filas). La funcion por fila deberia ser la mas lenta, unas 1,5 a 2 veces la nativa, porque serializa ~1 millon de filas hacia Python, una por una, y su rango no deberia solaparse con el de la nativa.
- **Hipotesis con la columna en memoria:** Con la columna de presion ya en memoria desaparece el costo de leer el CSV, asi que lo que queda es el costo de la funcion: esperamos que las diferencias se amplifiquen. La nativa (codigo generado en la JVM) seria la mas rapida; la vectorizada, unas 2 a 4 veces mas lenta por el viaje a Python en lotes; y la funcion por fila, 5 a 10 veces la nativa, sin solape entre ninguno de los tres rangos.

### Sobre la cruda en CSV (ampliacion, 12 corridas: 1 calentamiento descartado y 3 medidas por variante)

| variante | mediana_s | rango_s | razon_vs_nativa | rango_se_solapa_con_nativa |
|---|---|---|---|---|
| nativa | 0.614 | 0.546 a 0.972 | 1.0 | None |
| udf_fila | 1.478 | 1.31 a 1.732 | 2.4 | False |
| udf_pandas | 1.798 | 1.468 a 2.283 | 2.9 | False |

- **Conclusion:** OBSERVADO: sobre el CSV de la ampliacion (1 002 240 filas, 5 particiones), las medianas fueron: nativa 0,61 s (rango 0,55 a 0,97), por fila 1,48 s (1,31 a 1,73; 2,4 veces) y vectorizada 1,80 s (1,47 a 2,28; 2,9 veces). Ninguno de los dos rangos de las funciones propias se solapa con el de la nativa; los de las dos funciones propias SI se solapan entre si. El conteo por clase es identico en las tres (47 567 / 906 454 / 48 219). En la corrida anterior del mismo cuaderno el orden de las dos funciones propias habia salido al reves (vectorizada 0,84 s y por fila 0,94 s). INTERPRETADO: se cumple la parte firme de la hipotesis (la nativa es la mas rapida, sin solape) y la funcion por fila queda en 2,4 veces, algo por encima del 1,5 a 2 estimado. Se refuta que la vectorizada se solapara con la nativa. Entre por fila y vectorizada NO hay diferencia que declarar con el CSV: sus rangos se solapan y su orden cambio entre dos ejecuciones del cuaderno, porque la lectura y la conversion del CSV (comun a las tres) domina y mete ruido.

### Con la columna de presion en memoria

| variante | mediana_s | rango_s | razon_vs_nativa | rango_se_solapa_con_nativa |
|---|---|---|---|---|
| nativa | 0.066 | 0.059 a 0.092 | 1.0 | None |
| udf_fila | 0.736 | 0.643 a 0.809 | 11.2 | False |
| udf_pandas | 0.306 | 0.279 a 0.313 | 4.6 | False |

- **Conclusion:** OBSERVADO: con la columna de presion en memoria, las medianas fueron: nativa 0,066 s, vectorizada 0,306 s (4,6 veces) y por fila 0,736 s (11,2 veces), sin solape entre ningun par de rangos; el orden coincide con el de la corrida anterior (5,5 y 9,8 veces). INTERPRETADO: se cumple la hipotesis del orden, y aqui la diferencia si es estable porque sin la lectura del CSV queda solo el costo de la funcion. La nativa se evalua en la JVM (sin nodo de Python en el plan); la vectorizada paga 104 lotes de Arrow (ArrowEvalPython); la funcion por fila serializa y llama 1 002 240 veces a Python (BatchEvalPython). Las razones observadas (4,6 y 11,2) quedaron por encima de lo estimado (2 a 4 y 5 a 10).

## Cierre diario: DataFrame frente a la linea base RDD

| medicion | trabajos | etapas | tareas | bytes_de_mezcla | tiempo_s |
|---|---|---|---|---|---|
| groupBy de DataFrame (hoy) | 2 | 2 (1 omitida) | 5 | 41100 | 0.44 |
| reduceByKey con RDD (bitacora de la carga con Spark) | n/d | 2 | 8 | 9822 | 3.282 |

- **Conclusion:** OBSERVADO: el cierre diario con groupBy de DataFrame sobre refinada_spark/ de la ampliacion lanzo 5 tareas en 2 etapas (1 omitida) y movio 41 100 B de mezcla en 0,44 s. El RDD con reduceByKey de nuestra T15 lanzo 8 tareas y movio 9 822 B en 3,28 s (ese tiempo incluye leer y convertir el Parquet heredado). El DataFrame mueve 4,2 veces mas bytes. INTERPRETADO: el DataFrame gana en tareas (AQE fusiona las particiones de mezcla), en tiempo y en codigo (una linea, sin lambdas), pero no en bytes: su agregacion parcial serializa filas de Tungsten con claves y acumuladores tipados, y el RDD solo serializa tuplas pequenas. Con 480 grupos, la diferencia de 31 KB es irrelevante: elegiriamos el DataFrame por mantenibilidad y por el optimizador.
