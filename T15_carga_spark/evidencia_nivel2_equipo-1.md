# Evidencia del Nivel 2 · equipo-1

## Perfil de la ampliacion

- Comando: `python practica/generar_ampliacion_s15.py --equipo equipo-1` (3.5 s)
- Medidores: 348 · lecturas (total, no incremento): 1002240
- Sector dominante: S-SUR · fraccion de medidores: 0.3448 · disco: 56.0 MiB

## Carga reejecutable y reconciliacion

| volumen | corrida | filas | consumo_total_m3 | hash_salida | duplicados | archivos_part | reconciliacion | tiempo_s |
|---|---|---|---|---|---|---|---|---|
| base | 1 | 480 | 92166.022 | e3e31f0e1f783725 | 0 | 1 | OK | 1.336 |
| base | 2 | 480 | 92166.022 | e3e31f0e1f783725 | 0 | 1 | OK | 2.012 |
| ampliacion | 1 | 480 | 160402.481 | a675b5e7fff0cd84 | 0 | 1 | OK | 2.689 |
| ampliacion | 2 | 480 | 160402.481 | a675b5e7fff0cd84 | 0 | 1 | OK | 2.761 |

## Barrido de particiones de mezcla (reduceByKey, ampliacion)

| particiones_mezcla | particiones_entrada | etapas | tareas | mezcla_escritura_bytes | mezcla_lectura_bytes | tiempo_mediana_s | tiempo_min_s | tiempo_max_s | hashes_distintos |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 4 | 2 | 8 | 9822 | 9822 | 2.587 | 2.577 | 2.6 | 1 |
| 8 | 4 | 2 | 12 | 11890 | 11890 | 3.142 | 2.895 | 3.485 | 1 |
| 50 | 4 | 2 | 54 | 26485 | 26485 | 4.043 | 3.96 | 4.092 | 1 |
| 200 | 4 | 2 | 204 | 41318 | 41318 | 7.241 | 6.827 | 7.281 | 1 |

### Configuracion: 4 particiones de mezcla

- **Hipotesis (escrita antes de medir):** Esperamos 4 particiones de entrada + 4 de mezcla = 8 tareas en 2 etapas. Con reduceByKey cada particion de entrada combina antes de mezclar y emite a lo sumo un registro por clave (fecha, sector) que contiene, asi que los bytes de mezcla seran del orden de 10 a 20 KB (unos 480 registros), los mismos que en la base aunque la ampliacion tenga 1,7 veces mas lecturas. Es la configuracion mas rapida: 4 ranuras, 4 tareas por etapa, una ola.
- **Configuracion exacta:** `python pipeline/carga_spark.py --equipo equipo-1 --volumen ampliacion --metodo reduceByKey --particiones-mezcla 4`
- **Medicion en la interfaz (pestana Stages):** 8 tareas, Shuffle Write 9822 B, Shuffle Read 9822 B; tiempo 2.6 s (mediana de 3)
- **Conclusion:** OBSERVADO: 8 tareas (4 + 4) en 2 etapas, 9 822 B de mezcla escritos y leidos, tiempo mediano de 2,6 s (rango 2,58 a 2,60), el menor del barrido (pestana Stages). INTERPRETADO: se confirma la hipotesis de tareas y tiempo. Los 9 822 B son unos 480 registros combinados: el combinador de reduceByKey reduce 1 002 240 lecturas a un registro por clave y por particion de entrada. Una sola ola de 4 tareas por etapa aprovecha exactamente las 4 ranuras.

### Configuracion: 8 particiones de mezcla

- **Hipotesis (escrita antes de medir):** 12 tareas (4 + 8). Los bytes de mezcla no deberian cambiar respecto de 4: dependen de cuantas claves salen de cada particion de entrada, no de n. El tiempo sera casi igual: con local[4] las 8 tareas de reduccion corren en dos olas, pero cada una procesa ~60 claves y dura milisegundos.
- **Configuracion exacta:** `python pipeline/carga_spark.py --equipo equipo-1 --volumen ampliacion --metodo reduceByKey --particiones-mezcla 8`
- **Medicion en la interfaz (pestana Stages):** 12 tareas, Shuffle Write 11890 B, Shuffle Read 11890 B; tiempo 3.1 s (mediana de 3)
- **Conclusion:** OBSERVADO: 12 tareas, 11 890 B de mezcla (+21 % frente a n = 4) y un tiempo mediano de 3,1 s, +0,5 s frente a n = 4 (dentro de la variacion del reloj). INTERPRETADO: la hipotesis de tiempo se confirma, pero la de bytes 'iguales' se REFUTA en parte: los bytes crecen con n aunque los registros sean los mismos, porque cada tarea de mapa escribe un bloque por particion de mezcla (4 x 8 = 32 bloques), y cada bloque tiene su propio encabezado de serializacion y compresion.

### Configuracion: 50 particiones de mezcla

- **Hipotesis (escrita antes de medir):** 54 tareas (4 + 50). Bytes de mezcla iguales o apenas mayores (cada bloque de mezcla tiene encabezado). Esperamos un tiempo algo mayor que con 4 u 8 por el costo fijo de planificar 50 tareas de ~10 claves cada una.
- **Configuracion exacta:** `python pipeline/carga_spark.py --equipo equipo-1 --volumen ampliacion --metodo reduceByKey --particiones-mezcla 50`
- **Medicion en la interfaz (pestana Stages):** 54 tareas, Shuffle Write 26485 B, Shuffle Read 26485 B; tiempo 4.0 s (mediana de 3)
- **Conclusion:** OBSERVADO: 54 tareas, 26 485 B de mezcla (2,7 veces los de n = 4) y un tiempo mediano de 4,0 s (+56 %). INTERPRETADO: se confirma el costo de planificacion: 50 tareas de unas 10 claves cada una duran menos que su propio arranque. Los bytes crecen por los 4 x 50 = 200 bloques de mezcla; nuestra hipotesis de 'bytes iguales' subestimo ese costo fijo por bloque.

### Configuracion: 200 particiones de mezcla

- **Hipotesis (escrita antes de medir):** 204 tareas (4 + 200): sobre-particionado, con ~2,4 claves por tarea y muchas tareas casi vacias. Bytes de mezcla similares. Esperamos el mayor tiempo del barrido (quizas +0,5 a 1 s) porque el costo de planificacion y de abrir 200 bloques de mezcla domina sobre el computo.
- **Configuracion exacta:** `python pipeline/carga_spark.py --equipo equipo-1 --volumen ampliacion --metodo reduceByKey --particiones-mezcla 200`
- **Medicion en la interfaz (pestana Stages):** 204 tareas, Shuffle Write 41318 B, Shuffle Read 41318 B; tiempo 7.2 s (mediana de 3)
- **Conclusion:** OBSERVADO: 204 tareas, 41 318 B de mezcla (4,2 veces los de n = 4) y un tiempo mediano de 7,2 s, casi el triple que n = 4. Es el mayor del barrido, con un rango de min. y max. que no se solapa con los de n = 4 u 8. INTERPRETADO: se confirma el sobre-particionado: unas 2,4 claves por tarea y 800 bloques de mezcla. El tiempo crecio mas de lo que estimamos (+0,5 a 1 s), asi que el costo por tarea en local[4] es de unos 25 ms.

## Operacion de agregacion sobre la ampliacion

| metodo | particiones_entrada | particiones_mezcla | etapas | tareas | mezcla_escritura_bytes | tiempo_s | hash_salida |
|---|---|---|---|---|---|---|---|
| reduceByKey | 4 | 4 | 2 | 8 | 9822 | 3.282 | a675b5e7fff0cd84fc0a3eabfea9d021fe04fa2375940c778f175c00f4ecc9f0 |
| groupByKey | 4 | 4 | 2 | 8 | 3240135 | 3.094 | a675b5e7fff0cd84fc0a3eabfea9d021fe04fa2375940c778f175c00f4ecc9f0 |

## Formato de entrada: Parquet de la refinada frente a CSV de la cruda (ampliacion)

| ruta | particiones_entrada | etapas | tareas | mezcla_escritura_bytes | tiempo_s | reconciliacion | hash_salida |
|---|---|---|---|---|---|---|---|
| parquet_refinada | 4 | 2 | 8 | 9822 | 3.282 | OK | a675b5e7fff0cd84fc0a3eabfea9d021fe04fa2375940c778f175c00f4ecc9f0 |
| csv_contraste | 120 | 2 | 240 | 50501 | 8.251 | OK | a675b5e7fff0cd84fc0a3eabfea9d021fe04fa2375940c778f175c00f4ecc9f0 |

## Tarea que lee mas bytes (groupByKey, clave sector, 8 particiones de mezcla)

| tarea | bytes_leidos | tiempo_ms | sectores |
|---|---|---|---|
| 0 | 0 | 118 | [] |
| 1 | 705753 | 173 | ['S-NORTE'] |
| 2 | 706246 | 288 | ['S-OESTE'] |
| 3 | 706272 | 172 | ['S-ESTE'] |
| 4 | 0 | 158 | [] |
| 5 | 0 | 158 | [] |
| 6 | 0 | 112 | [] |
| 7 | 1113066 | 139 | ['S-SUR'] |

Con reduceByKey y la misma clave:

| tarea | bytes_leidos | tiempo_ms | sectores |
|---|---|---|---|
| 0 | 0 | 80 | [] |
| 1 | 364 | 78 | ['S-NORTE'] |
| 2 | 364 | 87 | ['S-OESTE'] |
| 3 | 360 | 80 | ['S-ESTE'] |
| 4 | 0 | 79 | [] |
| 5 | 0 | 74 | [] |
| 6 | 0 | 75 | [] |
| 7 | 364 | 79 | ['S-SUR'] |

- **Conclusion:** OBSERVADO: con groupByKey, clave sector y 8 particiones, la tarea 7 (S-SUR) lee 1 113 066 B y las de S-NORTE, S-OESTE y S-ESTE unos 706 000 B cada una: razon 1,58. Cuatro tareas no reciben datos. La tarea mas lenta fue la 2 (S-OESTE, 288 ms), no la 7 (139 ms). Con reduceByKey cada tarea lee unos 364 B. INTERPRETADO: S-SUR es el sector dominante de nuestra ampliacion, con 120 de 348 medidores (p = 0,345) frente a 76 en cada uno de los otros tres. 120 / 76 = 1,58: los bytes leidos son proporcionales a las lecturas que mezcla groupByKey. El tiempo no lo refleja porque las tareas duran ~100 ms y el ruido de planificacion domina, por eso la senal fiable del sesgo son los bytes. Con reduceByKey el sesgo desaparece: cada sector llega ya combinado.

## Contraste con el groupBy de DataFrame

| aqe | shuffle.partitions | filas | tareas_reduccion | mezcla_bytes |
|---|---|---|---|---|
| true | 200 | 480 | 1 | 38255 |
| false | 200 | 480 | 200 | 38255 |
| false | 8 | 480 | 8 | 13373 |

- **Conclusion:** OBSERVADO: groupBy de DataFrame con AQE activa y shuffle.partitions = 200 ejecuto 1 sola tarea de reduccion (38 255 B de mezcla); con AQE desactivada ejecuto 200; con 8 ejecuto 8 (13 373 B). INTERPRETADO: lo configurado (200) no es lo ejecutado: AQE fusiona las particiones de mezcla pequenas despues de medirlas. Nuestro barrido uso la API de RDD (reduceByKey), donde lo configurado si es lo ejecutado.
