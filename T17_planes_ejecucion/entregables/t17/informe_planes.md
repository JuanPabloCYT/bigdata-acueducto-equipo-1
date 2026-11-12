# Informe de tres planes · equipo equipo-1

IFPN0025 · Big Data e Ingeniería de Datos · Universidad Ean · Autor: Juan Pablo Castro (trabajo individual: redactó los tres planes)

Un plan de ejecución es la evidencia de cuántos datos se leyeron, cuáles se pudieron evitar y por qué. Este informe lee tres planes con sus variantes de diagnóstico y los respalda con las métricas de `bitacora_planes.jsonl`. Todas las cifras corresponden a la **corrida 3** (la última medida) de cada consulta sobre la ampliación de `equipo-1`.

**Datos del informe.**
- **Volumen:** ampliación de `equipo-1` (348 medidores, 120 días y 1 002 240 lecturas), en `datalake_equipo/refinada_spark/` (120 archivos de unos 36,8 KiB, cada uno con un grupo de filas de 8 352 filas).
- **Día de la consulta A:** **2024-01-14**, el de mayor consumo total (1 343,381 m³) según `totales_control.csv`. Lo calcula `dia_de_a`, que desempata por la fecha más temprana.
- **Configuración** (de la bitácora): `spark_version` 3.5.3, `aqe` false (true solo en C-aqe), `shuffle_partitions` 8, `umbral_difusion` 10 485 760 B (−1 en C-forzada), `inferencia_tipos_particion` false (anio, mes y dia como texto) y `modo_explain` formatted.

## Plan A · Consumo por sector de un día (con su variante A-fecha)

**1. Qué se pidió y qué esperaban.** El consumo y las lecturas por sector del 2024-01-14. Hipótesis, registrada en `hipotesis_s17.json` a las 22:17:34.046 Z, antes de la primera medición (22:17:34.475 Z): **A lee 1 archivo**, porque `anio`, `mes` y `dia` son columnas de la ruta y se podan; **A-fecha lee 120**, porque `fecha` vive dentro de los archivos y no permite podar carpetas. Las dos hipótesis se cumplieron.

**2. Dónde se resolvió cada filtro.**
- **A** se resolvió **en la ruta**: `PartitionFilters: [isnotnull(anio), isnotnull(mes), isnotnull(dia), (anio = 2024), (mes = 01), (dia = 14)]` y `PushedFilters` vacío. El literal es texto con ceros (`'01'`), del mismo tipo que la columna, y por eso la poda funciona.
- **A-fecha** se resolvió **en el lector**: `PartitionFilters` vacío y `PushedFilters: [IsNotNull(fecha), EqualTo(fecha,2024-01-14)]`.

**3. Qué se leyó.**
- **A:** 1 archivo (36,7 KiB), 8 352 filas de salida del escaneo y `ReadSchema struct<sector,consumo_m3>`.
- **A-fecha:** 120 archivos (4 413,4 KiB, 120 veces más), las **mismas 8 352 filas** de salida y `ReadSchema` con `fecha` además.

**4. Qué no pudo evitarse y por qué.** A-fecha devuelve las mismas filas leyendo 120 veces más archivos porque el filtro empujado se evalúa **dentro** de cada archivo. El lector abre los 120 pies de archivo, compara las estadísticas mínima y máxima de `fecha` de cada grupo de filas (en 120 de 120 archivos el mínimo es igual al máximo: un archivo, un día) y **salta 119 grupos de filas** sin descomprimirlos. Pero **abrir** los 120 archivos y leer sus metadatos no se evita: eso solo lo logra la poda por la ruta. Hay que distinguir *archivos abiertos* (120) de *grupos de filas leídos* (1).

**Evidencia.**

| Campo de la bitácora | A | A-fecha |
|---|---|---|
| `partition_filters` | `[isnotnull(anio), isnotnull(mes), isnotnull(dia), (anio = 2024), (mes = 01), (dia = 14)]` | (vacío) |
| `pushed_filters` | (vacío) | `[IsNotNull(fecha), EqualTo(fecha,2024-01-14)]` |
| `read_schema` | `struct<sector:string,consumo_m3:double>` | `struct<sector:string,consumo_m3:double,fecha:date>` |
| `archivos_leidos` | 1 | 120 |
| `filas_salida_escaneo` | 8 352 | 8 352 |
| `etapas` · `tareas` | 2 · 9 | 2 · 12 |
| `hash_resultado` (deben ser iguales) | `d88345bc8d0a…` | `d88345bc8d0a…` |
| `diferencia_max_independiente` | 9,1e-13 | 9,1e-13 |

## Plan B · Lecturas con presión alta

**1. Qué se pidió y qué esperaban.** Las lecturas con `presion_bar > 3,5` por sector. Hipótesis: **120 archivos**, porque la presión no es columna de partición ni está ordenada, y cada archivo diario tiene presiones por encima y por debajo de 3,5 bar. Se cumplió.

**2. Dónde se resolvió el filtro.** En el lector: `PushedFilters: [IsNotNull(presion_bar), GreaterThan(presion_bar,3.5)]`, sin `PartitionFilters`. En el plan aparece también un `Filter` por encima de `ColumnarToRow`, porque el lector de Parquet usa el predicado para saltar grupos de filas y el filtro fila a fila se aplica después.

**3. Qué se leyó.** 120 archivos (4 413,4 KiB), **1 002 240 filas de salida del escaneo** (la capa completa) y 4 filas en el resultado. `ReadSchema` con tres columnas: `presion_bar` sola es el **78,5 %** de los bytes que lee B (N2.7).

**4. Qué no pudo evitarse y por qué.** El predicado está empujado, pero **no evita ningún archivo**. El lector independiente (pyarrow, N2.7) muestra que cada archivo tiene **un solo grupo de filas de 8 352 filas**, y que el **menor de los máximos de `presion_bar` es 3,977 bar**. Como en los 120 archivos el máximo supera 3,5, ninguna estadística permite descartar uno: el umbral de 3,5 bar deja **0 de 120** archivos saltables (con 4,0 bar serían 3, y con 4,2 bar, 97). Por eso `PushedFilters` no significa "menos datos leídos": significa "el lector podría saltar si las estadísticas lo permitieran". Aquí no lo permiten, porque la presión alta está repartida en todos los días.

**Evidencia.**

| Campo de la bitácora | B |
|---|---|
| `pushed_filters` | `[IsNotNull(presion_bar), GreaterThan(presion_bar,3.5)]` |
| `archivos_leidos` | 120 |
| `filas_salida_escaneo` | 1 002 240 |
| `filas_resultado` | 4 |
| `diferencia_max_independiente` | 1,8e-12 (conteo exacto: n_lecturas igual al del cálculo independiente) |

## Plan C · Consumo por estrato con la referencia de medidores (con C-forzada y C-aqe)

**1. Qué se pidió y qué esperaban.** El consumo y las lecturas por estrato de las lecturas confiables, uniendo por `meter_id` con `referencia/medidores.parquet` (348 filas). Hipótesis: 120 archivos en las tres variantes. Unión esperada: **BroadcastHashJoin** en C, porque la referencia pesa unos KiB, muy por debajo de los 10 MiB del umbral; **SortMergeJoin** en C-forzada, porque con umbral −1 no puede difundir; y **BroadcastHashJoin** en C-aqe, porque el plan inicial ya difunde. Las tres hipótesis se cumplieron.

**2. Dónde se resolvió cada filtro.** `es_confiable = true` se empuja al lector: `PushedFilters: [IsNotNull(es_confiable), EqualTo(es_confiable,true), IsNotNull(meter_id)]`. El `IsNotNull(meter_id)` lo agrega Catalyst, porque una unión interna nunca empareja nulos. No hay `PartitionFilters`.

**3. Qué se leyó y cuánto se movió.** Las tres variantes leen 120 archivos y 1 002 240 filas, con `ReadSchema struct<meter_id,consumo_m3,es_confiable>`: la consulta solo lleva a la unión las columnas que necesita. Lo que cambia es la mezcla:
- **C:** 1 `Exchange` (el de la agregación final) y **724 B** de mezcla.
- **C-forzada:** 3 `Exchange` y 2 `Sort`, con **7 799 575 B** de mezcla (10 773 veces más).
- **C-aqe:** como C, 724 B, pero en 6 tareas en lugar de 13 (1 etapa omitida).

**4. Qué no pudo evitarse y por qué.**
- **Por omisión**, Catalyst compara el **tamaño estimado** de cada lado con el umbral. En la base, el lado de la referencia se estima en 1 639 B frente a 597 939 B de las lecturas, así que difunde la referencia (`BroadcastExchange`). El **tamaño real** de lo difundido (*data size* del `BroadcastExchange`) es mucho mayor que la estimación: 4 210 672 B en la ampliación, porque se difunde la tabla hash ya construida y no el archivo de 3 KiB. Aun así cabe bajo los 10 MiB.
- **Al forzar la mezcla**, los dos lados se reparten por `meter_id` (2 `Exchange`) y se ordenan (2 `Sort`) antes de unirse: el millón de lecturas viaja por la mezcla, y por eso aparecen los 7,8 MB.
- **Con ejecución adaptativa**, el plan inicial (`fe97c6731ebb…`) y el final (`f0f5a8a2d17b…`) son distintos: la ejecución adaptativa reduce las tareas de la agregación final, de 8 particiones a menos, pero mantiene la difusión.
- **Con más volumen**, la difusión sigue siendo razonable mientras la referencia de medidores, que crece con los medidores y no con las lecturas, quepa bajo el umbral. Con unos 870 medidores, si escala lineal desde 4,2 MB por 348 (supuesto), superaría los 10 MiB y Catalyst pasaría solo a SortMergeJoin.

**Evidencia.**

| Campo de la bitácora | C | C-forzada | C-aqe |
|---|---|---|---|
| `umbral_difusion` | 10 485 760 | −1 | 10 485 760 |
| `aqe` | false | false | true |
| `estrategia_union` | BroadcastHashJoin | SortMergeJoin | BroadcastHashJoin |
| `n_exchange` · `n_sort` | 1 · 0 | 3 · 2 | 1 · 0 |
| `etapas` · `tareas` | 3 · 13 | 4 · 21 | 3 · 6 |
| `bytes_mezcla` | 724 | 7 799 575 | 724 |
| `hash_resultado` (deben ser iguales) | `0142edc26554…` | `0142edc26554…` | `0142edc26554…` |
| `hash_plan_inicial` · `hash_plan_final` | (no aplica) | (no aplica) | `fe97c6731ebb…` · `f0f5a8a2d17b…` |

## Lo observado frente a lo interpretado

- **A:** *observado*: 1 archivo leído con `PartitionFilters` sobre la ruta. *Interpretado*: el diseño por día con columnas de texto permite podar el cierre diario hasta un solo archivo.
- **A-fecha:** *observado*: 120 archivos abiertos y las mismas 8 352 filas y el mismo hash que A. *Interpretado*: el filtro sobre una columna interna salta grupos de filas, pero no evita abrir archivos.
- **B:** *observado*: predicado empujado, 120 archivos y 1 002 240 filas escaneadas. *Interpretado*: con un máximo de presión por archivo de al menos 3,977 bar, ninguna estadística permite saltar un archivo con el umbral de 3,5.
- **C:** *observado*: BroadcastHashJoin con 724 B de mezcla. *Interpretado*: la referencia, pequeña, se difunde y las lecturas no se mueven.
- **C-forzada:** *observado*: SortMergeJoin con 3 `Exchange`, 2 `Sort` y 7 799 575 B. *Interpretado*: sin difusión, el millón de lecturas tiene que repartirse y ordenarse.
- **C-aqe:** *observado*: la misma difusión, 6 tareas, y un plan final distinto del inicial. *Interpretado*: la ejecución adaptativa ajusta la agregación después de medir, pero no cambia una unión que ya era óptima.

Ninguna conclusión se apoya en tiempos: los tiempos de esta capa son de 0,1 a 0,5 s y varían más entre corridas que entre consultas.
