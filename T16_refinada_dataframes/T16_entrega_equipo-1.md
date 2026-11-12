# T16 · Hito B · El pipeline del acueducto consolidado: una capa refinada con esquema declarado, una función propia medida y una página que la gerencia pueda decidir

**Equipo:** equipo-1 (trabajo individual)
**Integrantes:** Juan Pablo Castro
**Proyecto:** Plataforma de datos del acueducto (proyecto integrador)
**Identificador de equipo (`--equipo`):** `equipo-1`
**Entorno usado:** contenedor del servicio `spark` con el perfil `s16` de Docker Compose (`acueducto_spark_s15`: Python 3.11, Java 11, PySpark 3.5.3, `local[4]` con 1 g, tope de 3 GiB). El cuaderno se ejecutó dentro del contenedor con `jupyter nbconvert --execute`.
**Fecha de entrega:** 2026-11-12

---

## 0. Perfil de la ampliación y entorno

- **Ruta de `evidencia_nivel2_<equipo>.md` y carpeta de trabajo:** `T16_refinada_dataframes/evidencia_nivel2_equipo-1.md`. Carpeta de trabajo: `~/proyectos/trabajo/s16` en macOS, fuera de carpetas sincronizadas.
- **Perfil en una línea:** 348 medidores · 1 002 240 lecturas (total de la ampliación) · S-SUR dominante, p = 0,3448 (120 de 348 medidores) · 5 particiones de entrada del lector de la cruda (archivos por partición [29, 29, 29, 29, 4]) · 56,0 MiB en `datalake_equipo/`.

---

## 1. Nivel 1 y contraste con la solución de referencia (elemento obligatorio del producto)

- **Ruta de `mediciones_nivel1.csv` en el repositorio:** `T16_refinada_dataframes/mediciones_nivel1.csv`
- **Cifra del Nivel 1 que cambiaría su decisión sobre la forma de clasificar la presión, y por qué:** las medianas sobre el CSV de la base. En una ejecución del cuaderno los rangos de las tres variantes **se solaparon** (nativa 0,60 a 1,20 s, por fila 0,75 a 0,91 s, vectorizada 0,60 a 0,73 s), y en la ejecución final no (0,447, 0,746 y 0,649 s). Si con la columna en memoria también se solaparan, no habría diferencia que declarar y la decisión dependería solo de la mantenibilidad. No es el caso: en memoria, la función por fila tarda de 4,4 a 11,2 veces lo que la nativa en todas las ejecuciones.

| Decisión de su registro de la sesión anterior | En la solución de referencia | Qué mantienen o cambiarían, y por qué |
|---|---|---|
| Operación de agregación del cierre diario | `reduceByKey` y no `groupByKey`: combina dentro de cada partición antes de mezclar | **Coincide**: nuestra T15 midió 9 822 B frente a 3 240 135 B en la ampliación. Hoy cambiaríamos el cierre al `groupBy` de DataFrame: mueve 41 100 B (4,2 veces más), pero usa 5 tareas en lugar de 8, tarda 0,44 s y es una sola línea legible. Con 480 grupos, 31 KB más de mezcla no importan. |
| Qué se reconcilia | Sin filtro de calidad: la capa y las referencias cuentan todas las lecturas | **Coincide.** La capa nueva conserva las 576 000 y 1 002 240 lecturas y marca la confiabilidad en `es_confiable` (541 368 verdaderas en la base). |
| Formato de entrada | La refinada en Parquet (marca temporal en nanosegundos); hoy se construye una capa nueva desde la cruda | **Coincide** en que Parquet es el formato de lectura (en la T15, el CSV costó 30 veces las tareas). Hoy la capa nueva **se construye desde la cruda** con esquema declarado, y la refinada heredada pasa a ser la referencia de la reconciliación. |
| Forma de la escritura | `overwrite` con `coalesce(1)`: un archivo por salida | Para las 480 filas del agregado **coincide**. Para la capa refinada cambiamos a **partición por día con sobrescritura dinámica**: el reproceso de 6 días conservó 114 de 120 particiones, mientras que con la estática solo quedaron 28 800 filas. |

---

## 2. Capa refinada: esquema, reejecución, reconciliación y reproceso (condiciones de aceptación 1 y 2; criterio 2 de la rúbrica)

### 2.1 Esquema declarado y calidad de la lectura

- **Ruta de `pipeline/refinada_spark.py` y función donde vive el esquema:** `T16_refinada_dataframes/pipeline/refinada_spark.py`, función `definir_esquema()`. También está versionado en la raíz del repositorio (`pipeline/refinada_spark.py`).
- **Modo de lectura, correspondencia de columnas (`enforceSchema`) y cifra de filas no conformes:** `PERMISSIVE` con `enforceSchema=False` y `timestampFormat="yyyy-MM-dd HH:mm:ss"`. `filas_no_conformes` = **0** en las cuatro corridas (base y ampliación, dos cada una).
- **Nulos por columna antes de escribir:** **0** en las seis columnas, tanto en la base como en la ampliación (`nulos_por_columna`).

| Columna | Tipo declarado y por qué (precisión, rango, qué pasaría con otro tipo) |
|---|---|
| `meter_id` | `StringType`. Es un código (`MED0001`, `MEQ-0000001`), no un número: como número perdería el prefijo y los ceros a la izquierda. |
| `timestamp` | `TimestampType`, que Spark interpreta en la zona UTC de la sesión con el formato declarado. Se escribe en Parquet como `INT64` con microsegundos ajustados a UTC: la precisión de microsegundo sobra para lecturas horarias, y el instante no depende del computador que lee. |
| `sector` | `StringType`. Es un código de cuatro valores (`S-NORTE`…), la clave de los cierres. |
| `consumo_m3` | `DoubleType`. Con un **entero**, `PERMISSIVE` deja nulos sin error: en el Nivel 1 hubo 576 000 nulos y 0 de 480 grupos reconciliados. Con un **`float`** de 32 bits (unas 7 cifras significativas), la suma de 2 880 lecturas por grupo acumula un error del orden de 10⁻⁴ m³, por encima de la tolerancia de 10⁻⁶. |
| `presion_bar` | `DoubleType`, por la misma razón. Además, los umbrales 2,5 y 3,5 bar se comparan en doble precisión: un `float` puede convertir 2,5 en 2,4999999 y cambiar la clase en la frontera. |
| `quality_flag` | `StringType`. Es una bandera de texto (`OK`, `ESTIMADO`…); `es_confiable` se deriva de ella. |

### 2.2 Marca temporal leída con un lector independiente (pyarrow)

| Capa | Tipo físico y tipo lógico (unidad y si está ajustado a UTC) | Tipo que presenta pyarrow como Arrow |
|---|---|---|
| `refinada_spark/` (nueva) | `INT64` · `Timestamp(isAdjustedToUTC=true, timeUnit=microseconds)` | `timestamp[us, tz=UTC]` |
| `refinada/` (heredada) | `INT64` · `Timestamp(isAdjustedToUTC=false, timeUnit=nanoseconds)` | `timestamp[ns]` |

**Qué dos cifras del archivo permiten decir que otro motor leerá el mismo instante, y cómo se compararon las dos capas:** la **unidad** (`timeUnit=microseconds`) y el **ajuste a UTC** (`isAdjustedToUTC=true`), leídos del pie del archivo con `pq.ParquetFile(...).schema`. No usamos el tipo Arrow, que puede ocultar si el archivo es `INT96`. Las dos capas se compararon sobre el **entero de la época**: para MED0001 a las 13:00 del 2024-01-01, la capa nueva guarda 1704114000000000 (16 dígitos, µs) y la heredada 1704114000000000000 (19 dígitos, ns), es decir, el mismo instante × 1 000. La reconciliación por grupo comparó además la marca mínima y máxima de los 1 440 grupos (fecha, sector, calidad) en las dos capas: **1 440 de 1 440 iguales**.

### 2.3 Reejecución: dos corridas por volumen

| Volumen | Filas escritas | `hash_salida`, corrida 1 | `hash_salida`, corrida 2 |
|---|---|---|---|
| Base común (`datalake/refinada_spark/`) | 576 000 | `9cc455c0aff897a5…7c7a` | `9cc455c0aff897a5…7c7a` |
| Ampliación (`datalake_equipo/refinada_spark/`) | 1 002 240 | `7b0ac06a1ab1c7af…c982` | `7b0ac06a1ab1c7af…c982` |

Hashes completos: base `9cc455c0aff897a5a4a4a3c7e851152c19d2c623c9f9a0cb76c2e16e8ab37c7a` y ampliación `7b0ac06a1ab1c7afabbfa221572e30142ae3e5dc0f58af9f422ae830fde3c982`, en `bitacora_refinada_spark.jsonl`.

**¿Las dos corridas de cada volumen dejan el mismo número de filas, las mismas sumas y el mismo hash?** Sí. Son las mismas filas y el mismo hash de la firma por grupo, que incluye filas, sumas redondeadas a 6 decimales y marcas mínima y máxima. La diferencia máxima de sumas frente a la heredada es 0,00e+00 en los dos volúmenes, así que no hace falta invocar la tolerancia. Las dos corridas escriben 120 archivos.

**Por qué "misma salida" se entiende aquí como mismo contenido y no como mismos archivos, y qué pasaría con una escritura que anexa en lugar de sobrescribir:** cada escritura crea `part-000NN-<uuid>.snappy.parquet` con un identificador nuevo, así que dos corridas idénticas nunca dejan los mismos nombres de archivo. Lo que debe repetirse es el contenido, y eso lo mide el hash de la firma por grupo. Con `append`, cada carpeta de día acumularía un archivo más por corrida: tras dos corridas habría 1 152 000 filas en la base y la reconciliación con la heredada fallaría en los 1 440 grupos (el doble de filas y de consumo).

### 2.4 Reconciliación sin filtro de calidad

| Volumen y referencia | Grupos coincidentes · diferencia máxima |
|---|---|
| Base común contra `refinada/` heredada (fecha, sector, `quality_flag`; 1 440 grupos: filas, sumas, marca temporal mínima y máxima) | 1 440 de 1 440 · 0,00e+00 |
| Base común contra `consolidada/` (fecha y sector; 480 grupos: filas y suma de consumo) | 480 de 480 · 0,0 (lector independiente pandas) |
| Ampliación contra su `refinada/` heredada (1 440 grupos) | 1 440 de 1 440 · 0,00e+00 |
| Ampliación contra `totales_control.csv` (480 grupos) | 480 de 480 · 5,7e-14 (lector independiente pandas) |

La base también reconcilia contra `consolidada_spark/` de nuestra T15: 480 de 480 grupos.

- **Línea del código donde se ve que ninguna reconciliación lleva filtro de calidad, y columna donde queda marcada la confiabilidad:** `pipeline/refinada_spark.py`, en `main`: `refinada = transformar(crudo, a.variante)` y luego `escribir_capa(refinada, salida, ...)`, sin ningún `filter` sobre `quality_flag`. Los únicos `filter` son los de `--desde` y `--hasta`, sobre la fecha. La confiabilidad queda en la columna **`es_confiable`** (`quality_flag == 'OK'`), que se deriva en `columnas_derivadas`.
- **Contraste con un agregado filtrado por `quality_flag = 'OK'`:** deja **541 368 de 576 000** lecturas en la base (94,0 %) y 942 031 de 1 002 240 en la ampliación. No reconcilia porque las referencias (`consolidada/`, `totales_control.csv`) se calcularon con todas las lecturas: cada grupo filtrado tiene menos lecturas y menos consumo. En la T15, los 480 grupos fallaron en lecturas y en suma.

### 2.5 Árbol de salida y reproceso de una ventana (condición de aceptación 2)

- **Árbol resultante:** `datalake_equipo/refinada_spark/anio=2024/mes=01…04/dia=01…31` tiene **120 carpetas de día**, con **un archivo Parquet por carpeta** (unos 38 KiB; por ejemplo, `dia=05` pesa 37 797 B), unos **5,2 MiB** en total. Las rutas llevan ceros (`mes=01`, `dia=05`) porque `mes` y `dia` se derivan como **texto** con `date_format(..., 'MM')` y `'dd'`. Dentro de los archivos no quedan las columnas `anio`, `mes` ni `dia` (las retira `partitionBy`). En la base: 120 archivos, 3,8 MiB.
- **Ventana de 6 días reprocesada y ruta de la copia de la base:** del 2024-01-01 al 2024-01-06, sobre copias de `datalake/refinada_spark/` en `_copias_reproceso/estatica` y `_copias_reproceso/dinamica`, carpeta temporal del cuaderno (celda N2.6). La capa original no se tocó.

| Modo (`--sobrescritura`) | Filas que quedan · particiones intactas · reescritas · perdidas |
|---|---|
| `estatica` | 28 800 · 0 · 6 · 114 (reconciliación con la heredada: FALLA) |
| `dinamica` | 576 000 · 114 · 6 · 0 (mismo hash `9cc455c0aff8` que la capa original; reconciliación OK) |

**Cómo comprobaron las filas y las particiones sin abrir un archivo a mano, y qué modo adoptan para la capa:** la celda N2.6 compara el árbol de carpetas antes y después (`resumen_arbol` y `comparar_arboles`: carpetas y archivos con su tamaño y fecha de modificación) para contar las particiones intactas, reescritas y perdidas. Las filas se cuentan releyendo la copia, y la firma por grupo da el hash y la reconciliación. Con `mes` y `dia` como entero, el reproceso dinámico crea `mes=1` al lado de `mes=01` y al releer hay 604 800 filas (28 800 duplicadas). **Adoptamos la sobrescritura dinámica con rutas de texto con ceros.**

---

## 3. Comparación de las tres formas de clasificar y cierre diario (criterio 3 de la rúbrica)

- **Hipótesis escrita antes de medir:** sobre el CSV, la nativa sería la más rápida pero con poca diferencia, porque la lectura del CSV es un costo común; la vectorizada se solaparía con la nativa y la función por fila tardaría de 1,5 a 2 veces más, sin solape. Con la columna en memoria, las diferencias se amplificarían: vectorizada de 2 a 4 veces y por fila de 5 a 10 veces, sin solapes. El texto completo está en la celda N2.7 y en la evidencia.
- **Origen de cada condición:**
  - **Condición A:** `python pipeline/refinada_spark.py --equipo equipo-1 --volumen ampliacion --comparar-variantes`, ejecutado desde la celda N2.7 (a) con `correr_capa("ampliacion", comparar=True)`. Son 12 corridas registradas en la bitácora (`numero_corrida` de 0 a 3).
  - **Condición B:** la celda N2.7 (b), que carga `select("presion_bar").cache()` de la ampliación.
  - **Cómo se mide:** en las dos condiciones la consulta se construye de nuevo en cada corrida (`consulta_clase`) y la acción cronometrada es `groupBy("nivel_presion").count()`, que consume la clase.

### 3.1 Resultado idéntico entre variantes

| Concepto | Valor |
|---|---|
| Conteo por clase, idéntico en las tres variantes (`baja` · `normal` · `alta` · nulos) | 47 567 · 906 454 · 48 219 · 0 |
| Nodo del plan de la nativa · de la función por fila · de la vectorizada | ninguno (expresión `CASE WHEN` en la JVM) · `BatchEvalPython` · `ArrowEvalPython` |

### 3.2 Razones entre variantes y solape de rangos

| Comparación | Condición A (desde el CSV) | Condición B (columna en memoria) |
|---|---|---|
| Función por fila frente a nativa | 2,4 · no se solapan | 11,2 · no se solapan |
| Función vectorizada frente a nativa | 2,9 · no se solapan | 4,6 · no se solapan |
| Función por fila frente a vectorizada | 0,82 · **sí se solapan** (1,31 a 1,73 frente a 1,47 a 2,28) | 2,4 · no se solapan |

**Lectura.**
- **Observado:** en la condición A, la nativa (0,61 s) es la más rápida y no se solapa con ninguna función propia. Las dos funciones propias se solapan entre sí, y su orden cambió entre dos ejecuciones del cuaderno: vectorizada 0,84 s y por fila 0,94 s en la primera, 1,80 s y 1,48 s en la final. En la condición B, el orden nativa < vectorizada < por fila es estable y no hay ningún solape (0,066, 0,306 y 0,736 s).
- **Interpretado:** se confirma que la nativa gana en las dos condiciones. Se refuta nuestra hipótesis de que la vectorizada se solaparía con la nativa sobre el CSV: con 1 millón de filas, el viaje a Python ya pesa. Sobre el CSV **no hay diferencia que declarar entre por fila y vectorizada**, porque la lectura y la conversión del CSV, comunes a las tres variantes, meten un ruido del mismo orden que la diferencia. En memoria, sin ese costo común, queda solo el costo de la función, y ahí las razones (4,6 y 11,2) superaron lo que estimamos.

### 3.3 Invocaciones y lotes en su ampliación

- **Función por fila:** **1 002 240 invocaciones** y 1 002 240 filas serializadas hacia Python, una por fila. En el Nivel 1, sobre la base, los acumuladores midieron 576 000 y 576 000, enviadas en 5 760 lotes de 100 filas.
- **Función vectorizada:** las invocaciones son una por lote de Arrow. **Calculado para la ampliación:** el lector reparte la cruda en 5 particiones con [242 208, 242 208, 242 208, 242 208, 33 408] filas, así que los lotes son Σ⌈filas / 10 000⌉ = 25 + 25 + 25 + 25 + 4 = **104 lotes**. La bitácora del script no registra lotes, solo el nodo `ArrowEvalPython`. El contraste medido con acumuladores se hizo en el Nivel 1 sobre la base: 60 lotes (56 de 10 000 y 4 de 4 000) frente a 60 calculados, una coincidencia exacta. Son 1 002 240 invocaciones frente a 104: una razón de unas 9 600 en invocaciones, pero de solo 2,4 en tiempo (condición B). Una razón de invocaciones no es una razón de tiempo.

### 3.4 Cierre diario en DataFrame frente a la línea base RDD

| Cierre por fecha y sector | Etapas · tareas | Bytes de mezcla · tiempo (s) |
|---|---|---|
| RDD con `reduceByKey` (bitácora de T15, su ampliación) | 2 · 8 | 9 822 · 3,282 |
| `groupBy` de DataFrame sobre `refinada_spark/` | 2 (1 omitida) · 5 | 41 100 · 0,440 |

**Interpretación:** el **DataFrame gana en tareas** (5 frente a 8, porque AQE fusiona las particiones de mezcla), **en tiempo** y en código: una línea, sin lambdas, con tipos que el optimizador conoce. El tiempo del RDD incluye además convertir los nanosegundos de la heredada en Python. El **RDD gana en bytes de mezcla**, con 4,2 veces menos: su combinador emite tuplas mínimas, mientras que la agregación parcial del DataFrame serializa filas de Tungsten con claves tipadas y acumuladores. Como criterio de ingeniería, elegimos por la métrica que se va a escalar. Con 480 grupos, 31 KB de mezcla no pesan, así que elegimos el DataFrame por mantenibilidad y por el optimizador. Revisaríamos la decisión si la mezcla del cierre llegara a decenas de MB por partición.

---

## 4. Registro de decisiones (criterio 3 de la rúbrica)

- **Ruta del registro de decisiones en el repositorio:** esta sección (`T16_refinada_dataframes/T16_entrega_equipo-1.md`, sección 4), con el formato de la plantilla ADR del cuaderno (celda N2.9).

| Decisión | Estado y decisión adoptada (en qué condiciones vale) | Evidencia propia (cifra y origen) | Alternativa descartada y su costo | Cifra que obligaría a revisarla |
|---|---|---|---|---|
| ADR-1 · Esquema: inferido o declarado, y dónde vive (obligatoria) | **Aceptada.** Esquema **declarado**, versionado en `definir_esquema()` de `pipeline/refinada_spark.py`: texto, marca temporal y `double`. Vale mientras la fuente conserve las seis columnas del encabezado. | Nivel 1 (base): la lectura inferida lanza **3 trabajos y lee 56,54 MiB**; la declarada, **0 trabajos al leer y 28,24 MiB**. Ampliación: **0 nulos** en las seis columnas y 0 filas no conformes en las dos corridas. | `inferSchema`: lee la cruda dos veces (el doble de MiB) y los tipos pueden cambiar entre días sin aviso. Un esquema mal declarado (consumo como entero) deja **576 000 nulos** sin error. | Un nulo por columna mayor que 0 en una corrida, o un cambio en el encabezado de la fuente. |
| ADR-2 · Modo de lectura y correspondencia de columnas | **Aceptada.** `PERMISSIVE` con `enforceSchema=False` y conteo de nulos y filas no conformes antes de escribir. | Ampliación: `filas_no_conformes` = 0 y nulos = 0. Nivel 1: con columnas reordenadas y `enforceSchema=True`, S-ESTE suma **3 601,5 m³ en lugar de 190,7**, sin error ni nulos. Con `False`, la lectura **falla** ("CSV header does not conform"). Nivel 3: un formato de fecha distinto deja **4 800 nulos** en `timestamp`, que solo detecta el conteo de nulos. | `FAILFAST`: detiene el cierre completo por una sola fila mala, aunque las demás sean válidas. `DROPMALFORMED`: con el tipo errado deja **0 filas** sin error. | Más de 0 filas no conformes en una corrida. Cualquier valor exige investigar antes de publicar la capa. |
| ADR-3 · Tipo de la marca temporal, de los números y de las columnas de la ruta | **Aceptada.** `TIMESTAMP_MICROS` (`INT64`, microsegundos, ajustado a UTC), `double` para consumo y presión, y `anio`, `mes` y `dia` como texto con ceros. | pyarrow lee la capa nueva como `INT64`, `isAdjustedToUTC=true, unidad=microseconds`, `timestamp[us, tz=UTC]`, y la heredada como `INT64` con nanosegundos sin ajuste. Hay 1 440 grupos iguales en marcas mínima y máxima, y el mismo instante en la época (×1 000). Con `mes` entero: `mes=1` junto a `mes=01` y **28 800 filas duplicadas** al releer. | `INT96` (el valor por omisión): un formato heredado que otros lectores interpretan con otra zona o precisión. `mes` entero: rutas sin ceros y duplicados en el reproceso. | Un consumidor que necesite nanosegundos (lecturas por debajo del microsegundo), o un lector que no soporte `timestamp[us, tz=UTC]`. |
| ADR-4 · Función propia frente a expresión nativa (obligatoria) | **Aceptada.** **Expresión nativa** (`when`/`otherwise`) para `nivel_presion`. Si alguna vez hiciera falta una función propia, sería la vectorizada. | Ampliación: conteo idéntico (47 567 / 906 454 / 48 219). En memoria: nativa 0,066 s, vectorizada 0,306 s (4,6 veces) y por fila 0,736 s (11,2 veces), sin solapes. Sobre el CSV: nativa 0,61 s; las funciones propias de 1,48 a 1,80 s, sin solape con la nativa. | Función por fila: 1 002 240 invocaciones y serialización por cierre. Extrapolando linealmente (supuesto), a 100 000 medidores serían 2 400 000 invocaciones por cierre diario. Vectorizada: 104 lotes de Arrow y 4,6 veces el tiempo de la nativa. | Que la regla deje de poder escribirse con expresiones nativas (por ejemplo, un modelo de Python), o que la función por fila se solapara con la nativa también en memoria. |
| ADR-5 · Forma de la escritura | **Aceptada.** Partición por `anio/mes/dia` (texto con ceros), **un archivo por día**, `overwrite` con sobrescritura **dinámica**. | Ampliación: 120 carpetas con un archivo de unos 38 KiB cada una (5,2 MiB en total). Reproceso de 6 días sobre la copia de la base: la dinámica deja 576 000 filas y 114 particiones intactas con el mismo hash; la estática, 28 800 filas y 114 perdidas. | Sobrescritura estática: reprocesar 6 días **borra los otros 114** (547 200 filas perdidas). Un solo archivo para la capa: cada reproceso reescribiría el millón de filas. | Archivos por día por debajo de unos pocos KiB (habría que compactar por mes), o por encima de unos 128 MiB (habría que partirlos). |

**Decisiones justificadas con cifras propias en total (mínimo cuatro, entre ellas la ADR-1 y la ADR-4):** 5

---

## 5. Integración del pipeline T12 a T16 (condición de aceptación 3; criterio 1 de la rúbrica)

### 5.1 Índice de evidencias

| Etapa | Comando | Resultado esperado | Commit y resultado obtenido (última línea y las que difieran) |
|---|---|---|---|
| T12 · Ingesta incremental | `python pipeline/verificar_etapa.py --etapa t12` (perfil `s12` arriba) | `Etapa t12: APROBADA (4 de 4 comprobaciones, N s)` | VERIF_T12 |
| T13 · Conector de la API | `python pipeline/verificar_etapa.py --etapa t13` (perfil `s12` arriba) | `Etapa t13: APROBADA (5 de 5 comprobaciones, N s)` | VERIF_T13 |
| T14 · Captura de cambios | `python pipeline/verificar_etapa.py --etapa t14` (perfil `s12` arriba) | `Etapa t14: APROBADA (3 de 3 comprobaciones, N s)` | VERIF_T14 |
| T15 · Carga con Spark | `docker compose --profile s16 exec spark python pipeline/verificar_etapa.py --etapa t15` | `Etapa t15: APROBADA (6 de 6 comprobaciones, N s)` | VERIF_T15 |
| T16 · Capa refinada | `docker compose --profile s16 exec spark python pipeline/verificar_etapa.py --etapa t16` | `Etapa t16: APROBADA (15 de 15 comprobaciones, N s)` | VERIF_T16 |

Nota sobre T12 a T14: el verificador comprueba las tablas de **referencia** del paquete (base `acueducto`, con 1 200, 90 y 94 filas). Las cifras de nuestra instancia `equipo-1` están en las bases propias (`acueducto_equipo_1`) y en las evidencias de cada tarea: 888/1 776 lecturas, 99 medidores y 102 suscriptores.

**Reejecución sin duplicados en las etapas que cargan (T12 a T15):**
- **T12:** doble ejecución 888 → 888 filas, con la misma suma (141,897 m³) y las mismas marcas.
- **T13:** `stg_estado_medidor` 99 → 99 (la bitácora crece de 9 a 19, como debe).
- **T14:** una tercera corrida sin cambios no mueve ni el staging ni el checkpoint. La repetición con el checkpoint restablecido sí duplicó 21 filas: está documentada, junto con la clave única que lo evita.
- **T15:** dos corridas por volumen con el mismo hash; las 19 corridas de la ampliación tienen el hash `a675b5e7…`.
- **T16:** dos corridas por volumen con el mismo hash (sección 2.3).

### 5.2 Reparto por integrante

| Integrante · partes que hizo · horas |
|---|
| Juan Pablo Castro · todo el trabajo (trabajo individual: código, ejecución, evidencias, registro de decisiones, nota y lectura) · unas 4 h |

**Total de horas del equipo y, si superó las 4 horas estimadas, qué parte lo explica:** unas 4 horas. La ejecución completa del cuaderno dentro del contenedor tarda unos 2,5 minutos por corrida. Se ejecutó dos veces porque los tiempos de la primera corrida no coincidían con las conclusiones escritas; reescribimos las conclusiones con las cifras de la corrida final.

### 5.3 Decisión de una etapa anterior que revisaron con evidencia nueva

**Etapa T15 · operación del cierre diario.** La decisión original fue `reduceByKey` sobre RDD, porque mueve 9 822 B de mezcla frente a 3 240 135 B con `groupByKey`. La cifra nueva viene del cierre con `groupBy` de DataFrame sobre `refinada_spark/`: mueve **41 100 B (4,2 veces más que el RDD)**, pero usa **5 tareas en lugar de 8**, tarda **0,44 s frente a 3,28 s** y cabe en una línea sin lambdas. **Decisión que la reemplaza:** el cierre diario se calcula con `groupBy` de DataFrame sobre la capa refinada. La métrica en la que pierde (bytes) es irrelevante con 480 grupos, y las que gana (tareas, tiempo y mantenibilidad) pesan más.

### 5.4 README del repositorio

El bloque de la plantilla está copiado y completado en `README.md`, en la raíz del repositorio. Usa la opción B: `datalake/` se extrae del paquete.

### 5.5 Verificación de la etapa T16 desde un clon limpio

| Dato | Valor |
|---|---|
| Carpeta nueva usada para el clon (fuera de su copia de trabajo) y commit clonado | CLON_CARPETA |
| Duración total de la secuencia del README (segundos) y última línea de la verificación de la etapa T16 | CLON_RESULTADO |
| Pasos que hubo que agregar al README para que funcionara (y quedaron escritos en él) | CLON_PASOS |

---

## 6. Puente de modelado (criterio 4 de la rúbrica)

El proyecto del primer módulo es la **precipitación del IDEAM** (repositorio `bigdata-ean-ideam`).

| Decisión | En el modelo de su proyecto | En el modelo del acueducto y en la capa de hoy | ¿Coincide o diverge? ¿Por qué? |
|---|---|---|---|
| T9 · Decisión de arquitectura (registro de decisión) | ADR-001: **arquitectura híbrida**, con una vía principal por lotes para el histórico diario y un flujo acotado solo para la alerta de lluvia. Se descartó Lambda porque duplica lógica, y la alerta es un cálculo distinto, no una versión rápida del histórico. | El acueducto es una vía **por lotes** sobre el lago por capas (`cruda/` → `refinada_spark/` → `consolidada/`), sin vía de velocidad. La capa de hoy se reprocesa por día con sobrescritura dinámica. | **Coincide** en el núcleo por lotes: las dos fuentes publican por día y los cierres son sobre periodos cerrados. **Diverge** en que el acueducto no tiene requisito de segundos, así que no necesita el flujo acotado de la alerta. |
| T10 · Grano, dimensiones y claves | Grano de `hechos_precipitacion`: **una observación de un sensor de una estación en un instante** (clave `codigoestacion` + `codigosensor` + `fechaobservacion`, verificada en 141 007 de 141 007 filas). El tiempo se separa en `dim_fecha` y `dim_hora_del_dia`. | Grano de `hechos_lectura`: **una lectura de un medidor en una hora**. `refinada_spark/` conserva ese grano (576 000 filas = 200 × 24 × 120) y deriva `fecha` y `hora` como columnas separadas. | **Coincide**: los dos usan el grano más fino que publica la fuente y separan fecha y hora, lo mismo que hace `columnas_derivadas` hoy. |
| T11 · Tipo de dimensión o agregados | `dw.dim_estacion` de **tipo 2** (`vigente_desde`, `vigente_hasta`, `es_actual`) y `dw.dim_sensor` de **tipo 1**. Sin tabla agregada persistente: el agregado mensual se creó solo para medir Q2. | `dim_medidor` de **tipo 2** y `dim_sector` de **tipo 1**, más una **tabla agregada mensual** persistente. Hoy, `consolidada/` y el cierre diario por (fecha, sector) son agregados persistidos. | **Coincide** en el tipo de las dimensiones: lo que cambia de forma rastreable (la estación o el medidor) es tipo 2, y las correcciones de catálogo son tipo 1. **Diverge** en los agregados: el acueducto persiste cierres diarios y mensuales porque se reconcilian y se facturan; el IDEAM los calculaba al vuelo. |

**Modificación escrita sobre su propio modelo:** cambiaría `dw.hechos_precipitacion`, hoy una tabla sin partición, por una tabla **particionada por día de `fechaobservacion` (`anio=AAAA/mes=MM/dia=DD`, en texto con ceros) y con sobrescritura dinámica**, porque la fuente del IDEAM republica por día. Reprocesar una partición de 141 007 observaciones no debe reescribir el histórico, y el reproceso de 6 días de hoy mostró que con sobrescritura estática se pierden 114 de 120 particiones.

---

## 7. Lectura en inglés (criterio 5 de la rúbrica)

### 7.1 Resumen técnico en inglés (de 150 a 200 palabras)

Armbrust et al. (2015) introduce Spark SQL, a module that integrates relational processing with Spark's functional programming API. The starting point is a limitation of programs written directly on RDDs: Spark sees each transformation as an opaque function over generic objects, so it cannot inspect the data's structure or the computation's intent and therefore cannot optimize it. The paper answers with the DataFrame, a distributed collection of rows with a known schema whose operations are built from column expressions. Because a query over columns is declarative, Spark knows which columns are read, which predicates filter rows and which aggregations are requested; the extensible optimizer, Catalyst, can then prune columns, push filters into the data source and generate compact bytecode for the whole expression. A user-defined function breaks this knowledge: the engine sees a black box, must materialize every row in the format the function expects and, in PySpark, serialize it to a Python worker and back. The Arrow guide adds that vectorized pandas UDFs reduce this cost by transferring columnar batches instead of single rows, although they still cross the JVM–Python boundary.

**Número de palabras:** 182

### 7.2 Respuestas a las tres preguntas guía

1. **¿Qué limitación de los programas escritos sobre RDD motiva los DataFrames?** Que las transformaciones de un RDD son **funciones opacas** sobre objetos sin esquema: Spark ejecuta lo que el programa dice, pero no puede saber qué columnas se usan ni qué hace cada función, y por eso no puede optimizar. Armbrust et al. lo presentan como la tensión entre el procesamiento relacional, que es optimizable, y el procedimental, que es flexible. Los DataFrames dan una API con esquema y expresiones declarativas que el optimizador sí entiende.
2. **¿Qué sabe Spark de una consulta sobre columnas que no puede saber de una función anónima?** Sabe el **esquema** y los tipos, **qué columnas** se leen (puede podar las demás), **qué predicados** filtran (puede empujarlos hacia la fuente) y **qué agregaciones** se piden (puede combinarlas por partes y generar código). De una lambda solo sabe que recibe una fila y devuelve algo. Lo vimos en el plan: la expresión nativa no agrega ningún nodo y se evalúa dentro de la JVM; las funciones propias agregan `BatchEvalPython` o `ArrowEvalPython`, que son cajas negras para el optimizador.
3. **¿Por qué una función por fila cuesta más que una vectorizada, y qué concuerda o discrepa con lo que midieron?** Según la guía de Arrow, la función por fila **serializa cada fila** hacia el proceso de Python y la invoca una vez por fila. La vectorizada transfiere **lotes columnares de Arrow** (10 000 filas por defecto) y opera con pandas sobre la serie completa. **Concuerda** con la condición B: en memoria, la función por fila tardó 0,736 s (1 002 240 invocaciones) y la vectorizada 0,306 s (104 lotes), 2,4 veces menos. **Discrepa** en dos puntos: la razón de tiempo (2,4) es muchísimo menor que la de invocaciones (unas 9 600), porque mover los datos entre la JVM y Python cuesta en los dos casos; y sobre el CSV la ventaja de la vectorizada no fue estable (en la ejecución final fue más lenta, 1,80 s frente a 1,48 s, con rangos solapados), porque la lectura domina.

### 7.3 Glosario español e inglés (10 términos)

| Término en inglés | Equivalente en español y definición de una línea (puede usar un ejemplo del acueducto) |
|---|---|
| *schema* | **Esquema:** los nombres y tipos de las columnas; el de la cruda tiene seis columnas, de `meter_id` a `quality_flag`. |
| *schema inference* | **Inferencia de esquema:** Spark deduce los tipos leyendo los datos; en la cruda cuesta 3 trabajos y el doble de MiB. |
| *declared schema* | **Esquema declarado:** los tipos fijados en el código (`definir_esquema`), un contrato versionado que no depende del contenido. |
| *DataFrame* | **DataFrame:** colección distribuida de filas con esquema, que Spark optimiza; `refinada_spark/` se construye con uno. |
| *column expression* | **Expresión de columna:** una operación declarativa sobre columnas, como `when(presion < 2.5, 'baja')`, que el optimizador entiende. |
| *user-defined function* | **Función definida por el usuario (función propia):** código de Python que Spark trata como caja negra, como `clasificar_udf_fila`. |
| *serialization* | **Serialización:** convertir filas en bytes para enviarlas a otro proceso; la función por fila serializa 1 002 240 filas hacia Python. |
| *vectorized* | **Vectorizado:** que opera sobre un lote completo de valores a la vez; la Pandas UDF procesa lotes de 10 000 presiones. |
| *cast* | **Conversión de tipo:** cambiar el tipo de un valor; convertir `consumo_m3` a entero deja nulos sin error en modo `PERMISSIVE`. |
| *timestamp* | **Marca temporal:** un instante en el tiempo; la capa la guarda en microsegundos ajustados a UTC (`INT64`). |

---

## 8. Nota para la gerencia (criterio 6 de la rúbrica · Power Humanise)

- **Ruta de la nota en el repositorio:** `T16_refinada_dataframes/nota_gerencia_t16.md` (el mismo texto de esta sección).

**Pregunta de la dirección que responde esta nota:** ¿podemos confiar en la nueva capa de lecturas para el cierre de facturación, y conviene usar programas propios para clasificar la presión del agua?

**Respuesta en una frase:** sí. La nueva capa cuadra al centavo con las tres referencias que ya teníamos, y la clasificación de presión debe hacerse con la herramienta que trae el motor, que da el mismo resultado y en nuestras mediciones fue entre 4 y 11 veces más rápida que un programa propio.

*Términos:* **capa refinada**: la versión limpia y ordenada de las lecturas, de donde salen los cierres. **Reconciliar**: comprobar, grupo por grupo, que dos fuentes independientes dan los mismos totales. **Esquema**: la lista de columnas con su tipo de dato (texto, número, fecha). **Función propia**: un programa escrito por el equipo, que el motor no sabe optimizar. **Partición por día**: guardar cada día en su propia carpeta, para poder rehacer un día sin tocar los demás.

| Cifra propia y de dónde sale (campo de la bitácora o tabla de la evidencia) | Qué significa para el negocio |
|---|---|
| **1 440 de 1 440** grupos iguales a la capa anterior y **480 de 480** iguales a los totales de control, con una diferencia máxima de 0,0 m³ (`detalle_heredada`, `detalle_control`) | El cierre que sale de la nueva capa es el mismo que el actual: no hay riesgo de facturar distinto por el cambio. |
| Reprocesar 6 días con el modo correcto conserva los **114 días restantes**; con el modo incorrecto se **pierden 547 200 lecturas** (tabla `Reproceso de 6 dias`) | Corregir un día con datos malos no pone en riesgo el resto del histórico: el error operativo más costoso queda bloqueado por diseño. |
| Clasificar la presión con la herramienta del motor: **0,066 s** frente a **0,736 s** con un programa propio por fila (11,2 veces), con resultados idénticos (`Variantes de clasificacion`) | Hoy son fracciones de segundo, pero el costo crece con cada medidor: con 100 000 medidores, el programa propio haría 2,4 millones de llamadas por cierre diario. |
| **0** filas con datos inválidos y **0** valores faltantes en 1 002 240 lecturas (`filas_no_conformes`, `nulos_por_columna`) | Si la fuente cambia de formato, el sistema lo detecta y lo cuenta antes de publicar, en lugar de producir un cierre equivocado sin aviso. |

**Recomendación:** adoptar la capa refinada con esquema declarado, partición por día y modo de reproceso que solo reemplaza los días corregidos, y clasificar la presión con la expresión nativa del motor. No se recomiendan funciones propias.

**Riesgo que se acepta:** las mediciones de tiempo se hicieron en un solo computador, con el motor en modo local; los tiempos absolutos pueden variar hasta 3 veces de una ejecución a otra. Decidimos por la diferencia que se repitió en todas las ejecuciones (la columna en memoria), no por una cifra aislada.

**Cifra que cambiaría la recomendación:** que una regla de negocio para clasificar no pudiera escribirse con la herramienta del motor (por ejemplo, un modelo estadístico externo). Entonces se usaría la función vectorizada, que en nuestras mediciones costó 4,6 veces la nativa, frente a las 11,2 veces de la función por fila.

**Lo que no sabemos todavía:** cómo se comporta el tiempo con 100 000 medidores reales, porque la cifra de 2,4 millones de llamadas es un cálculo lineal y no una medición; si la fuente real del acueducto cambia de formato con frecuencia; y cuánto cuesta operar esto en un clúster en lugar de una máquina.

**Dos preguntas difíciles de la dirección y su respuesta.**

1. **"¿Por qué confiar en tres corridas?"** No confiamos en una cifra aislada: en dos ejecuciones completas, el orden entre los dos programas propios **se invirtió** sobre el CSV (1,80 frente a 1,48 s en una; 0,84 frente a 0,94 s en la otra), y por eso no declaramos ganador entre ellos. Lo que sí se repitió en todas las ejecuciones, sin solape de rangos, es que la herramienta del motor es la más rápida, entre 4,4 y 11,2 veces con los datos en memoria. Con tres corridas no podemos dar un intervalo de confianza formal; para eso harían falta decenas de corridas.
2. **"¿Qué pasa si la fuente reordena las columnas un viernes?"** La lectura **falla y se detiene** ("CSV header does not conform") en lugar de cargar datos cruzados. En una prueba con dos columnas intercambiadas, sin esa protección, el consumo de un sector salió en 3 601,5 m³ en lugar de 190,7 m³, sin ningún error. El cierre de ese día se retrasa hasta corregir la fuente, pero nunca se publica un consumo 19 veces mayor. No sabemos con qué frecuencia cambia la fuente real.

**Términos técnicos usados y dónde se definen en la nota:** capa refinada, reconciliar, esquema, función propia y partición por día, en el párrafo "Términos", justo después de la respuesta. Modo local, en el párrafo de riesgo, explicado como "un solo computador".

---

## 9. Declaración de uso de asistentes de inteligencia artificial

Usamos **Claude Code (Anthropic)** como asistente en estas partes del trabajo:
- Escritura de las cuatro funciones de `pipeline/refinada_spark.py` y de las celdas "Su turno" del cuaderno, incluidas las opcionales del Nivel 3 y del reto.
- Redacción de las hipótesis, escritas antes de ejecutar las variantes, y de las conclusiones, escritas y luego corregidas con las cifras de la ejecución final.
- Primera redacción del registro de decisiones, del puente de modelado (a partir de los documentos T9 a T11 del repositorio `bigdata-ean-ideam`), del resumen en inglés, de las respuestas y del glosario, y de la nota para la gerencia.

Todas las cifras salen de ejecutar el cuaderno y los scripts en el contenedor `acueducto_spark_s15` (perfil `s16`) contra la ampliación `equipo-1`. La revisión de las cifras y de los textos es responsabilidad del autor.

---

## 10. Ampliaciones opcionales (si las abordaron)

- **Nivel 3 · evolución del esquema (celdas del Nivel 3 del cuaderno, sobre copias modificadas del día 2024-01-05):**
  - **Lo que hace la lectura declarada con `enforceSchema=False`:** con una columna nueva o una columna menos, **falla** (`IllegalArgumentException`: el encabezado tiene 7 o 5 columnas y el esquema 6). Con el orden distinto, **falla** ("CSV header does not conform"). Con un formato de fecha distinto **no falla**: deja 4 800 nulos en `timestamp`.
  - **Lo que hace con `enforceSchema=True` (el valor por omisión):** absorbe la columna nueva sin aviso; con una columna menos deja 4 800 nulos en `quality_flag`, y con el orden distinto suma 14 415,1 m³ en lugar de 768,6.
  - **Política elegida:** **fallar** ante un cambio de estructura del encabezado, que `clasificar_encabezado` detecta como `columna_nueva`, `columna_menos` u `orden_distinto`, y **poner en cuarentena** el día con nulos en `timestamp`: no se publica y se alerta.
- **Reto de diseño:** se completaron las funciones `invocaciones_por_cierre` y `extrapolar_segundos`. Un cierre de 100 000 medidores serían **2 400 000 invocaciones** con la función por fila frente a **240 lotes** con la vectorizada. No se grabó el video.

---

## Autoverificación antes de entregar

**Condiciones de aceptación.**

- [x] 1. Carga reejecutable: `refinada_spark/` se construye desde la cruda con esquema declarado, Parquet con marca temporal en microsegundos y partición por día con ceros, sin modificar la `refinada/` heredada; dos corridas por volumen dejan el mismo número de filas, las mismas sumas y el mismo hash, y reconcilian sin filtro de calidad, con tolerancia de $10^{-6}$, contra la heredada, `consolidada/` (base) y `totales_control.csv` (ampliación), con la diferencia máxima mostrada (secciones 2.1, 2.3 y 2.4).
- [x] 2. Reproceso de una ventana: el reproceso de 6 días se probó sobre una copia de la capa de la base, con sobrescritura estática y dinámica; la dinámica deja 576 000 filas y 114 de 120 particiones intactas, la estática deja 28 800 filas (sección 2.5).
- [x] 3. README: la etapa de la capa refinada se levantó desde un clon limpio siguiendo solo el README, que declara de dónde sale `datalake/` antes de los comandos y el identificador de equipo, y `verificar_etapa.py --etapa t16` terminó en `APROBADA` (secciones 5.4 y 5.5).

**Rúbrica (calidad de lo medido, de lo decidido, de lo documentado y de lo comunicado).**

- [x] Criterio 1: las cinco etapas tienen su salida registrada, la etapa de la capa refinada se levantó desde un clon limpio siguiendo solo el README, hay reejecución sin duplicados y una decisión revisada con la evidencia de hoy (sección 5).
- [x] Criterio 2: esquema justificado columna por columna, modo de lectura y filas no conformes, tipo físico y lógico con un lector independiente, reconciliación con las tres referencias, dos corridas con el mismo contenido, reproceso de una ventana, y `bitacora_refinada_spark.jsonl` con el identificador de equipo en cada línea y las corridas de variantes numeradas de 0 a 3 (sección 2).
- [x] Criterio 3: al menos cuatro decisiones con cifras propias, alternativa descartada y cifra de revisión; las tres variantes con el mismo conteo por clase, calentamiento descartado, tres corridas, mediana, mínimo y máximo en las dos condiciones; contraste del cierre diario con la línea base RDD; `evidencia_nivel2_equipo-1.md` versionado (secciones 3 y 4).
- [x] Criterio 4: el puente nombra T9 por separado de T10 y T11, incluye coincidencia, divergencia y una modificación "cambiaría X por Y porque Z" (sección 6).
- [x] Criterio 5: el resumen cubre los tres puntos, las respuestas remiten al texto y al menos una se contrasta con una cifra propia (sección 7).
- [x] Criterio 6: la nota define sus términos, traduce al menos tres cifras propias a consecuencias, recomienda, declara riesgo y cifra de cambio, dice qué no se sabe y responde dos preguntas difíciles (sección 8).
- [x] El producto está versionado en el repositorio en `T16_refinada_dataframes/`, y el pipeline completo en la raíz (`pipeline/`, `README.md`).

---

## Referencias

Apache Software Foundation. (s. f.). *Apache Arrow in PySpark*. En *PySpark documentation 3.5.3*. https://archive.apache.org/dist/spark/docs/3.5.3/api/python/user_guide/sql/arrow_pandas.html

Armbrust, M., Xin, R. S., Lian, C., Huai, Y., Liu, D., Bradley, J. K., Meng, X., Kaftan, T., Franklin, M. J., Ghodsi, A., y Zaharia, M. (2015). Spark SQL: Relational data processing in Spark. En *Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data* (pp. 1383-1394). ACM. https://doi.org/10.1145/2723372.2742797

Nygard, M. T. (2011, 15 de noviembre). *Documenting architecture decisions*. Cognitect. https://www.cognitect.com/blog/2011/11/15/documenting-architecture-decisions
