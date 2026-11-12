# T15 · La carga distribuida del acueducto que se puede repetir, reconciliar y justificar con lo que muestra la interfaz de Spark

**Equipo:** equipo-1 (trabajo individual)
**Integrantes:** Juan Pablo Castro
**Proyecto:** Plataforma de datos del acueducto (proyecto integrador)
**Identificador de equipo (`--equipo`):** `equipo-1`
**Entorno usado:** contenedor del perfil `s15` de Docker Compose (`acueducto_spark_s15`: Python 3.11, Java 11, PySpark 3.5.3, `local[4]` con 1 g, tope de 3 GiB). El cuaderno se ejecutó dentro del contenedor con `jupyter nbconvert --execute`.
**Fecha:** 2026-11-12

---

## 0. Perfil de la ampliación del equipo

- **Comando exacto usado para generar la ampliación, tiempo que tomó y dónde corrió:** la celda N2.1 del cuaderno, equivalente a `docker exec acueducto_spark_s15 python practica/generar_ampliacion_s15.py --equipo equipo-1` desde la carpeta del paquete. Tardó **3,5 s** en el contenedor, sobre una carpeta montada desde macOS.

| Valor del perfil | Cifra que reporta el generador |
|---|---|
| Medidores | 348 |
| Lecturas (total de la ampliación, no un incremento sobre la base) | 1 002 240 |
| Sector dominante | S-SUR (120 medidores; 76 en cada uno de los otros tres sectores) |
| Fracción $p$ de medidores del sector dominante | 0,3448 |
| Disco ocupado por `datalake_equipo/` (MiB) | 56,0 |

---

## 1. Carga reejecutable (condición de aceptación 1)

- **Ruta del script de la carga en el repositorio, modo de escritura y forma de la salida:** `T15_carga_spark/pipeline/carga_spark.py` escribe con `mode("overwrite")` y `coalesce(1)`, es decir, un solo archivo. Son 480 filas (unos 10 KiB). `partitionBy` por fecha produciría 120 carpetas con archivos de 4 filas, con más costo en metadatos que en datos, y `overwrite` reemplaza la salida completa en cada corrida.
- **Comandos usados (con `--equipo` en ambos):** `python pipeline/carga_spark.py --equipo equipo-1 --volumen base` y `python pipeline/carga_spark.py --equipo equipo-1 --volumen ampliacion`. Se ejecutaron dentro del contenedor, desde la celda N2.3 del cuaderno, con la función `main` del script.

| Volumen | Corrida | Filas de salida | `hash_salida` | Duplicados |
|---|---|---|---|---|
| Base común (`datalake/consolidada_spark/`) | 1 | 480 | `e3e31f0e1f783725…` | 0 |
| Base común | 2 | 480 | `e3e31f0e1f783725…` | 0 |
| Ampliación (`datalake_equipo/consolidada_spark/`) | 1 | 480 | `a675b5e7fff0cd84…` | 0 |
| Ampliación | 2 | 480 | `a675b5e7fff0cd84…` | 0 |

Hashes completos en `bitacora_carga_spark.jsonl`: base `e3e31f0e1f7837259597f405dba79dd9823680e272621dca227152016af82cd6` y ampliación `a675b5e7fff0cd84fc0a3eabfea9d021fe04fa2375940c778f175c00f4ecc9f0`.

**¿Las dos corridas de cada volumen dejan el mismo número de filas, las mismas sumas y el mismo hash?** Sí. En la base: 480 filas, 92 166,022 m³ y el mismo hash en las dos corridas. En la ampliación: 480 filas, 160 402,481 m³ y el mismo hash. Las dos salidas quedan en un solo archivo `part-*.parquet`. Además, el hash de la ampliación fue el mismo en las **19 corridas** de la bitácora: todos los valores de n, `groupByKey` y la ruta CSV.

**Por qué "misma salida" se entiende aquí como mismo contenido y no como mismos archivos, y qué pasaría con un anexado en lugar de `overwrite`:** Spark nombra cada archivo `part-00000-<uuid>.parquet` con un identificador nuevo en cada corrida, y el orden en que combina las particiones puede cambiar la última cifra de una suma en punto flotante. Por eso, "la misma salida" se define por su **contenido**: filas ordenadas por (fecha, sector) y consumo redondeado a 6 decimales. Eso es lo que compara el hash. Con `append`, la segunda corrida agregaría otras 480 filas: tendríamos 960 filas con 480 duplicados por (fecha, sector), y la tercera dejaría 1 440. Cada reintento cambiaría el total de consumo, que es justo el problema de las lecturas duplicadas de T12.

---

## 2. Reconciliación sin filtro de calidad (condición de aceptación 2)

| Volumen | Referencia | Filas obtenidas frente a esperadas | Lecturas por sector y día | Diferencia máxima en la suma de consumo | Resultado con tolerancia $10^{-6}$ por grupo |
|---|---|---|---|---|---|
| Base común | `consolidada/` | 480 frente a 480 | 1 200 en todos los grupos | 2,8 × 10⁻¹⁴ m³ | Cumple |
| Ampliación | `totales_control.csv` | 480 frente a 480 | Mínimo 1 824 y máximo 2 880 (S-SUR: 120 × 24 = 2 880; los demás: 76 × 24 = 1 824) | 0,0 m³ | Cumple |

**Línea del código que muestra que ningún agregado reconciliado lleva filtro de calidad:** `pipeline/carga_spark.py`, función `leer_refinada`: `df.selectExpr("date_format(timestamp_seconds(`timestamp` div 1000000000), 'yyyy-MM-dd') AS fecha", "sector", "consumo_m3")`. Lee la carpeta completa sin ningún `filter`/`where` sobre `quality_flag`; la columna ni siquiera se selecciona.

**Contraste con un agregado filtrado por `quality_flag` (que no reconcilia):**

| Volumen | Lecturas con `quality_flag = 'OK'` frente al total | ¿Reconcilia el agregado filtrado? | Por qué no |
|---|---|---|---|
| Base común (celda del sub-paso D.4) | 541 368 frente a 576 000 (93,99 %) | No: los 480 grupos tienen distinto número de lecturas y distinta suma | El filtro descarta el 6 % de las lecturas (ESTIMADO y otras banderas), así que cada grupo tiene menos lecturas y menos consumo que la referencia, que se calculó sin filtro. |
| Ampliación (celda de caché del Nivel 2) | 942 031 frente a 1 002 240 (94,0 %) | No, por la misma razón: `totales_control.csv` cuenta las 1 002 240 lecturas | Faltan 60 209 lecturas en el agregado filtrado. |

---

## 3. Bitácora y barrido de configuraciones (condición de aceptación 3; criterio: evidencia observada en la interfaz y experimentación reproducible)

- **Rutas en el repositorio de `bitacora_carga_spark.jsonl` y de `evidencia_nivel2_<equipo>.md`:** `T15_carga_spark/bitacora_carga_spark.jsonl` (19 líneas, todas con `"equipo": "equipo-1"`) y `T15_carga_spark/evidencia_nivel2_equipo-1.md`, que contiene la hipótesis y la conclusión de cada configuración.
- **Configuración común del barrido:** `master = local[4]`, `driver_memory = 1g`, `aqe = true`, método `reduceByKey`, volumen `ampliacion` y ruta `parquet_refinada`. Comando: `python pipeline/carga_spark.py --equipo equipo-1 --volumen ampliacion --metodo reduceByKey --particiones-mezcla n`, con n ∈ {4, 8, 50, 200}, tres corridas cada uno (celda N2.4).

### 3.1 Barrido de particiones de mezcla (al menos tres valores distintos de `--particiones-mezcla`, tres corridas cada uno)

| # | `particiones_mezcla` | `particiones_entrada` | Etapas | Tareas | Bytes de mezcla (escritos) | Tiempo mediana (s) | Tiempo mín. y máx. (s) |
|---|---|---|---|---|---|---|---|
| 1 | 4 | 4 | 2 | 8 | 9 822 | 2,59 | 2,58 y 2,60 |
| 2 | 8 | 4 | 2 | 12 | 11 890 | 3,14 | 2,90 y 3,49 |
| 3 | 50 | 4 | 2 | 54 | 26 485 | 4,04 | 3,96 y 4,09 |
| 4 | 200 | 4 | 2 | 204 | 41 318 | 7,24 | 6,83 y 7,28 |

**Verificación de las tareas totales:** se cumple en las cuatro configuraciones: 4 + 4 = 8, 4 + 8 = 12, 4 + 50 = 54 y 4 + 200 = 204. La autoverificación del cuaderno lo comprueba con un `assert`.

| Configuración | Hipótesis (escrita antes de medir) | Pestaña de la interfaz de la que sale la cifra | Conclusión (lo observado, y por separado lo interpretado; si la hipótesis se refuta, dígalo) |
|---|---|---|---|
| 1 (n = 4) | 8 tareas en 2 etapas. Con `reduceByKey`, cada partición de entrada combina antes de mezclar, así que los bytes de mezcla serán de 10 a 20 KB (unos 480 registros), iguales a los de la base aunque haya 1,7 veces más lecturas. Será la más rápida: una ola de 4 tareas por etapa. | Stages (Tasks, Shuffle Write y Shuffle Read) y Jobs (Duration) | **Observado:** 8 tareas, 9 822 B y 2,6 s de mediana, el menor tiempo. **Interpretado:** se confirma. Los bytes son prácticamente los de la base (9 803 B): el combinador reduce 1 002 240 lecturas a un registro por clave y por partición. |
| 2 (n = 8) | 12 tareas. Bytes iguales a n = 4, porque dependen de las claves que salen de cada partición y no de n. Tiempo casi igual. | Stages y Jobs | **Observado:** 12 tareas, 11 890 B (+21 %) y 3,1 s de mediana (+0,5 s). **Interpretado:** la hipótesis de tareas se confirma, pero la de bytes **se refuta en parte**. Los bytes crecen con n porque cada tarea de mapa escribe un bloque por partición de mezcla (4 × 8 = 32), cada uno con su propio encabezado de serialización y compresión. |
| 3 (n = 50) | 54 tareas, bytes iguales o apenas mayores y un tiempo algo mayor por el costo de planificar 50 tareas de unas 10 claves. | Stages y Jobs | **Observado:** 54 tareas, 26 485 B (2,7 veces n = 4) y 4,0 s (+56 %). **Interpretado:** se confirma el costo de planificación. La hipótesis de bytes subestimó el costo fijo de 4 × 50 = 200 bloques. |
| 4 (n = 200) | 204 tareas, unas 2,4 claves por tarea y muchas casi vacías. El mayor tiempo del barrido (+0,5 a 1 s). | Stages y Jobs | **Observado:** 204 tareas, 41 318 B (4,2 veces) y 7,2 s, casi el triple que n = 4, con rangos que no se solapan. **Interpretado:** sobre-particionado. El tiempo creció mucho más de lo estimado: unos 25 ms por tarea en `local[4]`, que es costo puro de planificación. |

### 3.2 Tarea que lee más bytes (`groupByKey`, clave solo sector, `numPartitions=8`)

| Tarea (índice) | Bytes leídos (Shuffle Read Size) | Sector que recibe | Tiempo de la tarea (ms) |
|---|---|---|---|
| 7 | 1 113 066 | S-SUR | 139 |
| 1 | 705 753 | S-NORTE | 173 |

Las tareas 2 (S-OESTE, 706 246 B, 288 ms) y 3 (S-ESTE, 706 272 B, 172 ms) leen casi lo mismo que la 1. Las tareas 0, 4, 5 y 6 no reciben datos.

- **Causa de que esa tarea lea más bytes, explicada con el perfil de su ampliación, y razón entre la mayor y la menor tarea con datos:** S-SUR es el sector dominante de nuestra ampliación, con 120 de 348 medidores (p = 0,345), frente a 76 en cada uno de los otros tres. `groupByKey` mezcla **cada lectura** individual, así que los bytes de la tarea son proporcionales a las lecturas del sector: 120 × 24 × 120 = 345 600 lecturas de S-SUR frente a 218 880 de cada otro sector. Razón: 1 113 066 / 705 753 = **1,58**, que coincide con 120 / 76 = 1,58.
- **Qué se ve con `reduceByKey` y la misma clave, y por qué:** cada tarea con datos lee unos **364 B** (360 a 364), sin diferencia entre S-SUR y los demás. El combinador del lado del mapa reduce cada sector a un solo par (suma, cuenta) por partición de entrada, así que lo que viaja es una cantidad fija de registros por sector y el sesgo de volumen desaparece.
- **Si la tarea con más bytes no coincide con la más lenta, qué implica para elegir el tiempo o los bytes como señal:** no coinciden. La más lenta fue la 2 (S-OESTE, 288 ms) y la de más bytes, la 7 (139 ms). En la primera corrida, la más lenta fue la 1. Con tareas de 100 a 300 ms, el ruido de planificación y de los procesos de Python domina sobre el volumen, y la tarea más lenta cambia de una corrida a otra. Los bytes, en cambio, son idénticos en las dos corridas. Por eso la señal para diagnosticar sesgo es **Shuffle Read Size**, y el tiempo es solo evidencia complementaria.

### 3.3 Contraste con el `groupBy` de DataFrame (solo si lo usaron en el barrido)

| Ejecución adaptativa | `spark.sql.shuffle.partitions` | Tareas de la etapa de reducción | Bytes de mezcla |
|---|---|---|---|
| Activa | 200 | 1 | 38 255 |
| Desactivada | 200 | 200 | 38 255 |
| Desactivada | 8 | 8 | 13 373 |

**Qué se configura frente a qué se ejecuta:** nuestro barrido usó la API de RDD (`reduceByKey`), donde lo configurado es lo ejecutado. La celda N2.6 se ejecutó de todos modos como contraste. Con AQE activa, Spark mide las 200 particiones de mezcla, ve que son diminutas y las fusiona en **1 tarea**: se configuran 200 y se ejecuta 1. Con AQE desactivada se ejecutan exactamente las 200 configuradas.

---

## 4. Registro de decisiones corto (condición de aceptación 4; criterio: calidad de las decisiones documentadas)

- **Ruta del registro de decisiones en el repositorio y ubicación de las capturas:** este documento, sección 4. Las cifras están escritas y salen de `bitacora_carga_spark.jsonl`, `evidencia_nivel2_equipo-1.md` y las salidas del cuaderno ejecutado. No usamos capturas.

### ADR-1 · Número de particiones (obligatoria)

- **Estado:** aceptada
- **Contexto:** cuántas particiones de mezcla usa la carga de la ampliación de `equipo-1` (1 002 240 lecturas en 120 Parquet, 56 MiB y 480 claves de salida). Pregunta: ¿el valor por omisión (las 4 de entrada) basta, o conviene subirlo "por si acaso", como los 200 del `spark.sql.shuffle.partitions` por omisión?
- **Opciones:** n = 4, 8, 50 y 200, con tres corridas cada una.
- **Evidencia (pestaña y cifras de la interfaz, de su ampliación):** Stages: 8, 12, 54 y 204 tareas, con 9 822, 11 890, 26 485 y 41 318 B de mezcla. Jobs: 2,59, 3,14, 4,04 y 7,24 s de mediana. El hash es idéntico en las 12 corridas.
- **Decisión:** **n = 4** (igual a las particiones de entrada y a las ranuras de `local[4]`). Vale mientras la salida tenga del orden de cientos o miles de claves y la máquina tenga 4 ranuras.
- **Alternativa descartada y su costo:** n = 200 cuesta 196 tareas más, 4,2 veces los bytes de mezcla (+31 496 B) y **+4,6 s por corrida** (de 2,6 a 7,2 s: casi el triple), sin ningún beneficio, porque el resultado es el mismo hash. n = 8 cuesta +0,5 s y +21 % de bytes, una diferencia que está dentro del ruido.
- **Consecuencias:** gana la corrida más corta y el menor costo de mezcla. Arriesga que, si una partición de mezcla recibe demasiadas claves pesadas, 4 tareas no basten para repartir la carga.
- **Cifra que obligaría a revisarla:** que una tarea de reducción lea más de unos 128 MiB (Shuffle Read Size), o que la salida pase de unas 100 000 claves. En ese punto convendría n ≈ Shuffle Read total / 128 MiB.

### ADR-2 · Operación de agregación (`groupByKey`, `reduceByKey` u otra)

- **Estado:** aceptada
- **Contexto:** con qué operación se suma el consumo y se cuentan las lecturas por (fecha, sector) sobre las 1 002 240 lecturas de la ampliación.
- **Opciones:** `reduceByKey` con el par (suma, cuenta) y `groupByKey` seguido de suma.
- **Evidencia (pestaña y cifras de la interfaz, de su ampliación):** Stages: `reduceByKey` escribe **9 822 B** de mezcla y `groupByKey`, **3 240 135 B** (329,9 veces más), con el mismo hash. Con la clave sector y 8 particiones, `groupByKey` deja tareas de 0,7 a 1,1 MB con sesgo de 1,58, y `reduceByKey`, tareas de unos 364 B sin sesgo.
- **Decisión:** **`reduceByKey`**. Es válido porque combinar (a₁, n₁) con (a₂, n₂) en (a₁ + a₂, n₁ + n₂) es asociativo y conmutativo.
- **Alternativa descartada y su costo:** `groupByKey` mezcla 3,2 MB en lugar de 9,8 KB, y ese volumen crece linealmente con las lecturas. En la base mezclaba 1,87 MB: con 1,74 veces más lecturas pasó a 3,24 MB. Además, hereda el sesgo del sector dominante (la tarea de S-SUR lee 1,58 veces más). En una sola máquina el tiempo casi no cambia (3,09 s frente a 3,28 s), pero en un clúster esos bytes viajarían por la red.
- **Consecuencias:** la mezcla deja de depender del volumen y del sesgo. A cambio, solo sirve para agregados que se puedan combinar por partes: una mediana, por ejemplo, no se podría calcular así.
- **Cifra que obligaría a revisarla:** que el negocio pida un estadístico no asociativo (mediana o percentiles exactos por sector y día).

### ADR-3 · Uso de caché

- **Estado:** aceptada
- **Contexto:** si la carga debe cachear las lecturas leídas de la ampliación (942 031 lecturas `OK`, 1 002 240 en total).
- **Opciones:** sin caché, o `cache()` (`MEMORY_ONLY` para un RDD).
- **Evidencia (pestaña y cifras de la interfaz, de su ampliación):** con tres acciones sobre el mismo RDD, sin caché tardaron 3,07 + 3,16 + 3,02 = **9,2 s**. Con caché tardaron 3,89 + 0,29 + 0,30 = **4,5 s**. En Storage, la caché ocupó **16,4 MiB** de los 434,4 MiB disponibles (4 de 4 particiones, 0 B en disco).
- **Decisión:** **no cachear** en la carga. La carga ejecuta **una sola acción** (`collect()`), y la primera acción con caché fue **0,8 s más lenta** que sin caché (3,89 s frente a 3,07 s), porque además de calcular escribe la caché.
- **Alternativa descartada y su costo:** cachear cuesta +0,8 s en la única acción y 16,4 MiB de memoria de almacenamiento retenidos sin reutilización.
- **Consecuencias:** la carga queda más simple y no presiona la memoria. Si un proceso posterior reutilizara el mismo RDD en varias acciones (por ejemplo, varios agregados de calidad), la decisión cambia.
- **Cifra que obligaría a revisarla:** dos o más acciones sobre el mismo RDD en la misma sesión. Con nuestras cifras, la caché paga desde la segunda acción, porque ahorra unos 2,8 s por acción.

### ADR-4 · Formato de entrada

- **Estado:** aceptada
- **Contexto:** leer la capa refinada (Parquet) o la cruda (CSV) de la ampliación.
- **Opciones:** `parquet_refinada` (ruta principal) y `csv_contraste` (`textFile` sobre los 120 CSV).
- **Evidencia (pestaña y cifras de la interfaz, de su ampliación):** Stages y Jobs: Parquet da 4 particiones de entrada, **8 tareas**, 9 822 B de mezcla y **3,28 s**. El CSV da 120 particiones (una por archivo), **240 tareas** (30 veces más), 50 501 B de mezcla y **8,25 s**. Las dos rutas dan el mismo hash.
- **Decisión:** **Parquet de la refinada.** Es columnar (solo se leen `timestamp`, `sector` y `consumo_m3`), viene tipado y Spark empaqueta los 120 archivos en 4 particiones.
- **Alternativa descartada y su costo:** el CSV cuesta 2,5 veces el tiempo (+5 s), 30 veces las tareas y 5 veces los bytes de mezcla: con 120 particiones de mapa, cada una emite sus parciales por clave. Además obliga a descartar a mano los 120 encabezados.
- **Consecuencias:** dependemos de que la capa refinada exista y esté actualizada antes de la carga.
- **Cifra que obligaría a revisarla:** que la refinada se retrase respecto de la cruda, o que la cruda llegue en pocos archivos grandes y no en 120 pequeños.

### ADR-5 · Tamaño de ejecutores y núcleos (en local: `local[N]` y memoria del controlador)

- **Estado:** aceptada
- **Contexto:** con cuántas ranuras y cuánta memoria corre la carga dentro del contenedor de 3 GiB.
- **Opciones:** `local[4]` con 1 g, o `local[*]` (6 núcleos visibles con `nproc`) o 2 g.
- **Evidencia (pestaña y cifras de la interfaz, de su ampliación):** Executors: 1 ejecutor (`driver`), 4 ranuras y **434,4 MiB** de memoria de almacenamiento. Storage: la caché más grande usó 16,4 MiB (3,8 % de esa memoria). `docker stats`, muestreado cada segundo durante todo el cuaderno: pico de **2,19 GiB de 3 GiB**.
- **Decisión:** **`local[4]` con 1 g.** Las 4 particiones de entrada ocupan exactamente 4 ranuras.
- **Alternativa descartada y su costo:** `local[*]` abre 6 ranuras y 6 procesos de Python (unos 90 MiB cada uno, +180 MiB) sin trabajo para las 2 ranuras extra. Con el pico medido de 2,19 GiB, eso deja unos 0,6 GiB de margen. 2 g sube el montón en 1 024 MiB y, con el mismo pico, superaría los 3 GiB del contenedor (`OOMKilled`).
- **Consecuencias:** queda un margen de unos 0,8 GiB bajo el tope del contenedor. Si una etapa necesitara más de 434 MiB de almacenamiento, Spark tendría que recalcular particiones.
- **Cifra que obligaría a revisarla:** un pico en `docker stats` superior a unos 2,7 GiB, o una caché en Storage que no quepa completa (menos de 4 de 4 particiones en memoria).

**Decisiones justificadas en total (mínimo tres, una de ellas la ADR-1):** 5

---

## 5. Contraste con la solución de referencia de la sesión anterior (criterio: contraste con la solución de referencia y documentación)

| Decisión de su memo | En la solución de referencia | Qué mantienen o coincide | Qué cambiarían y por qué |
|---|---|---|---|
| Nomenclatura de las tablas | Nombres predecibles para que las etapas siguientes los consuman (`maestro_suscriptores`, `stg_cambios_suscriptor`, `estado_cdc_suscriptores`) | **Coincide**: usamos los tres nombres exactos | Nada: las etapas siguientes (incluida esta carga) dependen de nombres predecibles. |
| Valor que se guarda como punto de control (checkpoint) | El mayor `updated_at` leído, con un centinela antiguo en la primera corrida | **Coincide** en el valor y en el centinela (2000-01-01) | Agregaríamos `GREATEST(checkpoint, :nuevo)` para que nunca retroceda: en nuestra T14, restablecer el checkpoint duplicó 21 filas. |
| Frontera de la transacción | Cambios y punto de control en una sola transacción | **Coincide** | Nada. Es el mismo principio de T12 (datos y marca juntos) y de esta carga (`overwrite`, que deja la salida completa o no la deja). |
| Método de captura | Consulta por marca temporal, con reconciliación periódica de llaves para las bajas | **Coincide** en el patrón híbrido: consulta horaria más reconciliación cada 6 h | Nada en el método. Lo justificamos con nuestras cifras: 11 bajas de 32 cambios y 420 llaves al día. |
| Punto ciego de las bajas duras | Se mide, no se supone: bajas generadas frente a bajas capturadas | **Coincide**: medimos 0 de 11 y la reconciliación encontró 11 de 11 | Agregaríamos la verificación de control del memo: el conteo del destino debe ser igual al del origen después de cada barrido. |

---

## 6. Repositorio e instrucciones de reproducción (condición de aceptación 5; criterio: contraste con la solución de referencia y documentación)

- **Enlace al repositorio, identificador del último commit que quieren que se valore y ruta del README:** ver el cuadro de entrega en Canvas (enlace y commit). README: `README.md` en la raíz del repositorio. Esta tarea está en `T15_carga_spark/`.
- **Pasos de reproducción:**
  1. Extraer `BiD_S15_proyecto_base_v1.zip` en `~/proyectos/s15` y copiar encima `T15_carga_spark/pipeline/carga_spark.py` (reemplaza el esqueleto) y `T15_carga_spark/BiD_S15_P4_cuaderno_v1.ipynb` (va en `practica/`).
  2. Apagar el perfil anterior y levantar el de Spark: `docker compose --profile s12 down`, luego `docker compose --profile s15 build` (solo la primera vez), luego `docker compose --profile s15 up -d`. Esperar a que `acueducto_spark_s15` esté `healthy`.
  3. Regenerar la ampliación del equipo: `docker exec acueducto_spark_s15 python practica/generar_ampliacion_s15.py --equipo equipo-1`. Debe imprimir `Equipo equipo-1: 348 medidores, 120 dias, 1,002,240 lecturas` y `Sector dominante S-SUR: 120 medidores (34.5%)`.
  4. Carga sobre cada volumen, dos veces: `docker exec acueducto_spark_s15 python pipeline/carga_spark.py --equipo equipo-1 --volumen base` y `docker exec acueducto_spark_s15 python pipeline/carga_spark.py --equipo equipo-1 --volumen ampliacion`. Cada corrida imprime su registro JSON y anexa una línea a `bitacora_carga_spark.jsonl`.
  5. Barrido: `docker exec acueducto_spark_s15 python pipeline/carga_spark.py --equipo equipo-1 --volumen ampliacion --particiones-mezcla 4`, y lo mismo con 8, 50 y 200, tres veces cada uno. Las cifras se leen en `bitacora_carga_spark.jsonl` (campos `tareas`, `mezcla_escritura_bytes` y `tiempo_s`). Alternativa: ejecutar todo el cuaderno con `docker exec -w /proyecto/practica acueducto_spark_s15 jupyter nbconvert --to notebook --execute --inplace BiD_S15_P4_cuaderno_v1.ipynb` (unos 4 minutos), que hace los pasos 3 a 5 y genera `evidencia_nivel2_equipo-1.md`.
  6. Salida esperada: `"reconciliacion": "OK"` y `480 grupos iguales a la referencia (tolerancia 1e-06)`, con hash `e3e31f0e…6cd6` en la base y `a675b5e7…c9f0` en la ampliación. Las tareas y los bytes deben coincidir con la tabla 3.1; los tiempos pueden variar hasta ×3.
- **Qué se versiona y qué no:** **no** se versionan el `.zip` ni las capas `cruda/` y `refinada/`, ni la ampliación completa (`datalake_equipo/`, que se regenera con el paso 3). **Sí** se versionan `pipeline/carga_spark.py`, el cuaderno ejecutado, `bitacora_carga_spark.jsonl`, `evidencia_nivel2_equipo-1.md`, `mediciones_nivel1.csv`, `datalake/consolidada_spark/` y `datalake_equipo/consolidada_spark/` (un Parquet de 480 filas cada uno), y `datalake_equipo/parametros_equipo.json`.

---

## 7. Lectura en inglés (condición de aceptación 6)

### 7.1 Resumen técnico en inglés (de 150 a 200 palabras)

Zaharia et al. (2016) present Apache Spark as a unified engine that replaces the collection of specialized systems that grew around MapReduce. Its core abstraction is the Resilient Distributed Dataset (RDD): an immutable, partitioned collection built through coarse-grained transformations such as map, filter and join. Transformations are lazy; nothing runs until an action, such as count or collect, asks for a result, which lets Spark plan the whole graph, pipeline narrow operations inside one stage and avoid materializing intermediate data. Users can persist an RDD in memory, which makes iterative algorithms and interactive queries far faster than reloading data from disk on every pass. Fault tolerance comes from lineage instead of replication: Spark records how each partition was derived and recomputes only the lost partitions; shuffle outputs are kept on local disk so that a failure does not force recomputation from the start. Because SQL, streaming, machine learning and graph processing are built as libraries on the same engine, they share data in memory and can be combined in a single program, with performance comparable to specialized systems. The authors argue that this generality reduces cost and complexity for big data applications.

**Número de palabras del resumen:** 192

### 7.2 Respuestas a las cinco preguntas guía

1. **¿Qué extensión del modelo de MapReduce introducen los RDD y qué cargas de trabajo permiten cubrir que antes requerían motores separados?** MapReduce solo comparte datos entre pasos escribiéndolos en el almacenamiento distribuido. Los RDD agregan una abstracción de **datos compartidos en memoria** entre operaciones, con transformaciones de grano grueso y la posibilidad de persistir resultados intermedios. Eso permite cubrir en un solo motor los algoritmos iterativos (aprendizaje automático, grafos), las consultas interactivas (SQL), el procesamiento de flujos en microlotes y el ETL por lotes, que antes requerían Pregel, Impala, Storm y otros motores separados.
2. **¿Qué diferencia hay entre una transformación y una acción? ¿Qué ventaja menciona el artículo de que Spark evalúe de forma perezosa?** Una **transformación** (`map`, `filter`, `reduceByKey`) define un RDD nuevo a partir de otro, pero no calcula nada. Una **acción** (`count`, `collect`, `save`) pide un resultado y dispara la ejecución. La evaluación perezosa le permite a Spark ver el grafo completo antes de ejecutar y optimizarlo: encadenar operaciones estrechas en una misma etapa sin materializar datos intermedios. Lo vimos en el cuaderno: tras definir dos transformaciones, la interfaz mostraba 0 trabajos, y el `count()` lanzó 1 trabajo con 4 tareas.
3. **¿Cómo recupera Spark una partición perdida sin replicar los datos? ¿Qué hace con la salida de una operación de mezcla para este fin?** Con el **linaje** (*lineage*): cada RDD recuerda las transformaciones que lo produjeron a partir de datos estables, así que una partición perdida se recalcula aplicando de nuevo esas transformaciones solo sobre las particiones de origen necesarias. La salida de cada mezcla se **conserva en el disco local** de los nodos que la escribieron. Si falla una tarea de la etapa siguiente, se vuelve a leer esa salida en lugar de recalcular toda la etapa de mapa.
4. **El artículo dice que, si los datos persistidos no caben en memoria, Spark los derrama a disco. Contrasten esa afirmación con lo que vieron en la pestaña Storage: ¿qué ocurre con un RDD en `MEMORY_ONLY` y qué con un DataFrame?** Esa afirmación vale para los niveles de persistencia que incluyen disco. Un RDD con `cache()` usa **`MEMORY_ONLY`**: las particiones que no caben **no se derraman a disco**, simplemente no se guardan y se recalculan desde el linaje cada vez que se necesitan. En la pestaña Storage, una caché que no cabe se ve con menos particiones en caché que el total. La nuestra mostró 4 de 4 particiones, 9,4 MiB en la base y 16,4 MiB en la ampliación, con 0 B en disco, porque cabía de sobra en los 434,4 MiB. Un **DataFrame** con `cache()` usa por omisión `MEMORY_AND_DISK`: lo que no cabe en memoria sí se derrama a disco, y eso es lo que describe el artículo.
5. **¿Qué ventaja ofrece que las bibliotecas de nivel superior corran sobre el mismo motor?** Pueden **combinarse en un mismo programa y compartir datos en memoria** sin exportarlos entre sistemas. Por ejemplo, una consulta SQL que prepara los datos y un algoritmo de aprendizaje automático que los consume. Además, todas heredan las optimizaciones y la tolerancia a fallos del motor, y la organización opera, aprende y despliega un solo sistema en lugar de varios.

### 7.3 Glosario español e inglés (al menos 10 términos, cada uno con un ejemplo del acueducto en la definición si lo desean)

| Término en español | Término en inglés |
|---|---|
| Conjunto de datos distribuido resiliente (las 1 002 240 lecturas de la ampliación como RDD) | Resilient Distributed Dataset (RDD) |
| Transformación (`mapValues` que convierte cada consumo en el par (consumo, 1)) | Transformation |
| Acción (el `collect()` que trae los 480 agregados al controlador) | Action |
| Evaluación perezosa (0 trabajos tras definir las transformaciones) | Lazy evaluation |
| Linaje (la cadena Parquet → selectExpr → mapValues → reduceByKey) | Lineage |
| Partición (las 4 particiones de entrada de los 120 Parquet) | Partition |
| Mezcla (los 9 822 B que mueve `reduceByKey` entre etapas) | Shuffle |
| Etapa (las 2 etapas, mapa y reducción, de cada carga) | Stage |
| Tarea (las 204 tareas con 200 particiones de mezcla) | Task |
| Dependencia estrecha / amplia (`filter` por `quality_flag` frente a `reduceByKey`) | Narrow / wide dependency |
| Persistencia en memoria (los 16,4 MiB en Storage) | In-memory persistence / caching |
| Tolerancia a fallos (recalcular solo la partición perdida) | Fault tolerance |
| Combinador del lado del mapa (la suma parcial por sector antes de mezclar) | Map-side combiner |
| Sesgo de datos (S-SUR con 1,58 veces más bytes por tarea) | Data skew |

---

## 8. Declaración de uso de asistentes de inteligencia artificial

Usamos **Claude Code (Anthropic)** como asistente en estas partes del trabajo:
- Escritura de las tres funciones de `pipeline/carga_spark.py` y de las celdas "Su turno" del cuaderno.
- Redacción de las hipótesis, que se escribieron en el cuaderno antes de ejecutar el barrido, y de las conclusiones, escritas después a partir de las cifras medidas.
- Primera redacción del registro de decisiones, del resumen en inglés, de las respuestas a las preguntas guía y del glosario.

Todas las cifras salen de ejecutar el cuaderno y el script en el contenedor `acueducto_spark_s15` contra la ampliación `equipo-1`. La revisión de las cifras y de los textos es responsabilidad del autor.

---

## 9. Nivel 3 y reto de diseño, si los abordaron (opcional)

- **Nivel 3 · carga nocturna de la capa cruda de la ampliación (120 CSV pequeños):**
  - **Lecturas medidas:** `textFile` da 120 particiones, 120 tareas y 4,5 s (con 1 002 360 líneas, incluidos los 120 encabezados). El DataFrame CSV sin inferir da 5 particiones, 127 tareas y 2,4 s. Con `inferSchema` son 5 particiones, 132 tareas y 3,1 s (lanza un trabajo extra para inferir). Parquet de la refinada: 4 particiones, 6 tareas y 1,3 s.
  - **Decisión para la carga nocturna desde la cruda:** DataFrame CSV con **esquema declarado** (sin `inferSchema`), que da 5 particiones y evita el trabajo extra de inferencia, `reduceByKey` o `groupBy` con 4 particiones de mezcla y sin caché, porque hay una sola acción.
  - **Presupuesto de memoria:** el cálculo del cuaderno da 1 024 (montón) + 300 (JVM fuera del montón) + 4 × 90 (Python) + 133 (núcleo de Jupyter) = **1 817 MiB**, con un margen de 1 255 MiB. Pero el pico observado en `docker stats` fue de **2,19 GiB (unos 2 240 MiB)**. La brecha de unos 420 MiB es memoria que el presupuesto no contaba: la caché de páginas de los archivos leídos y escritos, que el contenedor cuenta como suya, los búferes de Arrow y de pandas en las celdas que leen Parquet, y los procesos de Python de la sesión anterior que siguen vivos mientras se abre la siguiente. El presupuesto corregido es de unos 2 240 MiB, con un **margen real de unos 830 MiB**.
  - **Alternativa descartada:** `textFile`, que cuesta 30 veces las tareas y obliga a descartar a mano los encabezados.
- **Reto de diseño · dimensionar el procesamiento de 7,8 GB:**
  - **Particiones:** P = ⌈7,8 × 1 024 / 128⌉ = **63** particiones con bloques de 128 MB.
  - **Olas:** con E = 2 ejecutores de c = 4 núcleos, W = ⌈63 / 8⌉ = **8 olas**.
  - **Memoria por tarea:** con H = 4 096 MiB, (4 096 − 300) × 0,6 / 4 = **569,4 MiB por tarea**.
  - **Factor de expansión:** como supuesto, k = 3 (un bloque Parquet comprimido de 128 MB se expande a unos 384 MB en memoria al deserializarse en filas). Eso cabe en 569 MiB por tarea.
  - **Postura:** **una máquina única** basta para 7,8 GB, porque son 8 olas de unos segundos cada una. El umbral de escala estaría cuando k × 128 MB supere la memoria por tarea (k > 4,4) o cuando el tiempo total de las olas supere la ventana nocturna. Antes de eso, un clúster solo agrega costo de red en la mezcla.

---

## Autoverificación antes de entregar

**Condiciones de aceptación.**

- [x] La carga es reejecutable: dos corridas por volumen dejan el mismo número de filas, las mismas sumas y el mismo hash (suma redondeada a 6 decimales), con `overwrite` y sin duplicados (sección 1).
- [x] La reconciliación es sin filtro de calidad y cumple la tolerancia de $10^{-6}$ por grupo contra `consolidada/` (base, con 1 200 lecturas por sector y día) y contra `totales_control.csv` (ampliación) (sección 2).
- [x] `bitacora_carga_spark.jsonl` tiene los campos que escribe el script, el identificador de equipo en cada línea y al menos tres configuraciones distintas del barrido; el archivo `evidencia_nivel2_equipo-1.md` está versionado (sección 3).
- [x] El registro de decisiones tiene al menos tres decisiones, una de ellas el número de particiones, cada una con evidencia de la interfaz, opciones, una alternativa descartada con su costo, consecuencias y la cifra que obligaría a revisarla, y no descalifica la decisión del equipo por diferir de la de referencia (sección 4).
- [x] El repositorio lleva el identificador de equipo y las instrucciones para reproducir la corrida, incluido cómo se regenera `datalake_equipo/` (sección 6).
- [x] La lectura está entregada: resumen de 150 a 200 palabras en inglés, respuestas a las cinco preguntas y glosario de al menos 10 términos (sección 7).

**Rúbrica (calidad de lo medido, de lo decidido y de lo documentado).**

- [x] Las secciones 1 y 2 muestran las dos corridas de cada volumen, la diferencia máxima observada, por qué "misma salida" es mismo contenido y el contraste con un agregado filtrado que no reconcilia.
- [x] La sección 3 tiene hipótesis escrita antes de medir, cifras con su pestaña de origen, tiempo con mediana, mínimo y máximo, y conclusión por configuración, y explica con bytes por tarea la tarea que lee más.
- [x] La sección 4 justifica con cifras propias al menos tres decisiones (ADR), con alternativa descartada, consecuencias y cifra de revisión.
- [x] La sección 7 muestra vocabulario técnico preciso y respuestas apoyadas en el artículo y en lo observado en la interfaz.
- [x] Las secciones 5 y 6 contrastan las cinco decisiones con la solución de referencia (también si coinciden) y permiten que otra persona repita la corrida sin preguntar nada.
- [x] El producto está versionado en el repositorio en `T15_carga_spark/`.

---

## Referencias

Apache Software Foundation. (s. f.). *Web UI*. En *Apache Spark 3.5.3 documentation*. https://archive.apache.org/dist/spark/docs/3.5.3/web-ui.html

Nygard, M. T. (2011, 15 de noviembre). *Documenting architecture decisions*. Cognitect. https://www.cognitect.com/blog/2011/11/15/documenting-architecture-decisions

Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A., Meng, X., Rosen, J., Venkataraman, S., Franklin, M. J., Ghodsi, A., Gonzalez, J., Shenker, S., y Stoica, I. (2016). Apache Spark: A unified engine for big data processing. *Communications of the ACM, 59*(11), 56-65. https://doi.org/10.1145/2934664
