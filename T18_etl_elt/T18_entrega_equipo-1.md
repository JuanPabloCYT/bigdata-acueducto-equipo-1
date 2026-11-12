# T18 · Del trabajo heredado del tablero a capas con contratos: dónde se transforma el dato del acueducto, cuánto cuesta cada camino y cómo se le explica a quien consume la cifra

**Equipo:** equipo-1 (trabajo individual)
**Integrantes:** Juan Pablo Castro
**Proyecto:** Plataforma de datos del acueducto (proyecto integrador)
**Identificador de equipo (`--equipo`):** `equipo-1`
**Entorno usado:** contenedor del servicio `spark` con el perfil `s18` de Docker Compose (`acueducto_spark_s15`, Spark 3.5.3, `local[4]` con 1 g). El cuaderno se ejecutó dentro del contenedor con `jupyter nbconvert --execute`.
**Punto de partida:** `BiD_S18_proyecto_base_v1.zip`, con **nuestras** soluciones de las etapas anteriores en lugar de las de referencia del paquete: `pipeline/carga_spark.py` (T15), `pipeline/refinada_spark.py` (T16) y `pipeline/planes_s17.py` (T17).
**Fecha de entrega:** 2026-11-12

---

## 0. Perfil de la ampliación y entorno

- **Línea que imprime el generador de la ampliación:** `Equipo equipo-1: 348 medidores, 120 dias, 1,002,240 lecturas`
- **Lecturas guardadas de su ampliación, archivos de su capa de preparación y su tamaño total en bytes:** 1 002 240 lecturas, 120 archivos y 4 519 275 B.
- **Ventana de reproceso del equipo:** del **2024-01-09 al 2024-01-14**, 6 días. `ventana_de_equipo` toma como `hasta` el día de mayor consumo total del equipo (`P.dia_de_a`, el mismo día de la consulta A de T17: 1 343,381 m³), y como `desde`, ese día menos 5 días. El resultado se recorta al primer día de la historia (2024-01-01) con `max(primero, hasta − 5 días)`. En nuestro caso el recorte no actúa, porque el 2024-01-09 es posterior al 2024-01-01.

---

## 1. Nivel 1 y contraste con la solución de referencia

- **Ruta de `comparacion_nivel1.csv` y de la tabla comparativa:** `T18_etl_elt/comparacion_nivel1.csv`. La tabla comparativa, con una frase por compromiso, está en la consolidación del Nivel 1 del cuaderno y en `entregables/t18/decisiones.md`:
  - **Costo de leer:** el ETL lee 29,6 MB de la cruda y el ELT 0,86 MB de columnas de la preparación.
  - **Costo de escribir:** el ELT escribe más archivos (120 por producto frente a 1).
  - **Trazabilidad:** el ELT conserva las 34 632 lecturas no confiables de la base; el ETL no deja rastro de ellas.
  - **Reproceso:** 6 particiones por ventana.
  - **Contratos:** uno por frontera.
- **Contraste con la referencia (sub-paso A):** mantenemos la capa de T16 (texto con ceros, un archivo por día y todas las filas con su bandera), y coincidimos con la referencia en las cuatro decisiones. Cambiamos dónde se aplica la regla de confiabilidad: ahora en el SQL del producto (`quality_flag = 'OK'`) y no congelada en `es_confiable`. La decisión que hace **barato** rehacer un día de la consolidada es la partición por día con columnas de texto: la vista poda la ventana con `concat(anio, mes, dia)` y la sobrescritura dinámica reescribe solo esos días. Filtrar por `fecha` lo haría caro (120 archivos, como en T17).

---

## 2. Código, hipótesis y bitácora (condiciones de aceptación 1 a 3; criterio 2 de la rúbrica)

- **Rutas:** `pipeline/elt_s18.py`, `sql/consolidada/consumo_dia_sector.sql`, `sql/consolidada/presion_dia_sector.sql`, `contratos/cruda_a_preparacion.json` y `contratos/preparacion_a_consolidada.json`, todos en la raíz del repositorio y copiados en `T18_etl_elt/`. No queda ningún `NotImplementedError` ni `TODO`: la primera comprobación de `t18` da `([], 2, 2)`.
- **Rutas de `hipotesis_s18.json`, `bitacora_capas.jsonl` y `evidencia_nivel2_equipo-1.md`:** las tres en `T18_etl_elt/`.
- **Líneas de la bitácora por clase (ampliación):**

  | Clase | Líneas |
  |---|---|
  | ETL de consumo | 1 |
  | ETL de presión | 1 |
  | ETL de una pasada | 1 |
  | ETL por ventana | 1 |
  | Control de formato (ETL sobre Parquet) | 2 (consumo y presión) |
  | ELT de consumo | 2 |
  | ELT de presión | 2 |
  | Reproceso | 3 (dinámica, dinámica y estática) |
  | Contrato | 1 |
  | Integridad | 1 |

  Además hay 13 líneas de la base, del Nivel 1.

| Hipótesis (escrita antes de medir) | Valor esperado | Valor observado | Diferencia y causa en una frase |
|---|---|---|---|
| Bytes de columnas que lee el producto de consumo ELT | 1 380 000 | 1 382 744 | +2 744 B (+0,2 %): la hipótesis salió de sumar con pyarrow los bytes comprimidos de las cuatro columnas, y la diferencia es el redondeo que aplicamos al registrarla. |
| Bytes de columnas que lee el producto de presión ELT | 400 000 | 401 505 | +1 505 B (+0,4 %): la misma causa. El producto lee `nivel_presion` (197 016 B) y no `presion_bar` (2 076 896 B). |
| Particiones que reescribe el reproceso de la ventana | 6 | 6 | Ninguna: la sobrescritura dinámica reescribe una partición por día de la ventana. |

- **Marcas de tiempo (hipótesis → medición):**
  - ELT de consumo: 22:28:36.648051 Z → 22:28:42.682269 Z.
  - ELT de presión: 22:28:36.648058 Z → 22:28:44.528799 Z.
  - Reproceso: 22:28:36.648060 Z → 22:29:07.329507 Z.

  Todas del 2026-11-12, y en las tres la hipótesis es anterior a la medición.

---

## 3. Productos ELT, resultado idéntico, contratos y reproceso (condiciones de aceptación 4 a 7; criterio 3 de la rúbrica)

| Resultado idéntico (tolerancia $10^{-6}$; conteos exactos) | Diferencia máxima o conteos distintos |
|---|---|
| ELT filtrado con ETL, consumo (`n_confiables` y `consumo_confiable_m3` frente a `n_lecturas` y `consumo_total_m3`) | 480 grupos, 0 conteos distintos, diferencia máxima 0,0e+00 |
| ELT filtrado con ETL, presión (`n_confiables` frente a `n_lecturas`) | 1 440 grupos, 0 conteos distintos |
| ETL de una pasada con ETL por producto | igual: los hashes por producto coinciden (consumo `4a044d4ad6`, presión `7dd6d99d16`) |
| ELT sin filtro con la referencia (`totales_control.csv`) | 480 grupos, diferencia máxima 2,2e-12 |

- **Lecturas guardadas, confiables y excluidas por el ETL:** 1 002 240 guardadas · 942 031 confiables · **60 209 excluidas (6,01 %)**.
- **Contratos:**
  - **`cruda_a_preparacion`**, con **7 invariantes**. Los de reconciliación son **C4** (`conteo_igual`: filas de la preparación igual a las lecturas de la cruda) y **C5** (`suma_igual`: el consumo). Además, C6 compara por (fecha, sector).
  - **`preparacion_a_consolidada`**, con **6 invariantes**. Los de reconciliación son **PC1** (`conteo_igual`: lecturas) y **PC5** (`suma_igual`: consumo).
  - Los dos aprueban sobre las capas del proyecto.
- **Alteración inyectada:** se alteró el **2024-01-14** (el día de mayor consumo; coma decimal entrecomillada). El ETL sobre la copia **no falla**: deja 480 filas, como la real, pero **149 503,907 m³ frente a 150 767,171 m³**. La preparación de la copia queda con **8 352 nulos** en `consumo_m3`. En `cruda_a_preparacion`, el primer invariante que falla es **C2 (`sin_nulos`)**; también fallan C5 y C6. Origen: `_deriva/refinada_spark`.
- **Qué haría el equipo ante ese fallo en una carga real:** detener la publicación de la consolidada de ese día, poner en cuarentena el CSV del 2024-01-14, pedir a la fuente el archivo corregido y reprocesar solo esa ventana cuando el contrato apruebe.

| Reproceso de la ventana del equipo | Particiones intactas | Reescritas | Perdidas | `hash_resultado` (10 primeros caracteres) |
|---|---|---|---|---|
| Dinámica, primera corrida | 114 | 6 | 0 | `00fc866148` |
| Dinámica, segunda corrida | 114 | 6 | 0 | `00fc866148` |
| Estática (contraste) | 0 | 6 | 114 | `67920bd2cd` |

- **Granularidad de la partición de salida (decisión 5):** dejamos la partición **por día**. Una ventana de 6 días reescribe **6 archivos y 13 811 B** (medido). Con partición **por mes**, la misma ventana, que cae en enero, reescribiría 1 archivo con los 31 días de enero: unos **71 KB**. Lo calculamos como 275 768 B × 31/120, con el supuesto de un tamaño constante por día. Es unas 5 veces más, y mezcla 25 días que no cambiaron.
- **Planes de los productos ELT:** `T18_etl_elt/planes_s18/`, con `plan_ampliacion_consumo.txt` y `plan_ampliacion_presion.txt`. `hash_plan_normalizado`: consumo `13adaf5020df` y `13adaf5020df` en las dos corridas; presión `eb10a78640b1` y `eb10a78640b1`.
- **Lago intacto:** `hash_datalake_antes` = `hash_datalake_despues` = **`69fc81c464733edc…`**, en la línea de integridad. `refinada_spark/` tiene el mismo árbol antes y después: **sí** (comprobación 15 de `t18`: `True`).

---

## 4. Documento de decisión ETL/ELT (criterio 1 de la rúbrica)

- **Ruta del documento de decisión:** `entregables/t18/decisiones.md`
- **Para cada producto:** **consumo, ELT**, porque lee 1 382 744 B frente a 54 371 830 B del ETL (39 veces menos) y conserva las 60 209 lecturas descartadas. **Presión, ELT**, porque lee 401 505 B frente a 54 371 830 B del ETL sobre la cruda (135 veces menos) y frente a 2 281 385 B del ETL sobre Parquet: usa la clase ya calculada en la preparación.

| N | Decisión | ¿La justifican? | ¿Es completa? (alternativa descartada con su costo, cifra propia y cifra que la revisaría) | Cifra que la revisaría, con su dirección |
|---|---|---|---|---|
| 1 | Dónde transformar cada producto (obligatoria) | sí | sí | $K$ de construir la preparación por **encima** de 0: con 113 262 935 B y productos fijos, el ETL de una pasada gana |
| 2 | En qué capa vive cada regla, con su mapa regla a capa (obligatoria) | sí | sí | Más de 3 o 4 productos que repitan `quality_flag = 'OK'` (**subir** a una vista común) |
| 3 | Qué se retiene en la preparación | no se justifica por separado (está incluida en la decisión 8) | — | — |
| 4 | Cómo se reprocesa (obligatoria) | sí | sí | Una ventana de **más** de unos 60 días, o un hash distinto entre dos corridas dinámicas |
| 5 | Granularidad de la partición de salida | sí | sí | Archivos por día de **menos** de unos 1 KB, o **más** de unas 1 000 particiones |
| 6 | Qué dice el contrato y dónde se verifica | sí | sí | Que verificar el contrato tome **más** que la carga |
| 7 | Cómo se expresa y se versiona la transformación | sí | sí | Que el `hash_plan_normalizado` **cambie** sin que cambie `hash_sql` |
| 8 | Qué no debe aterrizar (Nivel 3a, opcional) | sí | sí | Una norma que prohíba retener `SIN_SENAL`, o que su fracción **suba** muy por encima del 1 % |

### 4.1 Decisión de la capa refinada o de la lectura de planes revisada con la evidencia de hoy

**Etapa T16 · "la capa conserva todas las lecturas con `es_confiable` junto a `quality_flag`".** La **confirmamos**: el cambio de regla "ESTIMADO también cuenta" se rehízo en el Nivel 1 leyendo **860 254 B** de la preparación, frente a **29 614 179 B** de la cruda en el ETL (34 veces menos). En la ampliación, rehacer el producto de consumo lee 1 382 744 B frente a 54 371 830 B. Lo que la reemplaza parcialmente: la regla ya no se aplica con `es_confiable` (congelada al construir la capa), sino con `quality_flag = 'OK'` en el SQL del producto. Así, cambiar la regla no obliga a reconstruir la preparación.

---

## 5. Comparación de costo (criterio 2 de la rúbrica)

- **Ruta de `costo.md` y de `comparacion.csv`:** `entregables/t18/costo.md` y `T18_etl_elt/comparacion.csv`.
- **Métrica de bytes declarada y por qué es una cota inferior:** `bytes_columnas_leidas`, que cuenta solo los bytes comprimidos de las columnas del `ReadSchema`. Con archivos pequeños y un solo grupo de filas, el lector pide el archivo completo: los 4 519 275 B de la preparación frente a los 1 382 744 B contados para consumo.

| Producto · enfoque | Bytes leídos | Bytes escritos | Archivos escritos | Filas descartadas visibles |
|---|---|---|---|---|
| Consumo · ETL por producto | 54 371 830 | 6 852 | 1 | 0 |
| Presión · ETL por producto | 54 371 830 | 6 238 | 1 | 0 |
| Ambos · ETL de una pasada | 54 371 830 | 12 996 | 2 | 0 |
| Consumo · ETL por ventana | 2 718 973 | 8 150 | 6 | 0 |
| Consumo · control de formato | 1 382 744 | 7 007 | 1 | 0 |
| Presión · control de formato | 2 281 385 | 6 238 | 1 | 0 |
| Consumo · ELT | 1 382 744 | 275 768 | 120 | 60 209 |
| Presión · ELT | 401 505 | 253 897 | 120 | 60 209 |

- **Escenario de $K$ y punto de equilibrio $n^{*}$:** estamos en el escenario de **preparación ya existente** ($K$ = 0, porque `refinada_spark/` se construyó en T16), así que **$n^{*}$ = 0**. Con la preparación por construir con una lectura de la cruda, $K$ = 58 891 105 B y **$n^{*}$ = 1,12 productos**. Con dos lecturas, $K$ = 113 262 935 B y **$n^{*}$ = 2,15 productos**. Con productos fijos y reglas estables, el ELT **no** supera al ETL de una pasada en los dos escenarios con preparación por construir, y **sí** cuando la preparación ya existe.
- **Supuestos:** la preparación se retiene completa (37 661 B por día, 13,1 MiB al año); no crece el número de medidores; hay al menos un producto nuevo y un cambio de regla al año; **no se usa precio**.
- **Cifra que cambiaría su decisión, con su dirección:** que $K$ **suba** de 0, porque la preparación deja de mantenerse y hay que construirla para estos productos. Con $K$ = 113 MB y 2 productos fijos, el ETL de una pasada es unos 61 MB más barato.

---

## 6. Ficha de la capa (criterio 5 de la rúbrica · Power Humanise)

- **Ruta de la ficha y número de palabras:** `entregables/t18/ficha_capa.md`, con **324 palabras** según la verificación (que cuenta los títulos).
- **Lecturas, tal como las dice la ficha:** 1 002 240 guardadas · 942 031 contadas por el tablero · **60 209 excluidas (6,0 %)**, con los dos consumos: 160 402 m³ y 150 767 m³.
- **Qué comprueba el contrato y cuándo, y qué no cubre:** antes de publicar cada tabla, el contrato comprueba que entre capas no se pierden ni se inventan lecturas ni consumo, y que hay una sola fila por día y sector. No garantiza que el medidor haya medido bien.
- **Cómo se pide un recálculo y qué se rehace:** se escribe al equipo de datos con los días afectados y se rehacen solo esos días en la capa consolidada. Por ejemplo, 6 días tocan 6 de 120 archivos. Si cambia la regla de confiabilidad, se recalculan las tablas sin recargar los datos de origen.
- **Si la dieron a leer a una persona ajena al equipo:** no se hizo.

---

## 7. Glosario (criterio 4 de la rúbrica)

- **Ruta del glosario y número de términos completos:** `entregables/t18/glosario.md`, con **8 términos** completos y un caso de nuestra bitácora en cada definición.

---

## 8. Integración: índice de evidencias, reparto, reproducción y README (condición de aceptación 8; criterio 4 de la rúbrica)

### 8.1 Índice de evidencias

- **Ruta del índice y salida de la fila de `t18`:** `indice_evidencias.md` en la raíz del repositorio. Salida: VERIF_T18
- **Etapas cuya salida difiere de la esperada:** VERIF_DIFERENCIAS

### 8.2 Reparto por integrante

| Integrante · partes que hizo · horas |
|---|
| Juan Pablo Castro · todo (trabajo individual): `elt_s18.py`, los dos SQL, los dos contratos, las hipótesis, la ejecución, las decisiones, el costo, la ficha, el glosario, el índice y el README · unas 3 h |

**Total de horas del equipo:** unas 3 horas. No quedó nada sin hacer, salvo lo que se declara en 8.3 y en la sección 10.

### 8.3 Reproducción de un producto por otra persona

- **Quién, cuándo, producto, hash y si coincide:** como el trabajo es **individual**, la repetición no la hizo otra persona. La hizo el mismo autor, en una ejecución independiente desde la terminal y fuera del cuaderno, con el comando del README: `python pipeline/elt_s18.py construir --producto consumo --equipo equipo-1 --volumen ampliacion --bitacora bitacora_repeticion.jsonl`, el 2026-11-12 a las 22:29:35 Z. El producto fue **consumo**, con hash **`00fc866148f940ee…`**, que **coincide** con `bitacora_capas.jsonl`. Declaramos que no se cumple la condición de que fuera otra persona.

### 8.4 README del repositorio

- **De dónde sale `datalake/` y pasos agregados:** **opción B**: `datalake/` se extrae del paquete base y la ampliación se regenera con el generador. Pasos agregados: CLON_PASOS
- **Sección del README con el procedimiento de reproceso por ventana:** "Reprocesar una ventana de días (T18)".
- **Resultado de `verificar_etapa.py --etapa t18` desde el clon limpio:** CLON_T18

---

## 9. Declaración de uso de asistentes de inteligencia artificial

Usamos **Claude Code (Anthropic)** como asistente en estas partes del trabajo:
- Escritura de `ventana_de_equipo`, de los dos SQL, de los invariantes de los dos contratos y de las celdas "Su turno" del cuaderno.
- Cálculo de las hipótesis con pyarrow antes de medir.
- Primera redacción del documento de decisión, de la comparación de costo, de la ficha, del glosario, del índice y del README.

Todas las cifras salen de la bitácora y del cuaderno ejecutados en el contenedor contra la ampliación `equipo-1`. La revisión de las cifras y de los textos es responsabilidad del autor. Esta declaración también está en el README.

---

## 10. Ampliaciones opcionales (si las abordaron)

- **Nivel 3a:** se abordó, en la decisión 8 de `entregables/t18/decisiones.md`. Las lecturas `SIN_SENAL` son el 1,00 % (10 003). Retener toda la preparación cuesta 37 661 B por día (13,1 MiB al año), y recuperar lo descartado cuesta releer la cruda (54 371 830 B). Alternativa adoptada: retener con bandera.
- **Nivel 3b (dbt-core):** no se abordó.
- **Reto de diseño:** se abordó en la ficha y en la celda del reto: mostrar las dos cifras lado a lado, con su diferencia explicada.

---

## Autoverificación antes de entregar

**Condiciones de aceptación.**

- [x] 1. Código, SQL y contratos: `pipeline/elt_s18.py`, los dos SQL y los dos contratos sin `NotImplementedError` ni `TODO` (sección 2).
- [x] 2. Bitácora: `bitacora_capas.jsonl`, sobre la ampliación del equipo, con todos los campos para el ETL (consumo, presión, una pasada y ventana), el ELT (consumo y presión), el reproceso y el contrato (sección 2).
- [x] 3. Hipótesis antes de medir: en las tres mediciones, la marca de la hipótesis es anterior a la de la medición y el valor es numérico (sección 2).
- [x] 4. Productos y resultado idéntico (sección 3).
- [x] 5. Contratos: los dos con columnas, grano, al menos tres invariantes y uno de reconciliación; aprueban con los datos; el contrato `cruda_a_preparacion` falla sobre la copia alterada, con el invariante registrado (sección 3).
- [x] 6. Reproceso e idempotencia: en modo dinámico, 6 reescritas, 0 perdidas y 114 intactas, con dos corridas del mismo hash (sección 3).
- [x] 7. Planes, comparación y lago intacto (secciones 3 y 5).
- [x] 8. Verificación: `verificar_etapa.py --etapa t18` termina en `APROBADA (18 de 18 comprobaciones, N s)`, con la salida pegada en el índice; `decisiones.md` con las decisiones 1, 2 y 4 (y 5 a 8), `costo.md`, glosario de 8 términos y ficha con los seis apartados y 324 palabras (secciones 4 a 8).

**Rúbrica.**

- [x] Criterio 1 (30 %): decisión 1 para cada producto, con las decisiones 1, 2 y 4 completas, la variante de una pasada entre las alternativas y una decisión anterior revisada (sección 4).
- [x] Criterio 2 (20 %): hipótesis antes de medir, contraste explicado, resultado idéntico, métrica declarada, punto de equilibrio en los tres escenarios y ninguna conclusión apoyada en tiempos (secciones 2, 3 y 5).
- [x] Criterio 3 (20 %): reglas en su capa, dos contratos completos, falla en la frontera correcta (C2), reproceso de solo la ventana, idempotencia y granularidad con dos diseños (sección 3).
- [ ] Criterio 4 (15 %): R1 a R5 se cumplen. **R6 no se cumple como lo pide la rúbrica**: la repetición la hizo el mismo autor, porque el trabajo es individual (sección 8.3).
- [x] Criterio 5 (15 %): la ficha tiene las cifras propias, dice qué garantiza y qué no, explica el recálculo, está sin jerga y no oculta la pérdida (sección 6).
- [x] El producto está versionado en el repositorio en `T18_etl_elt/`, más `pipeline/elt_s18.py`, `sql/`, `contratos/`, `entregables/t18/` e `indice_evidencias.md` en la raíz.

---

## Referencias

Nygard, M. T. (2011, 15 de noviembre). *Documenting architecture decisions*. Cognitect. Recuperado de https://www.cognitect.com/blog/2011/11/15/documenting-architecture-decisions
