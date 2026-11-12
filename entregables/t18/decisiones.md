# Documento de decisión · dónde se transforma y qué se conserva entre capas

Equipo: `equipo-1` · Volumen: ampliación del equipo (348 medidores, 1 002 240 lecturas) · Fuente: `bitacora_capas.jsonl` y `comparacion.csv`

Este documento responde a la pregunta de la dirección financiera: **¿conviene seguir transformando antes de guardar o guardar primero y transformar adentro?** Se decide por producto, y cada decisión se sostiene con una cifra de la bitácora y con la cifra que la haría cambiar. Los bytes se miden con la métrica declarada `bytes_columnas_leidas`, que es una cota inferior (ver `costo.md`).

## Las ocho decisiones posibles

| N | Decisión | Opciones | Criterios para decidir | Consecuencia de decidir mal |
|---|---|---|---|---|
| 1 | Dónde transformar cada producto | ETL por producto · ETL de varias salidas en una pasada · ELT sobre la preparación · híbrido | Productos actuales y futuros, frecuencia de cambio de reglas, necesidad de auditar, sensibilidad del dato, bytes | Pagar relecturas completas, o retener lo que no debía aterrizar |
| 2 | En qué capa vive cada regla | Preparación · consolidada · ambas | Si descarta o reinterpreta, y con qué frecuencia cambia | Reprocesar toda la historia por un cambio menor, o reglas que divergen |
| 3 | Qué se retiene en la preparación | Todo con bandera · cuarentena aparte · descarte | Costo de almacenar, riesgo de no poder explicar, sensibilidad | Datos inexplicables, o almacenamiento sin límite |
| 4 | Cómo se reprocesa | Ventana por partición con sobrescritura dinámica · rehacer toda la capa · sobrescritura estática | Tamaño de la ventana, dependencias, idempotencia | Perder particiones o duplicar filas |
| 5 | Granularidad de la partición de salida | Por día · por mes · sin partición | Ventana típica de reproceso, tamaño y cantidad de archivos | Reescribir un mes por un día, o 120 archivos de 4 filas |
| 6 | Qué dice el contrato y dónde se verifica | Esquema · grano · invariantes · momento · acción al fallar | Qué necesita quien consume y cuánto cuesta verificar | Alteraciones silenciosas que llegan al tablero |
| 7 | Cómo se expresa y se versiona la transformación | SQL declarativo · DataFrame · función propia · herramienta de modelado | Legibilidad, plan resultante, versionado | Lógica dispersa en cuadernos sin versión |
| 8 | Qué no debe aterrizar (Nivel 3) | Descartar en el borde · cuarentena · retener con bandera | Costo de lamentarse, almacenamiento, norma | Perder evidencia, o retener lo que no debía |

## Decisión de una etapa anterior revisada con la evidencia de hoy

**Etapa T16 · "la capa refinada conserva todas las lecturas con `es_confiable` junto a `quality_flag`".** Hoy la **confirmamos** con una cifra nueva. Con todas las lecturas retenidas, el cambio de regla "ESTIMADO también cuenta" se rehízo en el Nivel 1 leyendo **860 254 B** de columnas de la preparación, frente a **29 614 179 B** de la cruda que tuvo que releer el ETL (34 veces menos). En la ampliación, rehacer el producto de consumo lee 1 382 744 B frente a 54 371 830 B. Lo que **cambiamos** es dónde se aplica la regla: el producto ya no usa `es_confiable` (la regla congelada al construir la capa), sino `quality_flag = 'OK'` en el SQL (decisión 2). Así, cambiar la regla es cambiar una línea de SQL y reprocesar el producto, sin reconstruir la preparación.

## Decisiones

## Decisión 1 · Dónde transformar cada producto: ELT para consumo y presión

- **Contexto.** Hay dos productos del tablero (consumo por día y sector, y presión por día, sector y nivel) sobre 1 002 240 lecturas. El ETL heredado lee la cruda completa por producto: **54 371 830 B** por trabajo. La preparación `refinada_spark/` **ya existe**, porque se construyó en T16 y se reconcilia con tres referencias.
- **Opciones consideradas.**
  - **ETL por producto:** 54 371 830 B leídos y 6 852 B escritos (consumo); 54 371 830 B y 6 238 B (presión). En total, **108 743 660 B** leídos por los dos.
  - **ETL de una pasada:** 54 371 830 B leídos y 12 996 B escritos para los dos productos.
  - **ELT sobre la preparación:** consumo **1 382 744 B** leídos y 275 768 B escritos; presión **401 505 B** y 253 897 B. En total, **1 784 249 B** leídos.
  - **Híbrido:** ETL para presión y ELT para consumo. No aporta nada: la presión es justamente el producto más barato en ELT.
- **Decisión.** **ELT para los dos productos.** Con la preparación existente ($K$ = 0), el ELT lee **30 veces menos** bytes que el ETL de una pasada (1,78 MB frente a 54,4 MB) y **61 veces menos** que el ETL por producto. Además, conserva visibles las **60 209 lecturas** que el ETL descarta.
- **Alternativa descartada y por qué.** El **ETL de una pasada** es la mejor alternativa ETL, porque lee la cruda una sola vez para los dos productos. Lo descartamos porque, con la preparación ya construida, cuesta 54 371 830 B frente a 1 784 249 B, y cada cambio de regla o producto nuevo obliga a releer la cruda completa. Además, no permite responder cuántas lecturas se descartaron: `filas_descartadas_visibles` es 0 en todos los ETL y 60 209 en el ELT.
- **Costo de la decisión.** Escribe más: unos 529 665 B en 240 archivos (un archivo por día y producto), frente a 12 996 B en 2 archivos del ETL de una pasada. También retiene la preparación: unos 4,5 MB por 120 días, 37 661 B por día, 13,1 MiB al año.
- **Cifra que la revisaría.** Si la preparación **no existiera** y hubiera que construirla leyendo la cruda dos veces ($K$ = 113 262 935 B), el ELT solo se pagaría frente al ETL por producto desde $n^{*}$ ≈ 2,15 productos, y frente al ETL de una pasada con productos fijos no se pagaría (`costo.md`). **Si la preparación dejara de mantenerse por otras razones**, la decisión pasaría al ETL de una pasada.

## Decisión 2 · En qué capa vive cada regla

- **Contexto.** Hay tres reglas: (a) la conversión de tipos y la marca temporal, (b) la **clase de presión** (umbrales de 2,5 y 3,5 bar) y (c) la **confiabilidad** (`quality_flag = 'OK'`).
- **Opciones consideradas.** Ponerlas todas en la preparación, todas en el producto, o repartirlas según si la regla **descarta** o solo **reinterpreta**.
- **Decisión (mapa regla a capa).**

  | Regla | Capa | Por qué |
  |---|---|---|
  | Tipos, marca temporal en µs y partición por día | Preparación (`refinada_spark.py`) | No descarta nada: es el contrato de lectura que todos los productos comparten. |
  | Clase de presión (`nivel_presion`) | Preparación, con una sola copia de los umbrales | No descarta filas. El ETL heredado tenía su **propia copia** de los umbrales: dos copias pueden divergir. |
  | Confiabilidad (`quality_flag = 'OK'`) | **Producto** (SQL de `consolidada_elt/`) | Es la regla que **descarta**, y es la que cambia: en el Nivel 1 cambió a "OK o ESTIMADO". En el producto, cambiarla relee 1,38 MB y no reescribe la preparación. |

- **Alternativa descartada y por qué.** Filtrar la confiabilidad en la preparación, como hace el ETL. Cuesta perder 60 209 lecturas (6,01 %), sin forma de explicarlas, y releer la cruda (54,4 MB) cada vez que la regla cambie.
- **Costo de la decisión.** Cada producto repite la condición `quality_flag = 'OK'` en su SQL: dos lugares que hay que mantener iguales. Lo controla el resultado idéntico con el ETL (0 conteos distintos en 480 y 1 440 grupos).
- **Cifra que la revisaría.** Si los productos que comparten la regla pasaran de unos 3 o 4, convendría una vista intermedia `prep_confiables` con la regla en un solo lugar. Y si la regla dejara de cambiar (cero cambios en un año), podría bajar a la preparación como columna.

## Decisión 4 · Cómo se reprocesa: ventana por partición con sobrescritura dinámica

- **Contexto.** La ventana del equipo va del **2024-01-09 al 2024-01-14** (6 días que terminan en el día de mayor consumo, 1 343,381 m³). La salida `consolidada_elt/consumo_dia_sector` tiene 120 particiones por día.
- **Opciones consideradas.** Ventana con sobrescritura **dinámica**, ventana con sobrescritura **estática**, o rehacer toda la capa.
- **Evidencia propia** (sobre una copia, `_copias_reproceso/`):
  - **Dinámica, corrida 1:** 114 intactas, 6 reescritas, 0 perdidas; hash `00fc866148`.
  - **Dinámica, corrida 2:** las mismas 114, 6 y 0, y el **mismo hash** `00fc866148` (idempotente).
  - **Estática:** 0 intactas, 6 reescritas y **114 perdidas** (hash `67920bd2cd`).
  - Las dos leen 6 archivos y 69 278 B de columnas, y escriben 13 811 B.
- **Decisión.** **Ventana por partición con sobrescritura dinámica**, lanzada con `elt_s18.py reprocesar --desde --hasta --sobrescritura dinamica`.
- **Alternativa descartada y por qué.** La estática pierde 114 de 120 días con la misma lectura. Rehacer toda la capa lee 1 382 744 B y escribe 275 768 B (20 veces más) para corregir 6 días.
- **Costo de la decisión.** Depende de que el SQL y la vista limiten la ventana por las columnas de la ruta (`concat(anio, mes, dia)`). Si alguien filtra por `fecha`, se abren los 120 archivos (lo medimos en T17).
- **Cifra que la revisaría.** Una ventana de más de unos 60 días: a partir de la mitad de la historia, reescribir toda la capa cuesta lo mismo y es más simple. También un hash distinto entre dos corridas dinámicas de la misma ventana.

## Decisión 5 · Granularidad de la partición de salida: por día

- **Contexto.** El reproceso típico es una ventana de 6 días y la consulta típica es el cierre diario.
- **Opciones consideradas.** Por día: 120 archivos de unos 2,3 KB en consumo (275 768 B en total). Por mes: 4 archivos. Sin partición: 1 archivo de unos 7 KB, como el ETL.
- **Evidencia.** Con partición por día, la ventana de 6 días reescribe **6 archivos y 13 811 B**. Por mes, la misma ventana, que cae en enero, reescribiría **1 archivo con los 31 días** de enero: unos 71 KB (275 768 × 31/120, calculado con el supuesto de un tamaño constante por día). Sin partición, reescribiría el archivo completo (los 120 días).
- **Decisión.** **Por día**, con el mismo criterio que la preparación (T16) y que la consulta A (T17).
- **Alternativa descartada y por qué.** La partición por mes reescribe unas 5 veces más bytes por ventana (unos 71 KB frente a 13,8 KB) y mezcla días que no cambiaron.
- **Costo de la decisión.** Son 120 archivos pequeños por producto, de unos 2 KB, y el listado del directorio cuesta más que los datos.
- **Cifra que la revisaría.** Si los archivos por día bajaran de unos 1 KB, o el número de particiones superara unas 1 000 (unos 3 años), compactaríamos por mes la historia cerrada y dejaríamos por día solo el mes en curso.

## Decisión 6 · Qué dice el contrato y dónde se verifica

- **Contexto.** El ETL heredado no avisa cuando la cruda llega alterada. Sobre la copia con el **2024-01-14** alterado (coma decimal entrecomillada), el ETL terminó **sin error**: 480 filas y 149 503,907 m³ frente a 150 767,171 m³. Faltaron **1 263,264 m³** sin ningún aviso.
- **Opciones consideradas.** Un contrato solo al final (en el tablero), o un contrato por frontera: cruda → preparación y preparación → consolidada.
- **Decisión.** **Un contrato por frontera**, verificado al terminar de escribir cada capa y antes de publicar la siguiente:
  - **`cruda_a_preparacion`**, con 7 invariantes: C1 columnas en orden, **C2 sin nulos**, C3 grano único, **C4 filas iguales a la cruda**, **C5 consumo igual a la cruda**, C6 igual por (fecha, sector) y C7 sectores válidos.
  - **`preparacion_a_consolidada`**, con 6 invariantes: **PC1 lecturas iguales a la preparación**, PC2 grano único, PC3 sin nulos, PC4 confiables + no confiables = total, **PC5 consumo igual** y PC6 confiable ≤ total.
  - **Acción al fallar:** detener la publicación de la capa siguiente.
- **Evidencia.** Con los datos reales, los dos contratos **aprueban** (7 de 7 y 6 de 6). Sobre la copia alterada, `cruda_a_preparacion` **falla**, y el primer invariante que falla es **C2** (`sin_nulos`), con **8 352 nulos** en `consumo_m3`. También fallan C5 y C6. Falla en la frontera donde ocurrió el daño, no en el tablero.
- **Alternativa descartada y por qué.** Verificar solo en el tablero detecta tarde y no dice en qué capa entró el error.
- **Costo de la decisión.** Leer la cruda con pandas para el control es una pasada completa de la cruda. Es aceptable en una carga diaria, pero no en tiempo real.
- **Cifra que la revisaría.** Que verificar el contrato tome más que la carga misma. Ahí se verificaría por ventana y no por historia completa.

## Decisión 7 · Cómo se expresa y se versiona la transformación

- **Contexto.** El ETL heredado es código DataFrame con su propia copia de los umbrales; el ELT es SQL.
- **Decisión.** **SQL declarativo versionado** en `sql/consolidada/*.sql`. Cada corrida anota en la bitácora el hash del SQL (`hash_sql`) y el hash del plan normalizado: consumo `13adaf5020df` y presión `eb10a78640b1`, iguales en dos corridas.
- **Alternativa descartada y por qué.** Una función propia de Python pierde el plan legible y, como medimos en T16, cuesta de 4,6 a 11,2 veces más.
- **Cifra que la revisaría.** Que el `hash_plan_normalizado` de un producto cambie sin que cambie su `hash_sql`: sería una regresión del motor o de la configuración, y habría que investigarla.

## Decisión 8 · Qué no debe aterrizar (Nivel 3a)

- **Contexto.** En la ampliación, el **1,00 %** de las lecturas son `SIN_SENAL` (10 003 de 1 002 240), el 5,01 % `ESTIMADO` y el 93,99 % `OK`.
- **Opciones.** Descartarlas en el borde, ponerlas en cuarentena aparte o retenerlas con bandera.
- **Decisión.** **Retenerlas con bandera** en la preparación. Retener toda la preparación cuesta **37 661 B por día** (13,1 MiB al año), y recuperar lo que el ETL descartó obligaría a releer la cruda completa (**54 371 830 B**).
- **Alternativa descartada y por qué.** Descartar en el borde ahorra el 1 % de 13 MiB al año, pero pierde la evidencia de cuántos medidores no reportaron.
- **Cifra que la revisaría.** Una norma que obligue a no almacenar lecturas sin señal (por ejemplo, datos personales asociados), o que `SIN_SENAL` supere una fracción en la que el almacenamiento sí pese.

## Referencias

Nygard, M. T. (2011, 15 de noviembre). *Documenting architecture decisions*. Cognitect. https://www.cognitect.com/blog/2011/11/15/documenting-architecture-decisions
