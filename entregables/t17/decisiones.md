# Registro de decisiones · lectura de planes · equipo equipo-1

IFPN0025 · Big Data e Ingeniería de Datos · Universidad Ean

Una decisión por sección, con el formato de registro de decisiones de arquitectura (Nygard, 2011). Las cifras son de la ampliación de `equipo-1` (`bitacora_planes.jsonl`, corrida 3 de cada consulta), salvo donde se indica.

| # | Decisión | Opciones |
|---|---|---|
| 1 | Diseño físico de la capa para la consulta dominante (el cierre diario) | Partición por día, por mes o sin partición; columnas de partición como texto con ceros |
| 2 | Forma de los filtros y tipo de las columnas de partición | Columna de partición, columna derivada, función o `cast` sobre la ruta, rango de `timestamp`; literal del tipo correcto |
| 3 | Qué columnas se leen | `select` explícito o todas |
| 4 | Estrategia de unión y umbral de difusión | Dejar a Catalyst, pista, umbral global, referencia filtrada a la versión vigente |
| 5 | Qué se confía al optimizador | Configuración por omisión, pistas, ejecución adaptativa |
| 6 | Qué evidencia se guarda de un plan | Plan normalizado, métricas y hash del resultado; captura de pantalla; tiempos |
| 7 | Unión con clave sesgada (Nivel 3, opcional) | Catalyst, mezcla forzada, sufijo aleatorio |

## Decisión 1 · Diseño físico de la capa

- **Contexto.** La consulta dominante es el cierre diario: consumo por sector de **un** día. La capa de la ampliación tiene 120 días en 120 archivos de unos 36,8 KiB, y crecerá un día por cierre.
- **Opciones consideradas.** Partición por día (la actual, `anio/mes/dia` como texto), partición por mes (medida en N2.8, en `_diseno/`) y sin partición.
- **Evidencia propia.** Bitácora, consulta A: `archivos_leidos` 1 y `tamano_archivos_leidos_b` 37 605 B (36,7 KiB). Con el diseño por mes, el mismo cierre lee 1 archivo de **818,2 KiB** (22 veces más) y escanea 198 912 filas en lugar de 8 352 (N2.8a). Para el reporte del mes, el diseño por día abre 31 archivos (1 140,4 KiB) y el diseño por mes 1 (818,2 KiB).
- **Decisión.** **Mantener la partición por día** con `anio`, `mes` y `dia` como texto con ceros. Vale mientras la consulta dominante sea el cierre diario y los archivos diarios midan de decenas de KiB a cientos de MiB.
- **Alternativa descartada y su costo.** Partición por mes: el cierre diario lee 818,2 KiB en lugar de 36,7 KiB (+781,5 KiB por cierre). En 365 cierres serían unos 291,6 MiB frente a 13,1 MiB (cálculo con la fórmula de `costo.md`, suponiendo un tamaño diario constante). Su punto de equilibrio para el cierre es **infinito**: no ahorra archivos (1 frente a 1) y lee más bytes. Solo se paga para el **reporte mensual**: ahorra 30 archivos por reporte, y reescribir los 124 archivos se paga en 4,1 reportes.
- **Consecuencias.** El cierre diario lee un archivo de unos 37 KiB, siempre igual, sin importar cuánta historia haya. A cambio, el reporte mensual abre 31 archivos y los archivos son pequeños (muchos metadatos): a 3 años serían unos 1 215 archivos.
- **Cifra que la revisaría.** Que el reporte mensual pase a ser la consulta dominante (más de unas 4 ejecuciones por cada reescritura del diseño), o que un archivo diario baje de unos pocos KiB, cuando convendría compactar por mes conservando la poda por día con `fecha` ordenada.
- **Revisión de una decisión anterior con la evidencia de hoy.** En T16 decidimos "un archivo por día con `repartition(anio, mes, dia)`" y sobrescritura dinámica. Hoy la **evaluamos y la mantenemos**: la cifra que la sostiene es `archivos_leidos` = 1 en A, frente a 120 en A-fecha, con el mismo resultado. Lo que **cambiamos** es la forma de consultar, no la de escribir: el cierre debe filtrar por `anio`, `mes` y `dia` y nunca por `fecha` (decisión 2), porque `fecha` dentro del archivo no poda.

## Decisión 4 · Estrategia de unión y umbral

- **Contexto.** El consumo por estrato une 1 002 240 lecturas confiables con la referencia de 348 medidores (`medidores.parquet`, unos 3 KiB en disco).
- **Opciones consideradas.** Dejar a Catalyst con el umbral por omisión (10 MiB); forzar la mezcla con el umbral en −1; una pista `broadcast()`; activar la ejecución adaptativa; filtrar la referencia a la versión vigente (`es_actual`).
- **Evidencia propia.** C: **BroadcastHashJoin**, 1 `Exchange`, **724 B** de mezcla y 3 etapas con 13 tareas. C-forzada: **SortMergeJoin**, 3 `Exchange`, 2 `Sort`, **7 799 575 B** de mezcla y 4 etapas con 21 tareas. C-aqe: BroadcastHashJoin y 6 tareas. El `hash_resultado` es el mismo en las tres (`0142edc26554…`). La difusión real (`difusion_bytes`) es de **4 210 672 B**, mientras que el lado de la referencia se estimó en unos KiB (1 639 B en la base): la estimación subestima la tabla hash que se difunde, pero las dos cifras están bajo los 10 485 760 B del umbral.
- **Decisión.** **Dejar a Catalyst con el umbral por omisión** y **unir con la referencia vigente** (`medidores.parquet`, una fila por medidor). Sin pistas en el código.
- **Alternativa descartada y su costo.** Forzar la mezcla (SortMergeJoin) cuesta 7 799 575 B de mezcla frente a 724 B: 10 773 veces más bytes moviéndose entre tareas, más 2 ordenamientos y 8 tareas extra, con el mismo resultado. Una pista `broadcast()` fija la difusión aunque la referencia crezca por encima de lo que cabe en memoria. Unir con `medidores_con_historia.parquet` sin filtrar multiplica filas: en la base, 2 880 filas y 461,611 m³ de más por un solo medidor repetido.
- **Consecuencias.** Las lecturas no se mueven en la unión. Si la referencia crece, Catalyst cambiará solo a SortMergeJoin al cruzar el umbral: no hace falta cambiar código, pero sí vigilar el plan.
- **Cifra que la revisaría.** Que `difusion_bytes` se acerque al umbral de 10 MiB: hoy son 4,2 MB para 348 medidores, y escalando linealmente (supuesto) se cruzaría hacia los 870 medidores. Ese sería el momento de subir el umbral con cifras de memoria o de dejar que la unión pase a SortMergeJoin. También la revisaría que el `hash_resultado` de C deje de coincidir con C-forzada.

## Decisión 2 · Forma de los filtros y tipos de partición

- **Contexto.** El mismo día se puede filtrar de varias formas, y no todas podan.
- **Opciones consideradas.** Las columnas de la ruta (`anio`, `mes`, `dia`), la columna `fecha` interna, `concat` de las columnas de la ruta e `input_file_name()`. El literal se puede escribir con o sin ceros (`'01'` frente a `'1'`).
- **Evidencia propia.** Ampliación: A (ruta) lee **1** archivo y A-fecha **120**, con las mismas 8 352 filas y el mismo hash. Nivel 1 (base, rotulado): `concat(anio, mes, dia) = '20240215'` también poda (1 archivo) y `input_file_name()` no poda (120 archivos y 576 000 filas escaneadas). Con columnas de texto, el literal `'2'` no coincide con `'02'` (D.1b).
- **Decisión.** Filtrar el cierre por **`anio`, `mes` y `dia` como texto con ceros**, con literales del mismo tipo, y desactivar la inferencia de tipos de las columnas de partición (`partitionColumnTypeInference.enabled=false`).
- **Alternativa descartada y su costo.** Filtrar por `fecha` cuesta 119 archivos de más por cierre (4 413,4 KiB frente a 36,7 KiB). En un año serían 110 595 archivos frente a 365 (`costo.md`).
- **Consecuencias.** La poda depende de que quien consulte use las columnas de la ruta. Lo documentamos en el README y en el código (`consulta_a`).
- **Cifra que la revisaría.** Un `archivos_leidos` mayor que 1 en la consulta A de cualquier corrida futura (la bitácora lo registra).

## Decisión 5 · Qué se confía al optimizador

- **Contexto.** Los planes de esta práctica deben compararse entre corridas y entre equipos.
- **Opciones consideradas.** La configuración por omisión (ejecución adaptativa activa, 200 particiones de mezcla, inferencia de tipos de partición) o una configuración fija.
- **Evidencia propia.** Con la ejecución adaptativa, el plan inicial (`fe97c6731ebb…`) y el final (`f0f5a8a2d17b…`) difieren y las tareas bajan de 13 a 6. El hash del plan normalizado de A es el mismo en las tres corridas medidas (`049621f66597…`).
- **Decisión.** Fijar `adaptive.enabled=false`, 8 particiones de mezcla y la inferencia de tipos de partición desactivada para el análisis. La ejecución adaptativa se enciende solo para medir su efecto (C-aqe).
- **Alternativa descartada y su costo.** Con la configuración por omisión, el plan cambia después de ejecutar y el hash del plan no sería estable entre corridas: perderíamos la comparación. En producción sí activaríamos la ejecución adaptativa (−7 tareas en C).
- **Consecuencias.** Planes reproducibles y comparables, a costa de no ver las optimizaciones en tiempo de ejecución en las consultas A a C-forzada.
- **Cifra que la revisaría.** Que dos corridas de la misma consulta den distinto `hash_plan_normalizado` con la configuración fija.

## Decisión 7 · Unión con clave sesgada (Nivel 3)

- **Contexto.** Una unión por `sector` (4 claves) con 8 particiones de mezcla. S-SUR tiene 345 600 de 1 002 240 lecturas (34,5 %).
- **Opciones consideradas.** Catalyst por omisión, mezcla forzada y sufijo aleatorio (K = 8).
- **Evidencia propia.** La fracción **predicha** para la partición más cargada con mezcla forzada es **78,2 %**: S-SUR, S-NORTE y S-OESTE caen en la partición 7 por `hash(sector) mod 8`. La fracción **medida** también es **78,2 %**. Bytes de mezcla: Catalyst (difusión) 716 B; mezcla forzada 6 862 720 B, con 2 tareas con datos; sufijo aleatorio 8 655 193 B, con 8 tareas con datos y la más pesada con el 25,0 % de las filas. El resultado es idéntico en las tres.
- **Decisión.** **Dejar a Catalyst**: la tabla de sectores tiene 4 filas y se difunde, así que no hay mezcla y el sesgo no importa.
- **Alternativa descartada y su costo.** El sufijo aleatorio reparte la carga (del 78,2 % al 25,0 % en la tarea más pesada), pero mueve 8,66 MB frente a 716 B. La mezcla forzada concentra el 78,2 % de las filas en una tarea.
- **Consecuencias.** Sin mezcla, el sesgo no afecta. Si la tabla pequeña dejara de caber en la difusión, habría que aplicar el sufijo aleatorio.
- **Cifra que la revisaría.** Que la unión deje de ser BroadcastHashJoin, o que la tarea más pesada pase de unos 128 MiB de mezcla.

## Referencias

Nygard, M. T. (2011, 15 de noviembre). *Documenting architecture decisions*. Cognitect. https://www.cognitect.com/blog/2011/11/15/documenting-architecture-decisions
