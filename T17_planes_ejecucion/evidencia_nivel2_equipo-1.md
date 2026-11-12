# Evidencia del Nivel 2 · equipo-1

## 1. Perfil y configuracion
- Equipo: equipo-1 · volumen: ampliacion · medidores: 348 · sector dominante: S-SUR (0.3448)
- Dia de la consulta A: 2024-01-14 (el de mayor consumo total segun totales_control.csv)
- Spark 3.5.3 · modo de explain: formatted · ejecucion adaptativa apagada salvo en C-aqe · particiones de mezcla: 8 · umbral de difusion: 10 MiB (C-forzada: -1) · columnas de particion como texto

## 2. Hipotesis (con su marca de tiempo)
| consulta | archivos esperados | union esperada | marca | razon |
|---|---|---|---|---|
| A | 1 | None | 2026-11-12T22:17:34.046087Z | Filtra por anio, mes y dia, que son columnas de la RUTA: PartitionFilters poda 119 de las 120 carpetas y se abre solo el archivo del dia. |
| A-fecha | 120 | None | 2026-11-12T22:17:34.046096Z | fecha esta DENTRO del archivo, no en la ruta: no hay PartitionFilters, el filtro solo se empuja al lector (PushedFilters), asi que se abren los 120 archivos para mirar sus estadisticas, aunque devuelva las mismas filas que A. |
| B | 120 | None | 2026-11-12T22:17:34.046099Z | presion_bar no es columna de particion y no esta ordenada: cada archivo diario tiene presiones por encima y por debajo de 3,5 bar (max > 3,5), asi que el predicado empujado no puede descartar ningun archivo: 120. |
| C | 120 | BroadcastHashJoin | 2026-11-12T22:17:34.046101Z | Lee toda la capa (es_confiable no es particion): 120 archivos. La referencia de medidores (348 filas) estimada en KiB queda bajo el umbral de 10 MiB: Catalyst la difunde (BroadcastHashJoin) y no mezcla las lecturas. |
| C-forzada | 120 | SortMergeJoin | 2026-11-12T22:17:34.046103Z | Mismos 120 archivos; con el umbral en -1 no puede difundir: repartira ambos lados por meter_id y los ordenara (SortMergeJoin), con al menos 2 Sort y 3 Exchange y muchos mas bytes de mezcla que C. |
| C-aqe | 120 | BroadcastHashJoin | 2026-11-12T22:17:34.046104Z | Mismos 120 archivos; con ejecucion adaptativa el plan inicial ya difunde la referencia pequena, asi que el plan final tambien sera BroadcastHashJoin (AQE solo coalesce las particiones de la agregacion). |

## 3. Observado
| consulta | archivos esperados | archivos leidos | filas de salida del escaneo | union esperada | union observada | Exchange | etapas | tareas | bytes de mezcla | diferencia max. con el calculo independiente |
|---|---|---|---|---|---|---|---|---|---|---|
| A | 1 | 1 | 8352 | None | None | 1 | 2 | 9 | 224 | 9.1e-13 |
| A-fecha | 120 | 120 | 8352 | None | None | 1 | 2 | 12 | 224 | 9.1e-13 |
| B | 120 | 120 | 1002240 | None | None | 1 | 2 | 12 | 891 | 1.8e-12 |
| C | 120 | 120 | 1002240 | BroadcastHashJoin | BroadcastHashJoin | 1 | 3 | 13 | 724 | 7.3e-10 |
| C-forzada | 120 | 120 | 1002240 | SortMergeJoin | SortMergeJoin | 3 | 4 | 21 | 7799575 | 1.0e-10 |
| C-aqe | 120 | 120 | 1002240 | BroadcastHashJoin | BroadcastHashJoin | 1 | 3 | 6 | 724 | 7.3e-10 |

## 4. Contraste de cada hipotesis con lo observado
- A: se cumplio (1 archivo esperado, 1 leido): el filtro por la ruta poda 119 de 120 carpetas.
- A-fecha: se cumplio (120 y 120): fecha esta dentro del archivo; el lector salta grupos de filas pero abre los 120 archivos.
- B: se cumplio (120 y 120): el predicado se empuja pero ningun archivo tiene su maximo de presion por debajo de 3,5 bar.
- C: se cumplio (120 y 120, BroadcastHashJoin): la referencia (348 medidores) cabe bajo el umbral de 10 MiB.
- C-forzada: se cumplio (120 y 120, SortMergeJoin): con el umbral en -1, 3 Exchange y 2 Sort.
- C-aqe: se cumplio (120 y 120, BroadcastHashJoin): el plan final difunde igual; AQE reduce las tareas de 13 a 6.

## 5. Decisiones del nivel
- Dia de A: 2024-01-14, el de mayor consumo total (1 343,381 m3), con desempate por la fecha mas temprana; predicado sobre anio, mes y dia como texto con ceros. Lo cambiaria un archivos_leidos de A mayor que 1.
- Union: Catalyst con el umbral por omision (10 MiB), sin pista y con la referencia vigente. La cambiaria que difusion_bytes (hoy 4 210 672 B) se acerque al umbral.
- Diseno fisico: particion por dia (A lee 36,7 KiB por cierre frente a 818,2 KiB con el diseno por mes, cuyo punto de equilibrio es infinito para el cierre).

## 6. Conclusion
OBSERVADO: A lee 1 archivo (PartitionFilters sobre anio, mes y dia) y A-fecha 120, con las mismas 8 352 filas y el mismo hash; B empuja presion_bar > 3,5 y aun asi lee 120 archivos y 1 002 240 filas; C difunde la referencia (724 B de mezcla) y C-forzada mezcla 7 799 575 B con 3 Exchange y 2 Sort, con el mismo resultado. INTERPRETADO: A poda porque el filtro esta en las columnas de la RUTA; B no ahorra archivos porque el menor de los maximos de presion por archivo es 3,977 bar, asi que ninguna estadistica permite saltar un archivo con el umbral de 3,5; la mezcla forzada cuesta lo que cuesta porque obliga a repartir y ordenar el millon de lecturas por meter_id en lugar de copiar 348 medidores a cada tarea.
