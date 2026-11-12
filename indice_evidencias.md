# Índice de evidencias del pipeline del acueducto · equipo equipo-1

IFPN0025 · Big Data e Ingeniería de Datos · Universidad Ean

Cada fila es una etapa del pipeline: el **commit** que la contiene, la **salida del comando de verificación** y quién la hizo. Las salidas se obtuvieron el 2026-11-12, desde la raíz de este repositorio (T12 a T14 con el perfil `s12`; T15 a T18 con el perfil `s18`).

| Etapa | Qué verifica | Comando (desde la raíz del repositorio) | Resultado esperado | Commit | Resultado obtenido (pegar la última línea y las que difieran) | Responsable |
|---|---|---|---|---|---|---|
| T12 · Ingesta incremental | Carga idempotente: sin claves repetidas y una marca de agua por sector | `python pipeline/verificar_etapa.py --etapa t12` (perfil `s12` arriba) | `Etapa t12: APROBADA (4 de 4 comprobaciones)` | `135dd06` | `Etapa t12: APROBADA (4 de 4 comprobaciones, 0.4 s)` | Juan Pablo Castro |
| T13 · Conector de la API | Estado de medidores sin duplicar y bitácora de intentos | `python pipeline/verificar_etapa.py --etapa t13` (perfil `s12` arriba) | `Etapa t13: APROBADA (5 de 5 comprobaciones)` | `135dd06` | `Etapa t13: APROBADA (5 de 5 comprobaciones, 0.1 s)` | Juan Pablo Castro |
| T14 · Captura de cambios | Cambios por tipo y punto de control | `python pipeline/verificar_etapa.py --etapa t14` (perfil `s12` arriba) | `Etapa t14: APROBADA (3 de 3 comprobaciones)` | `135dd06` | `Etapa t14: APROBADA (3 de 3 comprobaciones, 0.1 s)` | Juan Pablo Castro |
| T15 · Carga con Spark | Agregado diario por sector reconciliado y bitácora | `docker compose --profile s18 exec spark python pipeline/verificar_etapa.py --etapa t15` | `Etapa t15: APROBADA (6 de 6 comprobaciones)` | `135dd06` | `Etapa t15: APROBADA (6 de 6 comprobaciones, 0.3 s)` | Juan Pablo Castro |
| T16 · Capa refinada | Esquema declarado, tipo físico de la marca temporal, particiones por día, reconciliación y bitácora | `docker compose --profile s18 exec spark python pipeline/verificar_etapa.py --etapa t16` | `Etapa t16: APROBADA (15 de 15 comprobaciones)` | `135dd06` | `Etapa t16: APROBADA (15 de 15 comprobaciones, 0.7 s)` | Juan Pablo Castro |
| T17 · Lectura de planes | Planes normalizados y versionados, hipótesis antes de medir, resultado idéntico entre variantes, lago sin cambios y entregables de texto | `docker compose --profile s18 exec spark python pipeline/verificar_etapa.py --etapa t17 --volumen ampliacion` | `Etapa t17: APROBADA (18 de 18 comprobaciones)` | `135dd06` | `Etapa t17: APROBADA (18 de 18 comprobaciones, 0.2 s)` | Juan Pablo Castro |
| T18 · ETL frente a ELT | Productos ELT idénticos al ETL, contratos por frontera, reproceso por ventana y entregables | `docker compose --profile s18 exec spark python pipeline/verificar_etapa.py --etapa t18 --volumen ampliacion` | `Etapa t18: APROBADA (18 de 18 comprobaciones, N s)` | `135dd06` | `Etapa t18: APROBADA (18 de 18 comprobaciones, 2.6 s)` | Juan Pablo Castro |

**Diferencias explicadas.**
- **T12 a T14:** el verificador comprueba las tablas de **referencia** del paquete en la base `acueducto` (1 200 lecturas, 90 medidores, 94 suscriptores), y en eso coinciden con lo esperado. Las cifras de nuestra instancia `equipo-1` están en las bases propias `acueducto_equipo_1` y en las evidencias de cada carpeta: 888 → 888 lecturas tras la doble ejecución, 99 medidores con 9 → 19 intentos, y 105 → 102 suscriptores con 11 fantasmas.
- **T17 y T18:** se verifican con `--volumen ampliacion`, porque las bitácoras del equipo son de la ampliación `equipo-1`.

## Reparto por integrante

| Integrante | Partes que hizo | Horas |
|---|---|---|
| Juan Pablo Castro | Trabajo individual: todas las piezas de T12 a T18 (código, ejecución, evidencias, decisiones, memos, fichas, glosarios e índice) | unas 22 h (T12 3, T13 3, T14 2, T15 3, T16 4, T17 3, T18 3, sin contar la preparación de los entornos) |

## Reproducción de un producto por otra persona

Como el trabajo es **individual**, la repetición **no la hizo otra persona**. La hizo el autor en una ejecución independiente desde la terminal, fuera del cuaderno, siguiendo los comandos del README. Declaramos que no se cumple la condición de "otra persona".

| Quién | Cuándo | Producto repetido | Hash obtenido | Coincide con la bitácora |
|---|---|---|---|---|
| Juan Pablo Castro (autor) | 2026-11-12 22:29:35 Z | T18 · `consumo_dia_sector` (ELT) | `00fc866148f940ee9ba4b48f2ef98058201d7abf0859b0ee512fa7751c1e0ca8` | sí |
| Juan Pablo Castro (autor) | 2026-11-12 22:18:15 Z | T17 · plan A (y las otras cinco consultas) | `049621f66597…` | sí (las seis consultas) |

## Decisión de una etapa anterior que revisaron con evidencia nueva

**T16 · "la capa conserva todas las lecturas con `es_confiable` junto a `quality_flag`".** Confirmada con la evidencia de T18. El cambio de regla "ESTIMADO también cuenta" se rehízo leyendo 860 254 B de la preparación, frente a 29 614 179 B de la cruda en el ETL (base, Nivel 1). En la ampliación, rehacer el producto de consumo lee 1 382 744 B frente a 54 371 830 B. Se reemplaza solo **dónde** se aplica la regla: en el SQL del producto (`quality_flag = 'OK'`), no congelada en `es_confiable`, para que cambiarla no obligue a reconstruir la preparación.

**T15 · cierre diario con `reduceByKey` sobre RDD.** Cambiado en T16: el `groupBy` de DataFrame mueve 41 100 B frente a 9 822 B, pero usa 5 tareas en lugar de 8 y es una línea legible. Con 480 grupos, la diferencia de bytes no importa.
