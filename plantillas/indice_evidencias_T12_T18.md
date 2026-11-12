# Índice de evidencias del pipeline del acueducto · equipo @@EQUIPO@@

IFPN0025 · Big Data e Ingeniería de Datos · Universidad Ean

Cada fila es una etapa del pipeline. Para cada una, el equipo anota el **commit** que la contiene, pega la **salida del comando de verificación** y registra quién la hizo. La columna "Resultado esperado" ya está escrita: es el estado de referencia del proyecto. Si el resultado obtenido difiere, escriban por qué en la columna de observaciones; una diferencia explicada es evidencia, una diferencia sin explicar no lo es.

| Etapa | Qué verifica | Comando (desde la raíz del repositorio) | Resultado esperado | Commit | Resultado obtenido (pegar la última línea y las que difieran) | Responsable |
|---|---|---|---|---|---|---|
| T12 · Ingesta incremental | Carga idempotente: sin claves repetidas y una marca de agua por sector | `python pipeline/verificar_etapa.py --etapa t12` (perfil `s12` arriba) | `Etapa t12: APROBADA (4 de 4 comprobaciones)` | @@COMPLETAR@@ | @@COMPLETAR@@ | @@COMPLETAR@@ |
| T13 · Conector de la API | Estado de medidores sin duplicar y bitácora de intentos | `python pipeline/verificar_etapa.py --etapa t13` (perfil `s12` arriba) | `Etapa t13: APROBADA (5 de 5 comprobaciones)` | @@COMPLETAR@@ | @@COMPLETAR@@ | @@COMPLETAR@@ |
| T14 · Captura de cambios | Cambios por tipo y punto de control | `python pipeline/verificar_etapa.py --etapa t14` (perfil `s12` arriba) | `Etapa t14: APROBADA (3 de 3 comprobaciones)` | @@COMPLETAR@@ | @@COMPLETAR@@ | @@COMPLETAR@@ |
| T15 · Carga con Spark | Agregado diario por sector reconciliado y bitácora | `docker compose --profile s17 exec spark python pipeline/verificar_etapa.py --etapa t15` | `Etapa t15: APROBADA (6 de 6 comprobaciones)` | @@COMPLETAR@@ | @@COMPLETAR@@ | @@COMPLETAR@@ |
| T16 · Capa refinada | Esquema declarado, tipo físico de la marca temporal, particiones por día, reconciliación y bitácora | `docker compose --profile s17 exec spark python pipeline/verificar_etapa.py --etapa t16` | `Etapa t16: APROBADA (15 de 15 comprobaciones)` | @@COMPLETAR@@ | @@COMPLETAR@@ | @@COMPLETAR@@ |
| T17 · Lectura de planes | Planes normalizados y versionados, hipótesis antes de medir, resultado idéntico entre variantes, lago sin cambios y entregables de texto | `docker compose --profile s17 exec spark python pipeline/verificar_etapa.py --etapa t17` | `Etapa t17: APROBADA (18 de 18 comprobaciones)` | @@COMPLETAR@@ | @@COMPLETAR@@ | @@COMPLETAR@@ |
| T18 · ETL frente a ELT | Productos ELT idénticos al ETL, contratos por frontera, reproceso por ventana y entregables | `docker compose --profile s18 exec spark python pipeline/verificar_etapa.py --etapa t18` | `Etapa t18: APROBADA (18 de 18 comprobaciones, N s)` | @@COMPLETAR@@ | @@COMPLETAR@@ | @@COMPLETAR@@ |

## Reparto por integrante

Una línea por persona: qué parte del trabajo hizo y cuántas horas le dedicó. En T18, lo natural es que cada integrante escriba una pieza (la ventana, un SQL, un contrato) y redacte su apartado del documento de decisión. Si el reparto no es equilibrado, díganlo.

| Integrante | Partes que hizo | Horas |
|---|---|---|
| @@COMPLETAR@@ | @@COMPLETAR@@ | @@COMPLETAR@@ |
| @@COMPLETAR@@ | @@COMPLETAR@@ | @@COMPLETAR@@ |
| @@COMPLETAR@@ | @@COMPLETAR@@ | @@COMPLETAR@@ |

## Reproducción de un producto por otra persona

Una persona distinta de quien escribió el código repitió un producto siguiendo solo el README del repositorio y obtuvo el mismo `hash_resultado`.

| Quién | Cuándo | Producto repetido | Hash obtenido | Coincide con la bitácora |
|---|---|---|---|---|
| @@COMPLETAR@@ | @@COMPLETAR@@ | @@COMPLETAR@@ | @@COMPLETAR@@ | @@COMPLETAR@@ |

## Decisión de una etapa anterior que revisaron con evidencia nueva

Si una decisión de las etapas T12 a T17 cambió con lo que midieron hoy, regístrenla aquí: qué decían, qué cifra la cambió y qué decisión la reemplaza. Si ninguna cambió, escriban cuál evaluaron y por qué la mantienen con una cifra.

@@COMPLETAR@@
