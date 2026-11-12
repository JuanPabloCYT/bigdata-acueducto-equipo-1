# Informe de tres planes · equipo @@EQUIPO@@

IFPN0025 · Big Data e Ingeniería de Datos · Universidad Ean

Un plan de ejecución es la evidencia de cuántos datos se leyeron, cuáles se pudieron evitar y por qué. Este informe lee tres planes con sus variantes de diagnóstico y los respalda con las métricas de `bitacora_planes.jsonl`. Cada plan lleva los mismos cuatro apartados. **Citen siempre el campo del plan que muestra lo que afirman** (por ejemplo, `PartitionFilters` o `PushedFilters`) y la línea de la bitácora de donde sale cada cifra.

**Datos del informe.** Volumen: ampliación del equipo. Día de la consulta A y cómo lo obtuvieron: (complete aquí). Versión de Spark, ejecución adaptativa, particiones de mezcla, umbral de difusión, tipos de las columnas de partición y modo de `explain`: (copien los valores de la bitácora).

## Plan A · Consumo por sector de un día (con su variante A-fecha)

**1. Qué se pidió y qué esperaban.** (Su hipótesis, con la marca de tiempo anterior a la medición.)

**2. Dónde se resolvió cada filtro.** Para A y para A-fecha: ¿en la ruta, en el lector o después de leer? Citen el campo del plan.

**3. Qué se leyó.** Archivos leídos, filas de salida del escaneo y columnas leídas, de las dos consultas.

**4. Qué no pudo evitarse y por qué.** Expliquen por qué A-fecha devuelve las mismas filas que A leyendo más archivos, distinguiendo archivos abiertos de grupos de filas saltados.

**Evidencia.**

| Campo de la bitácora | A | A-fecha |
|---|---|---|
| `partition_filters` | | |
| `pushed_filters` | | |
| `read_schema` | | |
| `archivos_leidos` | | |
| `filas_salida_escaneo` | | |
| `etapas` · `tareas` | | |
| `hash_resultado` (deben ser iguales) | | |
| `diferencia_max_independiente` | | |

## Plan B · Lecturas con presión alta

**1. Qué se pidió y qué esperaban.**

**2. Dónde se resolvió el filtro.**

**3. Qué se leyó.**

**4. Qué no pudo evitarse y por qué.** Expliquen por qué el predicado aparece empujado y aun así se leen todos los archivos, con el mínimo y el máximo de `presion_bar` por archivo leídos con un lector independiente.

**Evidencia.**

| Campo de la bitácora | B |
|---|---|
| `pushed_filters` | |
| `archivos_leidos` | |
| `filas_salida_escaneo` | |
| `filas_resultado` | |
| `diferencia_max_independiente` | |

## Plan C · Consumo por estrato con la referencia de medidores (con C-forzada y C-aqe)

**1. Qué se pidió y qué esperaban.** Incluyan la estrategia de unión que esperaban.

**2. Dónde se resolvió cada filtro.**

**3. Qué se leyó.** Y cuánto se movió entre máquinas.

**4. Qué no pudo evitarse y por qué.** Estrategia por omisión y tamaños estimados de cada lado; qué cambia al forzar la mezcla (`Exchange`, `Sort`, bytes de mezcla); qué cambia con la ejecución adaptativa (plan inicial frente a plan final); y qué estrategia sería razonable con más volumen.

**Evidencia.**

| Campo de la bitácora | C | C-forzada | C-aqe |
|---|---|---|---|
| `umbral_difusion` | | | |
| `aqe` | | | |
| `estrategia_union` | | | |
| `n_exchange` · `n_sort` | | | |
| `etapas` · `tareas` | | | |
| `bytes_mezcla` | | | |
| `hash_resultado` (deben ser iguales) | | | |
| `hash_plan_inicial` · `hash_plan_final` | | | |

## Lo observado frente a lo interpretado

Una frase por plan que separe lo que midieron de lo que concluyen. Las conclusiones de decisión no se apoyan en tiempos.
