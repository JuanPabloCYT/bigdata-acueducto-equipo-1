# T17 · Tres planes de ejecución leídos como evidencia: por qué el cierre diario del acueducto cuesta lo que cuesta, qué diseño y qué unión conviene, y cuánto se paga por cambiarlos

**Equipo:** equipo-1 (trabajo individual)
**Integrantes:** Juan Pablo Castro
**Proyecto:** Plataforma de datos del acueducto (proyecto integrador)
**Identificador de equipo (`--equipo`):** `equipo-1`
**Entorno usado:** contenedor del servicio `spark` con el perfil `s17` de Docker Compose (`acueducto_spark_s15`, Spark 3.5.3, `local[4]` con 1 g). El cuaderno se ejecutó dentro del contenedor con `jupyter nbconvert --execute`.
**Capa usada:** `datalake_equipo/refinada_spark/`, escrita por nuestro `pipeline/refinada_spark.py` (el de T16) con `--equipo equipo-1 --volumen ampliacion`. Hash de la capa: `7b0ac06a1ab1…`, el mismo de T16, y reconciliaciones `OK`.
**Fecha de entrega:** 2026-11-12

---

## 0. Perfil de la ampliación y entorno

- **Línea de `parametros_equipo.json`:** `Equipo equipo-1: 348 medidores, 120 dias, 1,002,240 lecturas`
- **Sector dominante y día de la consulta A, con la regla de `dia_de_a`:** S-SUR (120 de 348 medidores, 0,3448). El día es **2024-01-14**, el de mayor consumo total sumando los cuatro sectores en `totales_control.csv` (1 343,381 m³). Desempate: la fecha más temprana (ordenamiento estable por consumo descendente y fecha ascendente).
- **Archivos de su capa y tamaño de un archivo diario en KiB:** 120 archivos de 36,7 KiB (37 605 B el del 2024-01-14), con un grupo de filas de 8 352 filas cada uno. 4 413,4 KiB en total.

---

## 1. Nivel 1 y contraste con la solución de referencia

- **Rutas:** `T17_planes_ejecucion/planes_nivel1.csv`, `T17_planes_ejecucion/planes_nivel1/` y el contraste del sub-paso A en la celda "Su decisión · Sub-paso A" del cuaderno (`T17_planes_ejecucion/BiD_S17_P4_cuaderno_v1.ipynb`).
- **La decisión de escritura que vuelve más cara una de las tres consultas, y cuál consulta:** guardar `fecha` **dentro** del archivo (y no solo en la ruta) encarece **A-fecha**, que abre los 120 archivos para devolver el mismo día que A lee de 1.

---

## 2. Código, hipótesis y bitácora (condiciones de aceptación 1 a 5; criterio 2 de la rúbrica)

- **Ruta de `pipeline/planes_s17.py`, y confirmación de que no quedan `NotImplementedError` ni `TODO`:** `pipeline/planes_s17.py` en la raíz del repositorio (copia en `T17_planes_ejecucion/pipeline/`). `grep -c "TODO\|NotImplementedError"` da **0**.
- **Rutas:** `hipotesis_s17.json`, `bitacora_planes.jsonl` y `evidencia_nivel2_equipo-1.md`, todas en `T17_planes_ejecucion/`.
- **Cómo ejecutaron las consultas y número de líneas de la bitácora por consulta:** con `P.ejecutar_todo` en el cuaderno (celda N2.5): 1 calentamiento descartado y **3 corridas medidas** por consulta, es decir, **3 líneas por consulta** y 18 en total.
- **Cifras de control** (última corrida medida de cada consulta): `archivos_leidos` de A = **1** y de A-fecha = **120**. `estrategia_union` de C = **BroadcastHashJoin** y de C-forzada = **SortMergeJoin**.
- **Hipótesis frente a lo observado:** **todas coinciden** (6 de 6: `hipotesis cumplidas` en N2.6). La razón es que la capa tiene exactamente un archivo por día, con un grupo de filas cada uno, y que la referencia de 348 medidores está muy lejos del umbral de difusión: no había ningún caso límite que pudiera desviar los archivos o la estrategia. La diferencia que sí encontramos está en los **tamaños** de la difusión: el lado de la referencia se estimó en KiB, pero lo difundido midió 4 210 672 B. Eso no cambió la estrategia, porque sigue bajo los 10 MiB.

| Resultado idéntico (tolerancia $10^{-6}$; conteo exacto en B) | Diferencia máxima (`diferencia_max_independiente`) |
|---|---|
| A con A-fecha y con el cálculo independiente | 9,1e-13 (el mismo `hash_resultado`, `d88345bc8d0a…`) |
| B con el cálculo independiente (conteo) | 1,8e-12 en el consumo; el conteo es exacto |
| C con C-forzada, C-aqe y el cálculo independiente | 7,3e-10 (el mismo `hash_resultado` en las tres, `0142edc26554…`) |

- **Planes normalizados:** están en `T17_planes_ejecucion/planes/`, con los seis `plan_ampliacion_<consulta>.txt`. El `hash_plan_normalizado` de A fue **`049621f66597`** en las corridas 1, 2 y 3. La repetición independiente desde la terminal dio el mismo valor (sección 7.3).
- **Lago intacto:** `hash_datalake_antes` = `hash_datalake_despues` = **`69fc81c464733edc…`**, en la línea de la consulta A. `referencia/` presente: **sí** (`medidores.parquet`, `medidores_con_historia.parquet` y `sectores.parquet`).

---

## 3. Informe de tres planes (criterio 1 de la rúbrica)

- **Ruta del informe y quién redactó cada plan:** `entregables/t17/informe_planes.md`. Los tres planes los redactó Juan Pablo Castro (trabajo individual).
- **Una frase por cada una de las seis consultas, que separe lo observado de lo interpretado:**
  - **A:** se observó 1 archivo leído con `PartitionFilters` sobre `anio`, `mes` y `dia`; se interpreta que el filtro por la ruta poda 119 carpetas.
  - **A-fecha:** se observaron 120 archivos abiertos y las mismas 8 352 filas; se interpreta que `fecha` interna solo permite saltar grupos de filas, no evitar archivos.
  - **B:** se observaron un predicado empujado, 120 archivos y 1 002 240 filas escaneadas; se interpreta que ninguna estadística de archivo permite descartar uno con el umbral de 3,5 bar.
  - **C:** se observó BroadcastHashJoin con 724 B de mezcla; se interpreta que la referencia pequeña se difunde y las lecturas no se mueven.
  - **C-forzada:** se observó SortMergeJoin con 3 `Exchange`, 2 `Sort` y 7 799 575 B; se interpreta que sin difusión hay que repartir y ordenar el millón de lecturas.
  - **C-aqe:** se observaron la misma difusión, 6 tareas y planes inicial y final distintos; se interpreta que la ejecución adaptativa ajusta la agregación, no la unión.
- **Una cifra clave de cada explicación:**
  - **L3:** el mínimo y el máximo de `presion_bar` por archivo que explican B: el **menor de los máximos es 3,977 bar** y el mayor de los máximos es 4,45 bar (el mínimo de cada archivo está entre 1,62 y 1,99 bar). Con el umbral de 3,5 se pueden saltar 0 de 120 archivos.
  - **L4:** A-fecha abre **120 archivos** y salta **119 grupos de filas**.
  - **L5:** la mezcla de C-forzada es de **7 799 575 B**, frente a **724 B** de C.

---

## 4. Registro de decisiones (criterio 3 de la rúbrica)

- **Ruta del registro de decisiones:** `entregables/t17/decisiones.md`
- **Números de las decisiones justificadas, y cuántas son completas:** 1, 4, 2, 5 y 7. Las **5 están completas**: tienen opciones, alternativa descartada con su costo, cifra propia y cifra de revisión.
- **La cifra que revisaría la decisión 1 y la que revisaría la decisión 4:** la decisión 1 se revisaría si el reporte mensual se ejecuta más de unas 4 veces por cada reescritura del diseño (su $n^{*}$ es de 4,1 reportes). La decisión 4, si `difusion_bytes` (hoy 4 210 672 B) se acerca a los 10 485 760 B del umbral, unos 870 medidores extrapolando linealmente.

### 4.1 Decisión de la capa o de una etapa anterior revisada con la evidencia de esta etapa

**Etapa T16 · forma de la escritura de la capa ("un archivo por día con `repartition(anio, mes, dia)`, sobrescritura dinámica").** La **evaluamos y la mantenemos**. La cifra que la sostiene: con esta escritura, el cierre diario filtrado por la ruta lee **1 archivo de 36,7 KiB**, frente a 818,2 KiB con la alternativa por mes, cuyo $n^{*}$ es infinito para el cierre. Lo que sí **cambiamos** es la regla de consulta: el cierre filtra por `anio`, `mes` y `dia` y nunca por `fecha`, porque `fecha` abre los 120 archivos.

---

## 5. Sección de costo (criterio 5 de la rúbrica · Emprendimiento Sostenible)

- **Ruta de la sección de costo:** `entregables/t17/costo.md`
- **Supuestos que sostienen la proyección:** $H$ = 120 días, $D$ = 365 cierres, tamaño por archivo de 37 605 B (36,7 KiB) sin crecimiento y **ningún precio por unidad** (la unidad es el archivo abierto).
- **Razón entre el costo sin poda y con poda en $D$ cierres:** **303**, es decir, (2H + D + 1)/2 = 606/2: 110 595 archivos frente a 365.
- **Punto de equilibrio $n^{*}$:** corregir el filtro tiene **$n^{*}$ = 0 cierres** (definido: cuesta 0 y ahorra 302 archivos por cierre en promedio). Cambiar a partición por mes para el cierre diario tiene **$n^{*}$ infinito**: no ahorra archivos (1 frente a 1) y lee 818,2 KiB en lugar de 36,7 KiB.
- **Recomendación en una frase, riesgo y cifra de revisión:** mantener la partición por día y corregir todo cierre que filtre por `fecha`. Aceptamos que el reporte mensual abra 31 archivos pequeños. Lo revisaríamos si el reporte mensual corre más de unas 4 veces por reescritura, o si un archivo diario supera unos 128 MiB.

---

## 6. Glosario (criterio 4 de la rúbrica)

- **Ruta del glosario y número de términos completos:** `entregables/t17/glosario.md`, con **8 términos** completos y un caso de nuestros planes en cada definición.

---

## 7. Integración: índice de evidencias, reparto, reproducción y README (condición de aceptación 6; criterio 4 de la rúbrica)

### 7.1 Índice de evidencias

- **Ruta del índice y salida de la fila de `t17`:** `indice_evidencias.md` en la raíz del repositorio. Salida: `Etapa t17: APROBADA (18 de 18 comprobaciones, 0.2 s)`, en el commit `135dd06`
- **Etapas cuya salida difiere de la esperada, con la explicación:** ninguna. T12 a T14 verifican las tablas de referencia del paquete y aprueban; las cifras de `equipo-1` están en las evidencias de cada tarea (nota del índice)

### 7.2 Reparto por integrante

| Integrante · partes que hizo · horas |
|---|
| Juan Pablo Castro · todo (trabajo individual): código de `planes_s17.py`, hipótesis, ejecución, lectura de los planes A, B y C, decisiones, costo, glosario, índice y README · unas 3 h |

**Total de horas del equipo:** unas 3 horas. No quedó nada sin hacer. El Nivel 3 y el reto también se resolvieron (sección 9).

### 7.3 Reproducción de un plan por otra persona

- **Quién, cuándo, plan repetido, hash obtenido y si coincide:** como el trabajo es **individual**, la repetición no la hizo otra persona, sino el mismo autor, en una ejecución independiente desde la terminal, fuera del cuaderno. Se usó el comando del README: `python pipeline/planes_s17.py --equipo equipo-1 --volumen ampliacion --corridas 1 --hipotesis hipotesis_s17.json --bitacora bitacora_repeticion.jsonl --planes planes_repeticion`, el 2026-11-12 a las 22:18:15 Z. Se repitieron las seis consultas. El plan A dio el hash **`049621f66597`**, que **coincide** con `bitacora_planes.jsonl`, y las otras cinco consultas también coinciden. Declaramos que no se cumple la condición de que fuera otra persona.

### 7.4 README del repositorio

- **De dónde sale `datalake/` y pasos agregados:** **opción B**: `datalake/` se extrae del paquete base (el README trae el comando `unzip`), y la ampliación se regenera con el generador. Pasos agregados: la regeneración de `datalake_equipo/cruda` y `refinada` con el generador, y el cuaderno de T18 como paso obligatorio (ver la sección 4 del README)
- **Resultado de `verificar_etapa.py --etapa t17` desde el clon limpio:** `Etapa t17: APROBADA (18 de 18 comprobaciones, 0.2 s)` (clon limpio del commit `135dd06`, en `~/proyectos/clon_prueba`)

---

## 8. Declaración de uso de asistentes de inteligencia artificial

Usamos **Claude Code (Anthropic)** como asistente en estas partes del trabajo:
- Escritura de las cuatro funciones de `planes_s17.py` y de las celdas "Su turno" del cuaderno.
- Redacción de las hipótesis, que se registraron antes de medir, y de los textos de decisión.
- Primera redacción del informe de tres planes, del registro de decisiones, de la sección de costo, del glosario, del índice y del README.

Todas las cifras salen de la bitácora y del cuaderno ejecutados en el contenedor contra la ampliación `equipo-1`. La revisión de las cifras y de los textos es responsabilidad del autor. Esta declaración también está en el README.

---

## 9. Ampliaciones opcionales (si las abordaron)

- **Nivel 3:** la fracción de filas **predicha** para la partición de mezcla más cargada (unión por `sector` con mezcla forzada) es **78,2 %**, porque S-SUR, S-NORTE y S-OESTE caen en la partición 7. La **medida** también es **78,2 %**. Decisión 7 en `entregables/t17/decisiones.md`: dejar a Catalyst (difusión, 716 B), frente al sufijo aleatorio (8,66 MB, con la tarea más pesada en el 25 %).
- **Reto de diseño:** `archivos_acumulados(H, D)` resuelto. A tres años, el cierre sin poda abriría 731 460 archivos-día (25,6 GiB), frente a 0,038 GiB con poda y 0,854 GiB con el diseño por mes. Los cálculos están en el cuaderno; no escribimos un memo aparte.

---

## Autoverificación antes de entregar

**Condiciones de aceptación.**

- [x] 1. Planes y bitácora: `bitacora_planes.jsonl` tiene todos los campos para A, A-fecha, B, C, C-forzada y C-aqe, sobre la ampliación del equipo, con al menos dos corridas de cada una de las cinco primeras; los seis planes de `planes/` están normalizados y su hash coincide con la bitácora (sección 2).
- [x] 2. Hipótesis antes de medir: en las cinco consultas, la marca de la hipótesis es anterior a la de la medición y los archivos esperados son un entero (sección 2).
- [x] 3. Lo que los planes muestran: A poda con `PartitionFilters` sobre `anio`, `mes` y `dia` y lee un archivo; A-fecha lee todos los archivos y devuelve las mismas filas; B empuja el predicado sin podar archivos; C difunde por omisión; C-forzada mezcla con ordenamiento (al menos 2 `Sort` y 3 `Exchange`); la ejecución adaptativa está apagada en las cinco (sección 2).
- [x] 4. Resultado idéntico entre variantes y coincidente con el cálculo independiente (diferencia máxima menor o igual que $10^{-6}$; conteo exacto en B) (sección 2).
- [x] 5. Lago intacto (sin las capas reconstruibles): el hash de `datalake_equipo/` es igual antes y después de las corridas y `referencia/` está presente (sección 2).
- [x] 6. Verificación: `verificar_etapa.py --etapa t17` termina en `APROBADA (18 de 18 comprobaciones)`, con la salida pegada en el índice (sección 7.1).

**Rúbrica (calidad de lo leído, de lo medido, de lo decidido, de lo documentado y de lo costeado).**

- [x] Criterio 1 (30 %): L1 y L2 completos para las cinco consultas, citando el campo del plan; L3 (B), L4 (A-fecha) y L5 (C) explicados; ninguna afirmación contradicha por la bitácora (sección 3).
- [x] Criterio 2 (20 %): hipótesis antes de medir en las cinco consultas (E1), contraste explicado (E2), resultado idéntico con la diferencia máxima mostrada (E3), métricas coherentes y mismo hash en dos corridas (E4), y ninguna conclusión de decisión apoyada en tiempos (E5) (sección 2).
- [x] Criterio 3 (20 %): al menos tres decisiones completas, entre ellas la 1 y la 4, y una decisión de la capa o de una etapa anterior revisada con la evidencia de esta etapa (sección 4).
- [ ] Criterio 4 (15 %): etapa aprobada con su salida en el índice (R1), mismo hash en dos corridas (R2), bitácora completa (R3), README con el comando de cada paso (R4) y glosario de 8 términos (R5) se cumplen. **R6 no se cumple como lo pide la rúbrica**: la repetición la hizo el mismo autor, porque el trabajo es individual (sección 7.3).
- [x] Criterio 5 (15 %): proyección a 365 cierres con la fórmula y la historia inicial (V1), al menos un cambio alternativo (V2), supuestos sin tarifas inventadas (V3), costo de cambiar y punto de equilibrio con uno definido y uno infinito justificado (V4), y recomendación con riesgo y cifra de revisión (V5) (sección 5).
- [x] El producto está versionado en el repositorio en `T17_planes_ejecucion/`, más `pipeline/planes_s17.py`, `entregables/t17/` e `indice_evidencias.md` en la raíz.

---

## Referencias

Nygard, M. T. (2011, 15 de noviembre). *Documenting architecture decisions*. Cognitect. https://www.cognitect.com/blog/2011/11/15/documenting-architecture-decisions
