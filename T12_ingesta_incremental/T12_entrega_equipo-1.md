# T12 · La ingesta de su instancia del acueducto, hecha para repetirse sin duplicar

**Equipo:** [completar: nombre del equipo]
**Integrantes:** [completar: tres integrantes]
**Proyecto:** Plataforma de datos del acueducto
**Identificador de equipo (`--equipo`):** `equipo-1`
**Motor usado:** PostgreSQL 16 (perfil `s12` de Docker Compose, contenedor `acueducto_postgres_s12`), base propia del equipo `acueducto_equipo_1`
**Fecha:** 2026-10-02

---

## 0. Instancia de datos del equipo

- **Comando exacto usado para generar la instancia:** `python generar_datos_S12.py --equipo equipo-1 --dias 2 --inicio 2024-06-01 --salida lotes_equipo-1` (lotes del Nivel 2 y de la doble ejecución). Para el memo del reto: `python generar_datos_S12.py --equipo equipo-1 --dias 30 --inicio 2024-06-01 --salida lotes_equipo-1_30d`.
- **Qué trae esa instancia (número de medidores, frecuencia de reentregas, proporción de tardíos):** el generador fija para `equipo-1` **37 medidores**, **2,2 % de reentregas** y **3,7 % de tardíos** (semilla base 42).
  - **Volumen diario:** 37 × 24 = **888 lecturas propias**, repartidas en S-NORTE con 10 medidores (240 lecturas) y en S-SUR, S-ESTE y S-OESTE con 9 medidores cada uno (216 lecturas cada uno).
  - **Lote del día 1:** 888 filas.
  - **Lote del día 2:** 939 filas, que son 888 propias del día 2 y 51 reentregas y tardíos del día 1.
  - **Histórico de 30 días:** 26.640 lecturas, en 28.119 filas recibidas.

---

## 1. El script y las dos tablas

- **Ruta del script de ingesta:** `practica/ingesta_incremental.py`. Trae las funciones `ingest_lote` (incremental) y `reprocesar_ventana` (reapertura de ventana), las mismas del cuaderno `practica/BiD_S12_P4_cuaderno_v3.ipynb`.
- **Nombre de la tabla de staging:** `stg_lecturas`. Es el nombre exacto, con clave primaria `(meter_id, "timestamp")`.
- **Nombre de la tabla de control de estado:** `estado_ingesta`. Es el nombre exacto, con una fila por sector.
- **Filas cargadas en `stg_lecturas` tras la primera corrida:** **888**: S-NORTE 240, S-SUR 216, S-ESTE 216 y S-OESTE 216. La suma de `consumo_m3` es 141,897 m³.

---

## 2. La prueba de doble ejecución (rúbrica: evidencia de idempotencia)

| Momento | Conteo de `stg_lecturas` |
|---|---|
| Después de la primera corrida | 888 |
| Después de la segunda corrida, mismo lote | 888 (0 filas nuevas en los cuatro sectores) |

**Comando o celda exacta usada para correr el script dos veces:** `python ingesta_incremental.py --equipo equipo-1 --dia 1 --reiniciar --doble`, ejecutado desde `practica/`. Parte de un estado en cero, carga el lote del día 1 dos veces seguidas y compara el conteo, la suma de consumo y las marcas. Termina con `RESULTADO: IDEMPOTENTE` y código de salida 0. La misma prueba está en las secciones 2.6 y 2.7 del cuaderno: 600 y 600 filas sobre el ejemplo de 25 medidores.

**Por qué este resultado demuestra idempotencia y no una simple coincidencia (Kleppmann, 2017):** que el conteo no cambie no es casualidad, porque la segunda corrida tiene dos barreras independientes. El filtro `"timestamp" > ultima_marca_agua` deja fuera las 888 filas, que ya están bajo la marca `2024-06-01 23:00:00`. Y aunque alguna pasara, `ON CONFLICT (meter_id, "timestamp") DO NOTHING` la ignoraría. Las dos barreras van en la misma transacción que la marca. Tampoco es un script que no hace nada, porque la primera corrida sí cargó las 888 filas esperadas (37 × 24). Además del conteo, comprobamos que **la suma de `consumo_m3` siguió en 141,897** en las dos corridas, es decir, que ninguna fila cambió de valor. También comprobamos que la marca de agua no se movió.

**Si los dos conteos no coinciden:** no ocurrió: los conteos coincidieron (888 y 888) desde la primera prueba. El script devuelve el código de salida 1 e imprime `FALLO DE IDEMPOTENCIA` si alguna de las tres medidas (conteo, suma o marcas) difiere.

---

## 3. La marca de agua en `estado_ingesta` (rúbrica: evidencia de idempotencia)

| Momento | Valor de la marca de agua |
|---|---|
| Antes de la primera corrida (valor inicial o centinela) | `2000-01-01 00:00:00` en los cuatro sectores (centinela, nunca `NULL`) |
| Después de la primera corrida | `2024-06-01 23:00:00` en los cuatro sectores |
| Después de la segunda corrida (mismo lote) | `2024-06-01 23:00:00` en los cuatro sectores (sin cambio) |

Corrimos también el lote del día 2: la marca avanzó a `2024-06-02 23:00:00` y `stg_lecturas` pasó a 1.776 filas (888 × 2). Al repetir ese lote, el conteo siguió en 1.776 y la marca no cambió. La marca avanza con cada lote nuevo, no se congela y no retrocede, porque se actualiza con `GREATEST`.

---

## 4. Instrucciones de reproducción

Requisitos: Docker Desktop, Python 3.11 o superior y las dependencias de `practica/requirements.txt`.

1. Desde la raíz del paquete, levantar PostgreSQL: `docker compose --profile s12 up -d`. Esperar a que `docker ps` muestre el contenedor `acueducto_postgres_s12` como `healthy`.
2. Instalar las dependencias, de preferencia en un entorno virtual: `pip install -r practica/requirements.txt`.
3. Entrar a la carpeta de la práctica: `cd practica`.
4. Generar la instancia del equipo: `python generar_datos_S12.py --equipo equipo-1 --dias 2 --inicio 2024-06-01 --salida lotes_equipo-1`. La consola debe mostrar `Equipo equipo-1 -> medidores=37, dup_rate=0.022, late_rate=0.037`, `lote_dia01.csv ... 888 filas` y `lote_dia02.csv ... 939 filas`.
5. Correr la prueba de doble ejecución: `python ingesta_incremental.py --equipo equipo-1 --dia 1 --reiniciar --doble`. El script crea la base `acueducto_equipo_1` si no existe y deja las tablas en cero.
6. Salida esperada:
   - **Corrida 1:** S-NORTE 240, S-SUR 216, S-ESTE 216 y S-OESTE 216 filas nuevas. `stg_lecturas: 888 filas · suma consumo_m3: 141.897` y las cuatro marcas en `2024-06-01 23:00:00`.
   - **Corrida 2:** 0 filas nuevas por sector, con el mismo conteo, la misma suma y las mismas marcas.
   - **Al final:** `Prueba de doble ejecucion: conteo 888 -> 888, suma 141.897 -> 141.897, marcas iguales: True` y `RESULTADO: IDEMPOTENTE`.
7. Opcional, para probar con corridas sueltas: `python ingesta_incremental.py --equipo equipo-1 --dia 2`, dos veces. La primera deja 1.776 filas con las marcas en `2024-06-02 23:00:00`; la segunda, 0 filas nuevas y el mismo conteo.
8. Reproceso de ventana, hueco y memo: ejecutar completo `BiD_S12_P4_cuaderno_v3.ipynb` con `EQUIPO = "equipo-1"`. La celda final debe mostrar las ocho comprobaciones en `OK`.

`--reiniciar` es destructivo: vacía `stg_lecturas` de la base del equipo. Solo sirve para repetir la prueba desde cero.

---

## 5. Decisiones sobre la instancia `--equipo` (rúbrica: decisiones sobre la instancia)

**Conclusión: el script del Nivel 1 no requiere cambios para `equipo-1`. Mantenemos el filtro estricto por marca de agua, sin margen de traslape, con un lote diario por sector y una transacción por sector.** Las razones, con cifras de nuestra instancia:

- **Margen de traslape: no hace falta.** El lote del día 2 trae 51 filas con fecha del día 1 (reentregas al 2,2 % y tardíos al 3,7 %). Las comparamos fila por fila con el lote del día 1: **las 51 son copias exactas de filas ya cargadas** (51 de 51 coinciden en todas las columnas) y ninguna es una lectura nueva. Llegan hasta 23 h por debajo de la marca, así que el filtro las retiene todas y no hay nada que rescatar. El día 2 insertó exactamente 888 filas (37 × 24), ni una más. Un margen de 24 h para cubrir ese retraso haría releer unas 888 filas extra por corrida, el doble de evaluación (sección 9 de la guía), sin recuperar ninguna lectura. El traslape solo se justificaría si los tardíos fueran lecturas nuevas, por ejemplo de un sensor que reporta tras recuperar la señal. Esa es la condición que vigilaríamos.
- **Momento en que se calcula la marca:** con la hora de la lectura (`"timestamp"`), no con la hora de llegada, y por sector. Si la marca usara la hora de llegada, los tardíos sí podrían colarse como nuevos y haría falta traslape.
- **Tamaño de lote y frontera transaccional:** con 37 medidores, cada transacción mueve como mucho 259 filas (S-NORTE del día 2), y la carga incremental de un día tarda unos 0,08 s. No hay riesgo de bloqueos largos que obligue a partir el lote.
- **Frecuencia de carga:** diaria. Coincide con la entrega de la fuente, y el costo de cada corrida depende de $\Delta n$ = 888 filas, no del histórico.

---

## 6. Reproceso de ventana y decisión de clave de deduplicación (rúbrica: evidencia de idempotencia y clave de deduplicación)

**Reproceso de una ventana ya completa (día 1), sin duplicar y sin mover la marca.**

| Momento | Conteo total de `stg_lecturas` |
|---|---|
| Antes de reabrir la ventana | 888 (suma de `consumo_m3`: 141,897) |
| Después de reabrir y recargar la ventana | 888 (suma 141,897; 0 filas nuevas en los cuatro sectores) |

**Prueba del hueco (tres horas de un sector, por ejemplo S-NORTE entre las 10:00 y las 12:59 del día 1).**

| Medición | Valor |
|---|---|
| Filas borradas para abrir el hueco | 30 (10 medidores de S-NORTE × 3 horas): de 888 a 858 |
| Filas recuperadas por la ingesta diaria | 0 (todo el lote queda bajo la marca `2024-06-01 23:00:00`) |
| Filas recuperadas por el reproceso de ventana | 30 (de vuelta a 888 filas y a una suma de 141,897) |
| Filas recuperadas por un segundo reproceso idéntico | 0 |
| Marca de agua del sector antes y después del reproceso | `2024-06-01 23:00:00` / `2024-06-01 23:00:00` |

**Archivo de evidencia del Nivel 2 (obligatorio):** `practica/evidencia_nivel2_equipo-1.md`, generado por la celda de evidencia del cuaderno. Sus cifras coinciden con las dos tablas anteriores.

**Por qué el reproceso no duplica ni mueve la marca:** `reprocesar_ventana` inserta la ventana completa **sin** el filtro de marca y confía solo en `ON CONFLICT (meter_id, "timestamp") DO NOTHING`. Cada fila de la ventana, o ya existe y se ignora, o falta y se inserta: por eso entran exactamente las 30 del hueco y un segundo reproceso no inserta ninguna. Además, el reproceso **no actualiza `estado_ingesta`**, porque rellena el pasado y no cambia hasta dónde llegó la ingesta diaria. Lo que deja fuera son las **correcciones de valores ya cargados**: si la fuente reenvía una lectura con otro `consumo_m3`, `DO NOTHING` conserva el valor viejo. Ese caso exigiría `DO UPDATE` y una regla sobre qué versión prevalece.

**Decisión de clave de deduplicación (nivel 3), sobre la fuente legacy sin marca temporal confiable.**

- **Estrategia elegida:** clave natural compuesta `(meter_id, fecha)`, porque coincide con el grano del negocio: una lectura manual por medidor y día. La huella de fila (SHA-256 de `meter_id|fecha|sector|consumo_m3`) se usa como control: si dos filas comparten clave natural pero tienen huellas distintas, no son una reentrega sino un conflicto, y se mandan a revisión en vez de descartarse. Nuestra conclusión documentada es que **ninguna clave calculada con las columnas disponibles resuelve el caso trampa**, así que la solución definitiva es pedir un campo a la fuente.
- **Caso límite construido o identificado, y qué pasó al probarlo:** usamos la fuente de `generar_fuente_legacy(rng, medidores=6)`, que tiene 9 filas y 8 lecturas legítimas. Incluye una reentrega exacta, el formulario de LEG001 radicado dos veces, y el caso trampa LEG900: dos lecturas legítimas del mismo día con contenido idéntico, consumo 0,0 m³ en S-SUR, de dos rutas de lectura que se solapan. **Las dos estrategias reducen de 9 a 7 filas**: eliminan bien la reentrega de LEG001, pero también colapsan las dos lecturas de LEG900 en una. Pierden **1 lectura legítima de 8**.
- **Riesgo residual que el equipo acepta conscientemente:** aceptamos perder lecturas legítimas indistinguibles antes que conservar duplicados. Un duplicado se suma dos veces en el consumo agregado y sobrefactura al usuario. La lectura perdida del caso trampa vale 0 m³, así que no cambia el consumo total, aunque sí subestima el número de visitas de lectura. Queda como pendiente hacia la fuente pedir el **número de radicado del formulario** o, en su defecto, la **ruta de lectura** y la **hora de captura**. Con ese campo, la reentrega exacta trae el mismo número y se descarta, y las dos rutas traen números distintos y se conservan ambas. El registro de decisión completo está en la celda **Su decisión · Nivel 3** del cuaderno.

---

## 7. Memo del reto de diseño, Emprendimiento Sostenible (rúbrica: memo del reto de diseño)

- **Ruta del memo en el repositorio:** `practica/memo_reto_reanudacion_equipo-1.md`
- **$n_{total}$ (volumen acumulado real en `stg_lecturas` para la instancia del equipo):** 26.640 filas: 30 días × 37 medidores × 24 lecturas por día, a partir de 28.119 filas recibidas.
- **$\Delta n$ (filas nuevas de un lote incremental típico de esta instancia):** 888 filas por día. El lote trae 939; 51 son copias del día anterior que no entran.
- **Costo evitado ($C_{completa} - C_{incremental}$), con las unidades que tengan sentido:** **2,04 s de cómputo por corrida**: 2,119 s de recarga completa frente a 0,083 s de carga incremental, unas 25 veces menos. En filas movidas son **25.752 filas por corrida** (26.640 − 888). Acumulado en 30 noches, son 412.920 filas con recargas completas frente a 26.640 con carga incremental, 15,5 veces menos. Con $c_{fila}$ ≈ 80 µs, una recarga de las 576.000 lecturas del modelo de referencia tardaría unos 46 s, frente a unos 0,08 s de un lote incremental.
- **Estrategia de reanudación ante una caída a mitad de proceso, conectada con la evidencia de las secciones 2 y 3:** se reintenta el mismo lote con `ingest_lote` y cada sector retoma desde su `ultima_marca_agua`. No duplica nada por tres razones:
  - **Transacción:** cada sector carga datos y mueve la marca en una sola transacción, así que una caída deshace las dos cosas. Lo comprobamos con la caída simulada del cuaderno: el conteo y la marca quedaron intactos.
  - **Filtro de marca:** deja fuera lo ya confirmado. En la sección 3, la marca siguió en `2024-06-01 23:00:00` en la segunda corrida.
  - **`ON CONFLICT DO NOTHING`:** absorbe cualquier fila repetida. En la sección 2, el conteo quedó en 888 y 888 y la suma en 141,897 en ambas corridas.

  Así se reanuda donde quedó, en lugar de recargar las 26.640 filas.

---

## 8. Declaración de uso de asistentes de inteligencia artificial

Usamos **Claude Code (Anthropic)** como asistente en estas partes del trabajo:
- Escritura del código de los niveles 2 y 3 y de la celda del reto del cuaderno (`reprocesar_ventana`, prueba del hueco y segundo reproceso, deduplicación por clave natural y por huella, medición de tiempos).
- Escritura del script `ingesta_incremental.py`.
- Primera redacción de los textos de decisión de los niveles 2 y 3, del memo del reto y de este documento.

Todas las cifras salen de ejecutar el cuaderno y el script contra nuestra instancia (`equipo-1`) en PostgreSQL. [completar: describan qué revisaron, ajustaron o redactaron ustedes, por ejemplo la verificación de cifras o la reescritura de los textos de decisión.]

---

## Autoverificación antes de entregar

**Condiciones de aceptación.**

- [x] Las tablas se llaman exactamente `stg_lecturas` y `estado_ingesta`.
- [x] `stg_lecturas` existe y su conteo tras la primera corrida está documentado en la sección 1.
- [x] La prueba de doble ejecución de la sección 2 muestra el mismo conteo antes y después de la segunda corrida.
- [x] `estado_ingesta` refleja la marca de agua actualizada después de cada corrida exitosa, sin quedar en `NULL` ni retroceder.
- [x] El reproceso de ventana de la sección 6 recupera exactamente las filas borradas del hueco, un segundo reproceso recupera 0 y la marca de agua no se movió. El archivo `evidencia_nivel2_<equipo>.md` está adjunto y coincide con esas cifras.
- [ ] El código está versionado en el repositorio del equipo, con el identificador de `--equipo` usado y las instrucciones de la sección 4.

**Rúbrica (calidad de las decisiones documentadas).**

- [x] La bitácora de idempotencia (secciones 2, 3 y 6) incluye una explicación de por qué el resultado demuestra idempotencia, no solo los números.
- [x] La sección 5 declara la decisión tomada sobre la instancia (o la conclusión razonada de no cambiar el script) y la respalda con al menos una cifra propia.
- [x] La sección 6 muestra, para la clave de deduplicación, el caso límite probado contra la clave de deduplicación elegida y el riesgo residual aceptado.
- [x] El memo de la sección 7 usa cifras propias de la instancia del equipo, no la cifra genérica del acueducto.
- [x] Las instrucciones de reproducción de la sección 4 alcanzan para que una persona externa repita la doble ejecución sin preguntar nada.

---

## Referencias

Densmore, J. (2021). *Data pipelines pocket reference: Moving and processing data for analytics*. O'Reilly Media.

Kleppmann, M. (2017). *Designing data-intensive applications: The big ideas behind reliable, scalable, and maintainable systems*. O'Reilly Media.

Reis, J., y Housley, M. (2022). *Fundamentals of data engineering*. O'Reilly Media.
