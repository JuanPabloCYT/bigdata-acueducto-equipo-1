# T14 · El memo que decide, con las bajas duras de su propio equipo, si un SELECT con marca temporal alcanza para el maestro de suscriptores

**Equipo:** equipo-1 (trabajo individual)
**Integrantes:** Juan Pablo Castro
**Proyecto:** Plataforma de datos del acueducto (proyecto integrador)
**Identificador de equipo (`--equipo`):** `equipo-1`
**Motor usado:** PostgreSQL 16 (perfil `s12` del archivo de servicios, contenedor `acueducto_postgres_s12`), base propia del equipo `acueducto_equipo_1`
**Fecha:** 2026-11-12

---

## 0. Perfil de cambios del equipo

- **Comando exacto usado para generar el perfil de cambios:** `python generar_cambios_suscriptores.py --equipo equipo-1`, que escribe `maestro_suscriptores.csv` y `flujo_cambios.csv`. El cuaderno usa las mismas funciones con `gen.poblacion_inicial(equipo="equipo-1")` y `gen.flujo_cambios(equipo="equipo-1")`.

| Valor del perfil | Cifra que reporta el generador |
|---|---|
| `total_suscriptores` | 105 |
| Número de cambios | 32 |
| `n_alta` | 8 |
| `n_modificacion` | 13 |
| `n_baja` | 11 |
| `prob_baja_dura` | 0,343 |

---

## 1. Las tres tablas nuevas y su nomenclatura

- **Ruta del cuaderno o script en el repositorio:** `T14_cdc_suscriptores/BiD_S14_P4_cuaderno_v2.ipynb`, ejecutado completo con `EQUIPO = "equipo-1"`.
- **Nombre de la tabla fuente:** `maestro_suscriptores`. Es el nombre exacto.
- **Nombre de la tabla de staging de cambios capturados:** `stg_cambios_suscriptor`. Es el nombre exacto.
- **Nombre de la tabla de checkpoint:** `estado_cdc_suscriptores`. Es el nombre exacto, con una fila por equipo.
- **Filas cargadas en `maestro_suscriptores` tras la población inicial:** 105.

---

## 2. Evidencia cuantitativa del punto ciego (rúbrica: evidencia cuantitativa del punto ciego)

| Tipo de cambio | Generadas (perfil del equipo) | Capturadas por el prototipo |
|---|---|---|
| Altas | 8 | 8 |
| Modificaciones | 13 | 13 |
| Bajas duras | 11 | **0** |

**Archivo de evidencia del Nivel 2 (obligatorio):** `T14_cdc_suscriptores/evidencia_nivel2_equipo-1.md`, generado por el cuaderno. Sus cifras coinciden con la tabla.

**Comando o celda exacta usada para correr las dos corridas del prototipo (antes y después):** la celda "Instancia del equipo" del Nivel 2 hace la población inicial y la **corrida 1** (`correr_prototipo_cdc(engine_equipo, equipo_etiqueta=EQUIPO)`, que captura 105 altas y lleva el checkpoint a `2024-06-01 06:00:00`). La celda "Corridas del equipo: flujo en vivo, segunda corrida y evidencia del punto ciego" aplica el flujo con `aplicar_flujo_cambios(engine_equipo, df_eventos_eq, gen.FECHA_CAMBIOS)` y hace la **corrida 2** (8 altas, 13 modificaciones y 0 bajas; checkpoint en `2024-06-02 09:00:00`). El staging pasa de 105 a 126 filas.

**Diferencia entre lo que ve el destino y lo que hay en el origen:** el destino cree que hay 105 + 8 = **113** suscriptores y el origen tiene **102** (105 + 8 − 11): **11 fantasmas**, el 10,8 % del maestro real.

**Por qué el resultado de bajas capturadas es una propiedad estructural del método, no un error del prototipo:** la captura por consulta solo puede devolver filas que **existen** y cumplen `updated_at > checkpoint`. Un `DELETE` físico elimina la fila junto con su `updated_at`, así que no queda nada que una cláusula `WHERE` pueda seleccionar. Que el prototipo esté bien lo prueban las otras dos filas de la tabla: captura el 100 % de las altas y de las modificaciones, y una tercera corrida sin cambios no mueve ni el checkpoint ni el staging. El 0 de 11 no depende del código, del intervalo de sondeo ni del margen de solapamiento: se repetiría con cualquier implementación del método. Solo cambia si cambia el método o si el origen deja de borrar.

**Si el perfil del equipo generó cero bajas duras por redondeo:** no aplica. Nuestro perfil generó 11 bajas, con la `prob_baja_dura` más alta del rango (0,343). Es el caso opuesto: 1 de cada 3 cambios es una baja invisible.

### 2.1 Repetición con el checkpoint restablecido (Nivel 2, obligatoria)

- **Filas duplicadas en `stg_cambios_suscriptor` tras repetir la corrida:** **21** (de 126 a 147), exactamente las 8 altas más las 13 modificaciones del perfil.
- **Filas sobrantes por pareja `subscriber_id` y `tipo_cambio`:** **21**.
- **Checkpoint tras la repetición:** `2024-06-02 09:00:00`. Se restableció a `2024-06-01 06:00:00` y la repetición lo devolvió a la fecha del flujo de cambios. Las bajas capturadas siguieron en 0.
- **Párrafo de decisión sobre cómo evitar los duplicados:** el staging es de solo inserción y no tiene clave única, así que cualquier relectura duplica. Elegimos dos barreras:
  - `GREATEST(checkpoint, :nuevo)` al avanzar el checkpoint, para que un error de operación no lo mueva hacia atrás.
  - Como garantía real, una **clave única `(subscriber_id, tipo_cambio, updated_at)` con `INSERT ... ON CONFLICT DO NOTHING`**. Esta protege también contra un restablecimiento intencional o un reproceso de ventana, que es justo lo que `GREATEST` no cubre.

  El par (`subscriber_id`, `tipo_cambio`) no basta: un suscriptor puede tener dos modificaciones legítimas en días distintos y la segunda se descartaría. Por eso la clave incluye `updated_at`. Descartamos el margen de solapamiento con deduplicación posterior porque deja duplicados temporales en el staging y traslada el problema a cada consumidor. Lo que aceptamos: dos modificaciones del mismo suscriptor con el mismo `updated_at` colapsarían en una. Ese caso lo detectaría la reconciliación de la sección 3.

---

## 3. Decisión de mitigación del nivel 3 (rúbrica: decisión de mitigación del nivel 3)

| Alternativa | ¿Elegida? | Costo estimado | Beneficio |
|---|---|---|---|
| Bandera de baja lógica en el origen | No | 24 consultas al día (el sondeo horario de siempre), pero un costo de adopción alto en **otro equipo**: cambiar el `DELETE` del sistema comercial por un `UPDATE ... activo = false` y revisar todas las consultas e informes que asumen "existe = activo". No recupera las bajas ya ocurridas ni cubre los borrados hechos por otra vía. | Latencia máxima de 1 h; la baja llega como una modificación más, sin componentes nuevos en el pipeline. |
| Reconciliación periódica de conjunto completo | **Sí** (híbrida: sondeo horario más 4 barridos al día) | 28 consultas al día (24 + 4) y **420 llaves leídas al día** (105 × 4). Sin cambios en el origen. | Detectó **11 de 11** bajas sobre nuestra base, con una latencia máxima de 6 h. Lo opera el equipo de datos. |
| Migración a CDC basada en registro de transacciones | No | 0 consultas de sondeo, pero `wal_level = logical` (reinicio del servidor de origen), más escritura de WAL, una ranura de replicación que retiene WAL si el consumidor cae y un decodificador nuevo que operar y vigilar. | Ve los 32 de 32 eventos, con unos 5 s de latencia. |

**Cálculos de apoyo** (con las cuatro funciones de costo del cuaderno y M = `total_suscriptores` = 105):
- Sondeo: N = 86 400 / 3 600 = **24 consultas al día**. A 5 s serían 17 280, 720 veces más carga.
- Reconciliación: K = M × B = 105 × 4 = **420 llaves al día**. Con B = 1 serían 105 y con B = 24, 2 520. Con el maestro real de 400 000 suscriptores: 400 000 con B = 1 y 1 600 000 con B = 4.
- Latencia máxima: sondeo, L = T = 3 600 s (1 h). Reconciliación, 86 400 / 4 = **21 600 s (6 h)**. Registro, unos 5 s (retraso supuesto del decodificador).

**`bajas_detectadas` por reconciliación sobre la base de su equipo:** 11. Son `['SUS-0000003', 'SUS-0000006', 'SUS-0000009', 'SUS-0000013', 'SUS-0000033', 'SUS-0000045', 'SUS-0000055', 'SUS-0000059', 'SUS-0000070', 'SUS-0000078', 'SUS-0000083']`, igual a las bajas generadas por el perfil.

**Tabla `comparacion` con las tres alternativas:**

```
     alternativa  detecta_bajas  intervalo_sondeo_s  barridos_por_dia  consultas_por_dia  llaves_leidas_por_dia  latencia_max_baja_s
     baja_logica           True                3600                 0               24.0                      0               3600.0
  reconciliacion           True                3600                 4               28.0                    420              21600.0
cdc_por_registro           True                   0                 0                0.0                      0                  5.0
```

**Justificación de la alternativa elegida, conectada con `prob_baja_dura` y el volumen de bajas propios del equipo:**
- **Por qué hay que cubrir las bajas:** con `prob_baja_dura` = 0,343, un tercio de los cambios diarios son bajas (11 de 32) y dejan un 10,8 % de fantasmas en un solo día. Si no se cubren, el error se acumula día tras día.
- **Por qué la reconciliación:** el maestro es pequeño (105 llaves), así que barrerlo completo 4 veces al día cuesta 420 llaves, una cantidad despreciable. Además, la reconciliación es la única alternativa que no exige tocar el sistema de origen.
- **Por qué alcanza una latencia de 6 h:** la facturación es mensual. El daño lo causaba que la baja no se viera nunca, no que se viera tarde.
- **Si el perfil fuera otro:** con un `prob_baja_dura` cercano a 0,05 y un maestro de 400 000 filas, la misma reconciliación leería 1,6 millones de llaves al día para encontrar muy pocas bajas, y la captura por registro empezaría a ser preferible.

**Costo estimado de las dos alternativas descartadas:**
- *Baja lógica:* su costo recurrente es bajo, pero su costo de adopción no lo controla el equipo de datos: modificar el sistema comercial, migrar sus consultas y negociar el cambio con otra área. Además, deja sin cubrir las 11 bajas que ya ocurrieron.
- *CDC por registro:* reiniciar el servidor de origen para activar `wal_level = logical`, el riesgo de que la ranura de replicación llene el disco del origen si el decodificador se detiene, y un servicio más que operar las 24 horas. Es un costo fijo desproporcionado para 105 filas y 32 cambios al día.

**Solo si una alternativa es la CDC basada en registro de transacciones:** no aplica, porque no la elegimos. Si se adoptara más adelante, bastaría `REPLICA IDENTITY DEFAULT`, porque para registrar una baja el destino solo necesita la llave primaria.

---

## 4. Memo de decisión ADR, Emprendimiento Sostenible (rúbrica: memo de decisión ADR)

- **Ruta del memo en el repositorio:** `T14_cdc_suscriptores/memo_adr_t14.md`
- **Contexto:** se evalúa la fuente `maestro_suscriptores` del sistema comercial, que se copia al staging con una captura por consulta cada hora. La decisión la detonó la pregunta de la gerencia: ¿basta ese método o hay que migrar a la captura por registro? El origen borra físicamente las bajas.
- **Evidencia:** de 11 bajas generadas (`prob_baja_dura` = 0,343) se capturaron **0**. Las altas (8 de 8) y las modificaciones (13 de 13) llegaron completas. El destino cree que hay 113 suscriptores y hay 102: **11 fantasmas (10,8 %)**. La consulta ve 21 de 32 cambios (el 34,4 % es invisible) y la reconciliación encontró 11 de 11 bajas.
- **Decisión:** **estrategia híbrida:** mantener la captura por consulta cada hora y agregar una reconciliación de llaves cada 6 horas para las bajas.
- **Alternativas descartadas y su costo:** la baja lógica, con un costo de adopción alto en el sistema comercial, que paga otra área, y sin cobertura de las bajas pasadas. La captura por registro, con el reinicio del origen, el riesgo de retención de WAL y un decodificador que operar.
- **Justificación por costo, latencia e impacto sobre el sistema de origen:** 420 llaves al día y 28 consultas, una latencia máxima de 6 h suficiente para una facturación mensual, y cero cambios o reinicios en el origen. Se acompaña de una verificación de control: después de cada barrido, el conteo del destino debe ser igual al del origen.
- **Intervalo de sondeo y margen de solapamiento elegidos, si aplica, y por qué:** sondeo de 1 h, porque la facturación es mensual y bajar a 5 s multiplicaría la carga sobre el origen por 720 sin beneficio. Margen de 0 s con checkpoint monótono (`GREATEST`): el caso límite de `updated_at` igual al checkpoint lo cubre la reconciliación, que compara también `updated_at`.
- **Datos personales (opcional):** con datos personales reales, la Ley 1581 de 2012 obliga a suprimir los datos del titular que se da de baja y lo solicita. Detectar la baja sería obligatorio y la latencia aceptable se acortaría, lo que inclinaría la decisión hacia la baja lógica con supresión programada o hacia la captura por registro.

---

## 5. Integración con el proyecto integrador (rúbrica: integración con el proyecto integrador)

**Cómo se compara el patrón de cambio de `maestro_suscriptores` con el de `stg_lecturas` (T12, solo agrega filas) y `stg_estado_medidor` (T13, estado paginado vía API):**

| Fuente | Patrón de cambio | Cómo se captura | Costo recurrente (equipo-1) | ¿Hay punto ciego? |
|---|---|---|---|---|
| `stg_lecturas` (T12) | Solo agrega: una lectura nunca se modifica ni se borra | Marca de agua por `timestamp` y `ON CONFLICT DO NOTHING` | Proporcional a Δn = 888 filas al día; 0,08 s por corrida | No. Los tardíos quedan bajo la marca, pero el reproceso de ventana los recupera. |
| `stg_estado_medidor` (T13) | Estado que se sobrescribe; la fuente es una API externa sin acceso a su base | Extracción **completa** de las 7 páginas en cada corrida, con `UPSERT` por `meter_id` | 99 registros en 7 solicitudes, más unos 2,7 reintentos esperados por corrida con p = 0,28 (7 × 0,28 / 0,72) | No para las bajas: como se lee el conjunto completo, un medidor ausente se puede detectar comparando conjuntos, lo mismo que hace nuestra reconciliación. |
| `maestro_suscriptores` (T14) | Altas, modificaciones y **bajas físicas** | Consulta incremental por `updated_at` más reconciliación de llaves cada 6 h | 28 consultas y 420 llaves al día | Sí, en la consulta sola (0 de 11 bajas). La reconciliación lo cierra. |

La conclusión es que el patrón de cambio de la fuente decide el método. T12 puede ser puramente incremental porque su fuente solo agrega. T13 ya lee el conjunto completo en cada corrida, así que en la práctica reconcilia sin costo extra. T14 es la única fuente donde una marca temporal no alcanza, y por eso es la única que agrega una pieza nueva. Su costo, 420 llaves al día, está en el mismo orden de magnitud que las 888 filas diarias de T12 y las 99 de T13: no cambia el presupuesto del proyecto.

**Por qué el memo no descalifica la decisión del equipo si difiere de la de referencia, aunque esté bien documentada:** el memo justifica la decisión con las cifras propias del perfil `equipo-1` (105 suscriptores y `prob_baja_dura` = 0,343). También dice de forma explícita en qué condiciones otra alternativa sería la correcta: un maestro de 400 000 filas, pocas bajas o la necesidad de verlas en minutos. Y cierra recordando que otra decisión bien documentada, con otro perfil, es igualmente válida. No compara contra la decisión de referencia ni la presenta como "la respuesta".

---

## 6. Instrucciones de reproducción (rúbrica: reproducibilidad y trazabilidad)

Requisitos: Docker Desktop, Python 3.11 o superior y el paquete base `BiD_S14_proyecto_base_v2.zip`, que no se versiona en este repositorio.

1. Extraer el paquete: `unzip BiD_S14_proyecto_base_v2.zip -d ~/proyectos/s14 && cd ~/proyectos/s14`.
2. Levantar PostgreSQL desde cero: `docker compose --profile s12 down -v` (solo si había un volumen anterior), luego `docker compose --profile s12 up -d`, y esperar a que `docker compose --profile s12 ps` muestre `acueducto_postgres_s12` como `healthy`.
3. Instalar las dependencias: `python -m venv .venv && source .venv/bin/activate && pip install -r practica/requirements.txt`.
4. Generar el perfil de cambios del equipo (opcional; el cuaderno lo genera en memoria con las mismas funciones): `cd practica && python generar_cambios_suscriptores.py --equipo equipo-1`. Salida esperada: `total_suscriptores=105, n_cambios=32, prob_baja_dura=0.343` y `altas=8, modificaciones=13, bajas=11`.
5. Copiar el cuaderno `T14_cdc_suscriptores/BiD_S14_P4_cuaderno_v2.ipynb` a `practica/` y ejecutarlo completo: `jupyter nbconvert --to notebook --execute --inplace BiD_S14_P4_cuaderno_v2.ipynb`, o abrirlo con `jupyter lab` y elegir "Run All". `EQUIPO = "equipo-1"` ya está fijado. El cuaderno crea la base `acueducto_equipo_1` y la deja en su punto de partida (población inicial).
6. Salida esperada en las celdas del Nivel 2:
   - **Corrida 1:** `Primera corrida: 105 altas (toda la poblacion) · stg_cambios_suscriptor: 105 filas · checkpoint: 2024-06-01 06:00:00`.
   - **Flujo y corrida 2:** `Flujo aplicado: {'altas_aplicadas': 8, 'modificaciones_aplicadas': 13, 'bajas_aplicadas': 11}`, la tabla `alta 8/8/0 · modificacion 13/13/0 · baja 11/0/11` y `El destino cree que hay 113 suscriptores; el origen tiene 102: 11 fantasmas.`.
   - **Repetición:** `stg antes: 126 · despues: 147 · duplicadas: 21 · sobrantes por pareja: 21 · checkpoint final: 2024-06-02 09:00:00`.
   - **Nivel 3:** `Reconciliacion sobre la base de equipo-1: 11 bajas detectadas`.
   - **Celda final:** las once comprobaciones en `OK`.
7. Se genera `practica/evidencia_nivel2_equipo-1.md`, idéntico al versionado en este repositorio.

---

## 7. Declaración de uso de asistentes de inteligencia artificial

Usamos **Claude Code (Anthropic)** como asistente en estas partes del trabajo:
- Escritura del código de las celdas "Su turno" del cuaderno: la tabla de generadas frente a capturadas, la segunda corrida, la repetición con el checkpoint restablecido, las cuatro funciones de costo, la reconciliación, la comparación y las cifras del reto.
- Primera redacción de los textos de decisión (contraste con la referencia, Nivel 2, repetición y Nivel 3), del memo ADR y de este documento.

Todas las cifras salen de ejecutar el cuaderno contra el perfil `equipo-1` en PostgreSQL. La revisión de las cifras y de los textos es responsabilidad del autor.

---

## Autoverificación antes de entregar

**Condiciones de aceptación.**

- [x] Las tablas se llaman exactamente `maestro_suscriptores`, `stg_cambios_suscriptor` y `estado_cdc_suscriptores`.
- [x] El prototipo se ejecuta contra el perfil propio del equipo (`--equipo`) y deja evidencia cuantitativa de altas, modificaciones y bajas generadas frente a las capturadas, con el archivo `evidencia_nivel2_equipo-1.md` (obligatorio) adjunto.
- [x] La repetición con el checkpoint restablecido (sección 2.1, obligatoria) queda ejecutada, con las filas duplicadas contadas y el párrafo de decisión sobre cómo evitarlas.
- [x] El nivel 3 (sección 3) incluye las cuatro funciones de costo, `bajas_detectadas` y la tabla `comparacion` de las tres alternativas.
- [x] El memo cuantifica al menos un caso de baja dura no detectada, con el dato numérico propio del equipo.
- [x] El memo declara una decisión explícita (mantener, migrar o híbrida) con justificación por costo, latencia e impacto sobre el sistema de origen.
- [x] El memo no descalifica la decisión del equipo por diferir de la de referencia.
- [x] El código y el memo están versionados con el identificador de `--equipo` usado y las instrucciones de la sección 6.

**Rúbrica (calidad de las decisiones documentadas).**

- [x] La sección 2 muestra la doble corrida y explica por qué el resultado es estructural, no un error.
- [x] La sección 3 justifica la alternativa de mitigación elegida con las cifras propias del perfil y estima el costo de las descartadas.
- [x] La sección 4 conecta la decisión del memo con la evidencia cuantitativa propia y pondera costo, latencia e impacto sobre el sistema de origen.
- [x] La sección 5 sitúa el costo de la CDC del maestro de suscriptores junto al de `stg_lecturas` y `stg_estado_medidor`.
- [x] Las instrucciones de reproducción de la sección 6 alcanzan para que otra persona repita la corrida sin preguntar nada.

---

## Referencias

Congreso de la República de Colombia. (2012). *Ley Estatutaria 1581 de 2012, por la cual se dictan disposiciones generales para la protección de datos personales*. Diario Oficial No. 48.587.

Nygard, M. T. (2011, 15 de noviembre). *Documenting architecture decisions*. Cognitect. https://www.cognitect.com/blog/2011/11/15/documenting-architecture-decisions

PostgreSQL Global Development Group. (s. f.). *ALTER TABLE*. En *PostgreSQL documentation*. Recuperado el 30 de septiembre de 2026, de https://www.postgresql.org/docs/current/sql-altertable.html
