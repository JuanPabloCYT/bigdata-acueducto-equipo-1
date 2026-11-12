# T13 · El conector de ingesta de su equipo, hecho para no perder ni duplicar un medidor cuando la API falla a medias

**Equipo:** equipo-1 (trabajo individual)
**Integrantes:** Juan Pablo Castro
**Proyecto:** Plataforma de datos del acueducto
**Identificador de equipo (`--equipo`):** `equipo-1`
**Motor usado:** PostgreSQL 16 (perfil `s12` de Docker Compose, contenedor `acueducto_postgres_s12`), base propia del equipo `acueducto_equipo_1`
**Puerto de la API simulada:** cuaderno: 8013 (Nivel 1), 8014 (Nivel 2) y 8015 (reto), con la variable `API_PUERTO` sin definir (valor predeterminado 8013). Script del conector: puerto 8014, con `--puerto 8014`.
**Fecha:** 2026-11-12

---

## 0. Perfil de fallos y latencia del equipo

- **Comando exacto usado para iniciar la API simulada:** `python iniciar_api_simulada.py --equipo equipo-1 --puerto 8014`, sin `--acelerar`, con la latencia real del perfil. Desde `practica/` del paquete base S13. En el cuaderno, la celda del Nivel 2 lo arranca con `iniciar_api(PUERTO_EQUIPO, equipo=EQUIPO)`.
- **Qué trae ese perfil:** `gem.parametros_equipo("equipo-1")` reporta `total_registros` = **99**, `page_size` = **16** (es decir, **7 páginas**), `prob_429` = **0,141**, `prob_5xx` = **0,139** y latencia inyectada de **157,4 ms** de base más un jitter de hasta 26,2 ms por página. La probabilidad de que falle un intento es p = 0,141 + 0,139 = **0,28**.

---

## 1. El conector y las dos tablas

- **Ruta del conector en el repositorio:** `T13_conector_api/conector_api_medidores.py`. Es la misma lógica del cuaderno `T13_conector_api/BiD_S13_P4_cuaderno_v2.ipynb`, con `--equipo`, `--puerto`, `--doble` y `--token`.
- **Nombre de la tabla de estado vigente:** `stg_estado_medidor`. Es el nombre exacto, con clave primaria `meter_id`.
- **Nombre de la tabla de auditoría:** `bitacora_ingesta_api`. Es el nombre exacto.
- **Filas cargadas en `stg_estado_medidor` tras la primera corrida:** **99**, igual a `total_registros`, sin páginas incompletas.
- **Total de páginas reportadas por la API bajo este perfil:** **7**. Las páginas 1 a 6 traen 16 medidores y la 7 trae 3; la página 7 responde `hay_siguiente = false`.

---

## 2. La prueba de fallo parcial y bitácora (rúbrica: evidencia de fallo parcial y bitácora)

| Momento | Conteo de `stg_estado_medidor` | Conteo de `bitacora_ingesta_api` |
|---|---|---|
| Antes de la primera corrida | 0 | 0 |
| Después de la primera corrida | 99 | 9 (la corrida aportó 9 intentos) |
| Después de la segunda corrida, mismo perfil | 99 | 19 (la corrida aportó 10 intentos) |

**Archivo de evidencia del Nivel 2 (obligatorio):** `T13_conector_api/evidencia_nivel2_equipo-1.md`, generado por la celda de evidencia del cuaderno. Sus cifras coinciden con la tabla. El script reproduce las mismas cifras en `T13_conector_api/bitacora_doble_corrida_equipo-1.txt` (99 → 99 y 9 → 19, `RESULTADO: OK`).

**Estado con que terminó cada corrida (`estado_corrida`) y qué significa para el lote:** las dos terminaron `COMPLETA`. Eso quiere decir que el conector siguió `hay_siguiente` hasta la página 7 (`false`), sin topes de presupuesto ni de páginas, y que ninguna página agotó sus intentos ni falló de forma permanente. Además, `stg_estado_medidor` = 99 = `total_registros`. Solo con esas dos condiciones juntas declaramos el lote completo: una lista vacía de páginas incompletas no basta, porque una corrida `PRESUPUESTO_AGOTADO` también puede tenerla vacía sin haber llegado al final.

**Comando o celda exacta usada para correr el conector dos veces:** en el cuaderno, la celda "Corridas del equipo: reporte por página y segunda corrida" (Nivel 2). Con el script: `python conector_api_medidores.py --equipo equipo-1 --puerto 8014 --reiniciar --doble`.

**Por qué esta comparación demuestra la diferencia entre estado vigente y auditoría:** las dos tablas responden a preguntas distintas y por eso se comportan distinto ante la misma repetición. `stg_estado_medidor` responde "¿cuál es hoy el estado de cada medidor?". Se escribe con `INSERT ... ON CONFLICT (meter_id) DO UPDATE`, así que la segunda corrida actualiza las mismas 99 filas en lugar de agregar otras 99: es idempotente respecto al **resultado**. `bitacora_ingesta_api` responde "¿qué se le pidió al proveedor, cuándo y qué contestó?". Cada intento es un hecho distinto, aunque pida la misma página, así que se registra con un `INSERT` simple y nunca se deduplica. Si la segunda corrida no hubiera agregado filas, habríamos perdido la evidencia de que en la corrida 2 la página 1 recibió un 503 que la corrida 1 no tuvo. El crecimiento distinto de cada corrida (9 y 10 intentos) muestra además que la bitácora registra lo que pasó de verdad y no una copia de la corrida anterior.

**Páginas que quedaron con resultado `AGOTADO` en las corridas del nivel 2 (si las hubo), y en qué intento:** ninguna. Todas las páginas reintentadas se recuperaron en el intento 2. En la corrida 1: las páginas 4 y 7 (429 → 200). En la corrida 2: las páginas 1 y 3 (503 → 200) y la página 2 (429 → 200). Las únicas páginas `AGOTADO` aparecen en la corrida del reto (sección 6), hecha con un solo intento por página.

---

## 3. Clasificación de códigos reintentables (rúbrica: clasificación de códigos reintentables)

| Código o situación | Clasificación (reintentable / permanente) y justificación | Evidencia en `bitacora_ingesta_api` del equipo |
|---|---|---|
| 429 | **Reintentable.** El proveedor no rechaza la solicitud: pide bajar el ritmo. Repetirla después de esperar suele funcionar, y si trae `Retry-After`, esa espera manda. | Corrida 1: página 4, intento 1, y página 7, intento 1 (429 → 200 en el intento 2). Corrida 2: página 2, intento 1. Reto: páginas 4 y 7, intento 1 (`AGOTADO` con un solo intento). |
| 5xx (503, y por regla todo 500 a 599) | **Reintentable.** Es un error del proveedor, no de nuestra solicitud. Un 503 ("servicio no disponible") es pasajero por definición. Tratamos igual el resto de los 5xx porque no tenemos evidencia de un 5xx que no se recupere. | Corrida 2: página 1, intento 1, y página 3, intento 1 (503 → 200 en el intento 2). |
| Sin respuesta (timeout o error de red) | **Reintentable.** No hubo respuesta, así que no hay nada que diga que la solicitud es inválida. Un GET de una página es idempotente: repetirlo no tiene efectos. Queda con `codigo_http` vacío. | Prueba del Nivel 1 en `acueducto_demo_fallos` (tiempo de espera de 1 ms y 3 intentos): `(1, NULL, REINTENTABLE)`, `(2, NULL, REINTENTABLE)` y `(3, NULL, AGOTADO)`. En las corridas del equipo no se presentó. |
| 4xx distintos de 429 y de 401 (404, y por regla 400, 403, 422 y demás) | **Permanente solo para esa página.** El proveedor dice que la solicitud misma está mal, o que la página no existe: repetirla daría lo mismo. Se registra `FALLO_PERMANENTE` y la corrida sigue con la página siguiente. | Reto: página 8, intento 1, código 404, `FALLO_PERMANENTE`. Como la página 7 se perdió con su `hay_siguiente`, el conector pidió la 8 y el 404 le confirmó que ya no había más datos. Ese 404 no se cuenta como página incompleta. En las corridas del Nivel 2 no se presentó. |
| 401 (aborta la corrida) | **Aborta toda la corrida.** La credencial es la misma para todas las páginas: si falla en una, falla en todas. El intento queda con `resultado = FALLO_AUTENTICACION` y la corrida con `estado_corrida = ABORTADA_AUTENTICACION`. Lo cargado antes queda cargado. | Prueba con token inválido, `python conector_api_medidores.py --equipo equipo-1 --puerto 8014 --token token-invalido` (`bitacora_prueba_401_equipo-1.txt`): página 1, intento 1, 401, `FALLO_AUTENTICACION`, y la corrida `ABORTADA_AUTENTICACION` con 1 solicitud. Tratarlo como transitorio habría costado 30 solicitudes en el ejemplo del Nivel 1 (6 páginas × 5 intentos). |

Los códigos reintentables son los que el equipo clasificó como tales, con justificación escrita: no hay una lista fija. Los demás 4xx se registran por página y la corrida continúa; el 401 es la excepción y aborta la corrida.

**Opciones consideradas para la clasificación:**
- **408 (Request Timeout) como permanente o como transitorio.** Elegimos **transitorio**. El 408 dice que el servidor se cansó de esperar la solicitud, no que la solicitud sea inválida. Es el mismo fenómeno que una solicitud sin respuesta, que ya reintentamos, y el GET es idempotente. Tratarlo como permanente nos haría perder una página entera por una congestión momentánea.
- **Reintentar el 500 con un tope más bajo que el 503.** Lo descartamos. Un 500 puede ser un fallo de programación del proveedor, que no se arregla solo, pero no tenemos ningún caso en nuestra bitácora para calibrar un tope distinto: nuestro simulador solo produjo 503. Preferimos una regla uniforme y vigilar la bitácora. Si aparecieran 500 que agotan los 6 intentos de forma repetida, revisaríamos la decisión.
- **Reintentar el 404 por si es un fallo pasajero.** Lo descartamos. En esta API, el 404 significa "página fuera de rango", y el conector lo usa para cerrar la corrida cuando perdió el `hay_siguiente` de la página anterior.

**Riesgo residual de esta clasificación:** que un 5xx sea en realidad permanente (por ejemplo, un 500 por un error en el servidor). En ese caso gastaríamos 6 intentos y unos 31 s por página para terminar igual en `AGOTADO`. Se detectaría en la bitácora como la misma página con `codigo_http` = 500 en los 6 intentos y resultado final `AGOTADO`, corrida tras corrida. El riesgo inverso, que un 4xx sea pasajero, nos haría perder una página que sí se habría recuperado. Se detectaría como `FALLO_PERMANENTE` en una página que en la corrida siguiente sí responde 200.

**Uso de la cabecera `Retry-After`:** el perfil la generó con cada 429: `Retry-After: 2` sin `--acelerar` y `1` con `--acelerar`. La función de espera la respeta por encima del cálculo propio: si la excepción trae `retry_after`, la espera es ese valor y no `min(t_max, t0·2ⁿ) + jitter`. Por eso, en las páginas 4 y 7 de la corrida 1, la espera tras el 429 fue de 2 s y no de 1 a 2 s con jitter. Se ve en `timestamp_intento`: entre el intento 1 (429) y el intento 2 (200) de la página 4 pasaron 2,20 s, y en la página 7 pasaron 2,21 s, es decir, los 2 s de `Retry-After` más unos 200 ms de latencia de la página. Entre páginas sin fallo pasan unos 0,21 s.

---

## 4. Parámetros de retroceso exponencial (rúbrica: parámetros de retroceso exponencial)

- **`t0` (base, en segundos):** 1
- **`t_max` (techo, en segundos):** 30
- **`max_intentos`:** 6
- **Espera acumulada máxima por página con esos valores:** 6 intentos dejan 5 esperas: 1 + 2 + 4 + 8 + 16 = **31 s** sin jitter. Con jitter U(0, t0), la suma puede crecer hasta 31 + 5 × 1 = **36 s**, con una espera individual de hasta 17 s, por debajo de `t_max` + `t0` = 31 s. El techo de 30 s no llega a actuar con 6 intentos: es la protección para quien suba `max_intentos`. En el peor caso de las 7 páginas son 7 × 36 = 252 s, y por eso el script usa un presupuesto blando de 300 s (el cuaderno usa 180 s). El presupuesto se evalúa antes de pedir cada página, así que la página en curso termina primero.
- **Probabilidad de que una página agote sus intentos con el perfil propio:** p = 0,141 + 0,139 = 0,28, y p⁶ = 0,28⁶ = **0,000482**, es decir, 0,048 %. Para la corrida de 7 páginas, 1 − (1 − 0,000482)⁷ = **0,34 %**.
- **Justificación, conectada con `prob_429` y `prob_5xx` del perfil propio:** nuestro perfil falla en el 28 % de los intentos, más de uno de cada cuatro. Con los 5 intentos de la guía, la corrida queda incompleta en el 1,2 % de las noches, unas 4 al año en una corrida diaria. Con 6 intentos baja a 0,34 %, alrededor de una al año, a cambio de solo 16 s más de espera por página en el peor caso. La espera esperada casi no cambia: con p = 0,28, la mayoría de las páginas sale en el primer o el segundo intento, como se ve en el reporte (máximo 2 intentos en las dos corridas). La comparación está en la celda del Nivel 3 del cuaderno, calculada con `simular_backoff`.
- **Opciones consideradas y por qué se descartaron** (mismo criterio: P(corrida incompleta) frente a espera máxima por página):

  | `t0` | `t_max` | `max_intentos` | Espera sin jitter | Espera máx. | P(agota página) | P(corrida incompleta) |
  |---|---|---|---|---|---|---|
  | 1 | 30 | 3 | 3 s | 5 s | 2,2 % | 14,4 % |
  | 1 | 30 | 5 (guía) | 15 s | 19 s | 0,17 % | 1,20 % |
  | **1** | **30** | **6 (elegida)** | **31 s** | **36 s** | **0,048 %** | **0,34 %** |
  | 1 | 30 | 8 | 91 s | 98 s | 0,0038 % | 0,026 % |
  | 0,5 | 10 | 8 | 35,5 s | 39 s | 0,0038 % | 0,026 % |
  | 2 | 60 | 6 | 62 s | 72 s | 0,048 % | 0,34 % |

  - **3 intentos:** demasiado riesgo, 1 de cada 7 corridas incompleta.
  - **8 intentos con techo de 30 s:** reduce el riesgo a 0,026 %, pero triplica el peor caso por página (98 s) y llevaría el peor caso de la corrida a unos 11 minutos.
  - **`t0` = 0,5 s, techo de 10 s, 8 intentos:** da la misma probabilidad con poca espera, pero reintenta a un ritmo que contradice el propósito del 429: si el proveedor pide bajar el ritmo, ráfagas de 0,5 s y 1 s lo sobrecargan más. Además, con una caída sostenida pasaría más rápido por todos los intentos sin dar tiempo a la recuperación.
  - **`t0` = 2 s, techo de 60 s, 6 intentos:** mismo riesgo que la elegida, con el doble de espera.
- **Riesgo residual:** el cálculo supone que los intentos son independientes. Si el proveedor cae de forma sostenida (por ejemplo, 5 minutos), los fallos dejan de ser independientes: cada página gasta sus 6 intentos (36 s) y termina `AGOTADO`, y tras unas 8 páginas se agota el presupuesto. En la bitácora se vería como varias páginas seguidas con 6 intentos, todas con 503, resultado `AGOTADO` y la corrida en `CON_PAGINAS_INCOMPLETAS` o `PRESUPUESTO_AGOTADO`. Ante ese patrón no conviene subir `max_intentos`, sino reprogramar la corrida y avisar a Facturación (sección 6).

---

## 5. Instrucciones de reproducción (rúbrica: reproducibilidad y trazabilidad)

Requisitos: Docker Desktop, Python 3.11 o superior y el paquete base `BiD_S13_proyecto_base_v2.zip`, que no se versiona en este repositorio.

1. Extraer el paquete base: `unzip BiD_S13_proyecto_base_v2.zip -d ~/proyectos/s13 && cd ~/proyectos/s13`.
2. Levantar PostgreSQL: `docker compose --profile s12 up -d` y esperar a que `docker compose --profile s12 ps` muestre `acueducto_postgres_s12` como `healthy`.
3. Instalar las dependencias: `python -m venv .venv && source .venv/bin/activate && pip install -r practica/requirements.txt`.
4. Copiar el conector a la carpeta de la práctica: `cp <este repo>/T13_conector_api/conector_api_medidores.py practica/` y entrar con `cd practica`.
5. **Terminal 1.** Iniciar la API simulada con el perfil del equipo: `python iniciar_api_simulada.py --equipo equipo-1 --puerto 8014`. Debe quedar escuchando en `http://127.0.0.1:8014`. Para comprobarlo: `curl http://127.0.0.1:8014/salud` → `{"estado":"arriba","equipo":"equipo-1","total_registros":99,"page_size_defecto":16,...}`.
6. **Terminal 2.** Correr el conector dos veces: `python conector_api_medidores.py --equipo equipo-1 --puerto 8014 --reiniciar --doble`. El script crea la base `acueducto_equipo_1` y las dos tablas si no existen. `--reiniciar` es destructivo: vacía las dos tablas.
7. Salida esperada, determinista con un proceso de API recién iniciado:
   - `Corrida 1: COMPLETA`, con las páginas 4 y 7 en 2 intentos y `Tras corrida 1: stg_estado_medidor=99 · bitacora_ingesta_api=9`.
   - `Corrida 2: COMPLETA`, con las páginas 1, 2 y 3 en 2 intentos y `Tras corrida 2: stg_estado_medidor=99 · bitacora_ingesta_api=19`.
   - `RESULTADO: OK` y código de salida 0.
8. Prueba del 401: `python conector_api_medidores.py --equipo equipo-1 --puerto 8014 --token token-invalido` → `Corrida 1: ABORTADA_AUTENTICACION`, con una sola fila `401 FALLO_AUTENTICACION`.
9. Cuaderno: copiar `BiD_S13_P4_cuaderno_v2.ipynb` a `practica/` y ejecutarlo completo (`EQUIPO = "equipo-1"`). Usa los puertos 8013, 8014 y 8015, así que antes hay que detener la API del paso 5. La última celda debe mostrar las diez comprobaciones en `OK`.

Como el simulador decide cada respuesta con una semilla fija por (equipo, página, número de intento), quien repita estos pasos con un proceso de API recién iniciado obtiene exactamente los mismos códigos y conteos.

---

## 6. Memo del reto de diseño, Power Humanise (rúbrica: memo del reto de diseño)

- **Ruta del memo en el repositorio:** `T13_conector_api/memo_reto_diseno_t13.md`
- **Cifras de la corrida del reto:** **80 de 99** medidores cargados y **19 sin confirmar**, con las páginas **4 y 7 `AGOTADO`** (429 en su único intento). Los sectores afectados son S-ESTE (5), S-NORTE (5), S-OESTE (4) y S-SUR (5). Según la fuente, entre los 19 hay 2 en mora y 3 suspendidos, con un saldo total de $1.314.791,81. La página 8 respondió 404 y marcó el fin de los datos.
- **Qué pasó:** el memo explica qué es una "página" (una hoja de 16 medidores del listado del proveedor) y que el proveedor contestó "demasiadas solicitudes, intente más tarde" en dos de ellas. Aclara que en la prueba se pidió cada hoja una sola vez, sin volver a preguntar, y que por eso faltaron 19 medidores. También explica que en la operación normal se vuelve a preguntar hasta 6 veces, esperando cada vez un poco más.
- **Qué NO se debe asumir:** que un medidor ausente esté "al día". El memo dice que ausente significa "no sabemos". Muestra con cifras que entre los ausentes hay medidores en mora y suspendidos, y pide no tomar decisiones de cobro, corte o reconexión sobre esos 19 medidores, que lista en un anexo.
- **Qué se va a hacer:** volver a pedir solo las páginas 4 y 7 con la protección normal. El criterio de cierre que se comunica es doble: la corrida debe terminar **COMPLETA** y la tabla debe tener **99 de 99** medidores. Hasta entonces, el lote se trata como provisional. Si el proveedor sigue caído, se reprograma la corrida y se avisa con la misma lista.

---

## 7. Declaración de uso de asistentes de inteligencia artificial

Usamos **Claude Code (Anthropic)** como asistente en estas partes del trabajo:
- Escritura del código de las celdas "Su turno" del cuaderno: `reporte_por_pagina`, la segunda corrida, `mi_clasificacion`, `simular_backoff`, la comparación de configuraciones y las cifras del reto.
- Escritura del script `conector_api_medidores.py`, a partir de las funciones del cuaderno.
- Primera redacción de los registros de decisión de los Niveles 2 y 3, del memo para Facturación y de este documento.

Todas las cifras salen de ejecutar el cuaderno y el script contra el perfil `equipo-1` en PostgreSQL. La revisión de las cifras y de los textos es responsabilidad del autor.

---

## Autoverificación antes de entregar

**Condiciones de aceptación.**

- [x] Las tablas se llaman exactamente `stg_estado_medidor` y `bitacora_ingesta_api`.
- [x] El conector recorre todas las páginas reportadas bajo el perfil del equipo, no duplica registros de medidor y reconcilia el conteo con el total que informa la API (99 = 99), y el archivo `evidencia_nivel2_equipo-1.md` (obligatorio) está adjunto.
- [x] Se reintentan únicamente los códigos que el equipo clasificó como reintentables, con justificación; los demás 4xx se registran en la bitácora y detienen solo el reintento de esa página; un 401 aborta la corrida.
- [x] El retroceso exponencial tiene techo (`t_max` = 30 s) y número máximo de intentos (6) documentados.
- [x] `bitacora_ingesta_api` permite reconstruir, para cada página, intentos, código y duración.
- [x] El memo del reto de diseño está versionado en el repositorio del equipo.
- [x] El código está versionado con el identificador de `--equipo` usado y las instrucciones de la sección 5.

**Rúbrica (calidad de las decisiones documentadas).**

- [x] La sección 2 explica, no solo muestra, por qué la comparación demuestra las dos semánticas de idempotencia.
- [x] La sección 3 respalda la clasificación de códigos con al menos un caso real de la bitácora del equipo, y presenta opciones consideradas y riesgo residual.
- [x] La sección 4 conecta los parámetros elegidos con `prob_429` y `prob_5xx` propias del perfil del equipo, y presenta opciones consideradas y riesgo residual.
- [x] El memo de la sección 6 cubre los tres elementos exigidos, con las cifras de la corrida del reto, sin jerga técnica sin explicar.
- [x] Las instrucciones de reproducción de la sección 5 alcanzan para que alguien externo repita la corrida sin preguntar nada.

---

## Referencias

Densmore, J. (2021). *Data pipelines pocket reference: Moving and processing data for analytics*. O'Reilly Media.

Nygard, M. T. (2011, 15 de noviembre). *Documenting architecture decisions*. Cognitect Blog. https://www.cognitect.com/blog/2011/11/15/documenting-architecture-decisions

Nygard, M. T. (2018). *Release it!: Design and deploy production-ready software* (2.ª ed.). Pragmatic Bookshelf.

Reis, J., y Housley, M. (2022). *Fundamentals of data engineering*. O'Reilly Media.
