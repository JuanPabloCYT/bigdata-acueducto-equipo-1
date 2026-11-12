# Memo · Estrategia de reanudación de la ingesta · equipo-1

**Asunto:** cuánto cómputo se evita reanudando la ingesta donde se quedó en lugar de recargar todo el histórico.
**Instancia medida:** `equipo-1` (37 medidores, 2,2 % de reentregas, 3,7 % de tardíos), 30 días desde el 2024-06-01, PostgreSQL 16 en Docker local, base `acueducto_equipo_1`. Cifras producidas por la celda del reto de `BiD_S12_P4_cuaderno_v3.ipynb`.

## Recomendación

Cuando el script se caiga, **reintentar la ingesta incremental**, no recargar el histórico. Con nuestras cifras, reanudar cuesta **0,083 s** y recargar cuesta **2,119 s**: se evitan **2,04 s por corrida**, unas 25 veces menos tiempo. Además, la diferencia crece cada día, porque el histórico aumenta y el lote diario no.

## Cifras medidas

| Medida | Valor |
|---|---|
| $n_{total}$: filas en `stg_lecturas` tras recargar los 30 días | 26.640 filas (37 medidores × 24 h × 30 días) |
| Filas recibidas en los 30 lotes (incluye reentregas y tardíos) | 28.119 filas |
| $\Delta n$: filas nuevas de un lote típico (día 30) | 888 filas (el lote trae 939; 51 son copias del día anterior que no entran) |
| $t_{incremental}$: carga del día 30 con `ingest_lote` | 0,083 s |
| $t_{completa}$: recarga de los 30 días con `reprocesar_ventana` | 2,119 s |
| $c_{fila} = t_{completa} / n_{total}$ | ≈ 80 µs por fila (7,96 × 10⁻⁵ s) |
| $c_{overhead} \approx t_{incremental} - \Delta n \cdot c_{fila}$ | ≈ 0,013 s por corrida |
| **Costo evitado** $= C_{completa} - C_{incremental}$ | **2,04 s de cómputo por corrida**, o 25.752 filas que no se vuelven a mover (26.640 − 888) |
| Proyección de una recarga de 576.000 lecturas (volumen del modelo de referencia) | ≈ 46 s, frente a ≈ 0,08 s de un lote incremental |

## Estrategia de reanudación

**Desde dónde retoma.** Cada sector retoma desde su `ultima_marca_agua` en `estado_ingesta`. El reintento vuelve a leer el lote que falló y solo inserta las filas con `"timestamp"` posterior a esa marca.

**Por qué no duplica.** Hay tres defensas independientes, todas verificadas:

1. **Atomicidad.** Cada lote de cada sector se carga en una sola transacción, que inserta los datos y mueve la marca a la vez. Si el proceso se cae a mitad, se deshace todo. En la caída simulada del Nivel 1, ni el conteo ni la marca cambiaron.
2. **Marca de agua.** El filtro `"timestamp" > ultima_marca_agua` descarta lo ya cargado. La marca nunca retrocede, porque se actualiza con `GREATEST`. En la doble ejecución del script quedó en `2024-06-01 23:00:00` en ambas corridas.
3. **Escritura por clave.** `ON CONFLICT (meter_id, "timestamp") DO NOTHING` absorbe cualquier fila repetida. En la doble ejecución, la primera corrida dejó 888 filas y la segunda también, con la misma suma de consumo (141.897 m³).

**Cuándo sí se recarga.** Solo para rellenar un hueco detectado en una ventana ya cargada, y entonces se reprocesa únicamente esa ventana con `reprocesar_ventana`, sin tocar la marca. En el Nivel 2 eso recuperó 30 de 30 filas, y un segundo reproceso recuperó 0. La recarga completa queda como último recurso, por ejemplo si cambia el esquema o se corrompe `stg_lecturas`.

## Sostenibilidad: lo que significa el costo evitado

El costo de recargar crece con el histórico acumulado ($n_{total}$) y el de la carga incremental solo con el lote del día ($\Delta n$). Con nuestra instancia:

- **En filas:** con recargas completas, en 30 noches se moverían $888 \times (1 + 2 + \dots + 30) = 412.920$ filas. Con carga incremental se mueven $888 \times 30 = 26.640$, es decir, 15,5 veces menos. Con 120 noches la proporción llega a 60,5 veces menos, la misma de la sección 3 de la guía.
- **En cómputo y energía:** cada fila que no se vuelve a mover es CPU, disco y red que no se usan. Para una empresa de servicio público como el acueducto, eso se traduce en servidores más pequeños, menos consumo eléctrico y una factura de nube que crece con el negocio y no con la antigüedad de los datos. Un pipeline que recarga todo cada noche hace que el costo de operar aumente cada día aunque el acueducto no crezca. Reanudar es la opción sostenible, tanto económica como ambientalmente.
- **En riesgo operativo:** una recarga larga deja más tiempo la tabla expuesta a fallos y a consultas sobre datos a medias. La incremental cierra en décimas de segundo.

## Decisiones y supuestos

- **Unidad de costo:** segundos de reloj (`time.perf_counter()`) de punta a punta. Incluyen pasar el lote a la tabla de paso (`to_sql`), el `INSERT ... ON CONFLICT`, los conteos y el commit. Son tiempos en un portátil con Postgres en Docker; en un servidor cambian los valores absolutos, pero no la proporción. Como segunda unidad, independiente de la máquina, usamos las filas movidas.
- **Cómo estimamos $c_{fila}$:** dividimos el tiempo de la recarga completa entre las filas que dejó (26.640). Es una estimación por exceso, porque reparte entre las filas el overhead fijo de 120 transacciones (30 días × 4 sectores). Es aceptable porque una recarga real también paga ese overhead.
- **Medición única:** cada tiempo se midió una sola vez y varía algunos centésimos entre ejecuciones. En otra corrida obtuvimos 0,082 s y 2,284 s, con la misma proporción de unas 25 a 28 veces. Para un informe formal tomaríamos la mediana de varias corridas.
- **Recarga completa sin reiniciar la marca:** la recarga usa `reprocesar_ventana`, que no lee ni mueve la marca, así que medimos solo el costo de mover los datos.
- **Si el volumen crece diez veces** (unos 370 medidores): con un $c_{fila}$ constante, $\Delta n$ pasaría a unas 8.880 filas diarias. Un lote incremental tardaría unos 0,7 s y una recarga de 30 días (266.400 filas) unos 21 s, así que el costo evitado sería de unos 20 s por corrida. Con más días, la diferencia sigue creciendo. A esa escala revisaríamos dos cosas:
  - El índice de la clave primaria crece y encarece cada `ON CONFLICT`. Habría que vigilar que $c_{fila}$ no aumente.
  - `to_sql` fila a fila se volvería el cuello de botella. Lo cambiaríamos por `COPY` para cargar la tabla de paso.

## Cómo reproducir

1. Levantar el perfil: `docker compose --profile s12 up -d`.
2. Ejecutar `BiD_S12_P4_cuaderno_v3.ipynb` completo con `EQUIPO = "equipo-1"`. La celda del reto genera los 30 días en `lotes_equipo-1_30d/`, mide los dos tiempos y la celda siguiente verifica que $t_{completa} > t_{incremental} > 0$ y que $n_{total} > \Delta n > 0$.
