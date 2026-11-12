# ADR-T14 · Captura de cambios del maestro de suscriptores: sondeo por marca temporal más reconciliación cada 6 horas

**Estado:** aceptada · **Fecha:** 2026-11-12 · **Equipo:** equipo-1 · **Autor:** Juan Pablo Castro

## Contexto

La plataforma del acueducto copia el maestro de suscriptores del sistema comercial (`maestro_suscriptores`) a su capa de staging (`stg_cambios_suscriptor`) con una **captura de cambios por consulta**: cada hora pregunta "¿qué filas tienen `updated_at` posterior al último checkpoint?". La gerencia preguntó si ese método basta o si hay que pasar a una captura basada en el registro de transacciones de la base de origen.

La pregunta surge porque el sistema comercial **borra físicamente** a los suscriptores dados de baja (`DELETE`). Una fila borrada ya no tiene `updated_at`, así que ninguna consulta puede devolverla.

## Evidencia (perfil `equipo-1`, ejecutado en PostgreSQL 16)

| Medida | Valor |
|---|---|
| Suscriptores al inicio | 105 |
| Cambios del día | 32: 8 altas, 13 modificaciones y **11 bajas** (`prob_baja_dura` = 0,343) |
| Capturados por la consulta | 8 de 8 altas, 13 de 13 modificaciones y **0 de 11 bajas** |
| Lo que cree el destino frente al origen real | 113 frente a 102: **11 suscriptores fantasma (10,8 % del maestro real)** |
| Cambios que ve la consulta frente a los que entregaría el registro | 21 frente a 32: el **34,4 %** de los cambios es invisible para la consulta |
| Bajas que encuentra una reconciliación de llaves sobre la misma base | **11 de 11** |

Los 11 suscriptores fantasma (SUS-0000003, -006, -009, -013, -033, -045, -055, -059, -070, -078 y -083) siguen "vivos" para cualquier proceso que lea el destino: se les seguiría facturando. Es una **propiedad estructural** del método, no un error del prototipo. Las altas y modificaciones llegan completas; las bajas no pueden llegar nunca.

## Decisión

**Estrategia híbrida.** Se **mantiene** la captura por consulta cada hora (T = 3 600 s) para altas y modificaciones, y se **agrega una reconciliación de llaves cada 6 horas** (B = 4 barridos al día). Cada barrido compara el conjunto de `subscriber_id` del destino con el del origen. Toda llave ausente en el origen se registra en `stg_cambios_suscriptor` como `tipo_cambio = 'baja'`, con `INSERT ... ON CONFLICT DO NOTHING` sobre `(subscriber_id, tipo_cambio, updated_at)`.

- **Intervalo de sondeo:** 1 h. La facturación es mensual y una modificación que llega con una hora de atraso no cambia ningún cobro. Bajar a 5 s multiplicaría la carga sobre el origen por 720 (de 24 a 17 280 consultas al día) sin beneficio para el negocio.
- **Margen de solapamiento:** 0 s, con el checkpoint avanzando solo hacia adelante (`GREATEST`). El riesgo de perder un cambio con `updated_at` igual al checkpoint lo cubre la reconciliación, que también compara `updated_at` y no solo las llaves. No necesitamos releer una ventana en cada sondeo.

## Alternativas descartadas y su costo

| Alternativa | Detecta bajas | Costo recurrente sobre el origen | Latencia máx. de una baja | Costo de adopción |
|---|---|---|---|---|
| Bandera de baja lógica en el origen | Sí | 24 consultas al día (el sondeo actual) | 1 h | **Alto, y lo paga otro equipo:** cambiar el `DELETE` del sistema comercial por un `UPDATE`, más todas sus consultas e informes que asumen que "existe = activo". Tampoco cubre las bajas que ya ocurrieron ni los borrados hechos por otra vía (scripts de soporte). |
| **Reconciliación cada 6 h (elegida)** | Sí | 28 consultas al día y **420 llaves leídas al día** (105 × 4) | 6 h | **Bajo:** una consulta de llaves y una diferencia de conjuntos. Lo opera el equipo de datos y no toca el sistema comercial. |
| CDC por registro de transacciones | Sí, con todos los eventos | 0 consultas de sondeo, pero **más escritura de WAL** | Unos 5 s | **Alto:** `wal_level = logical` exige reiniciar el servidor de origen. La ranura de replicación retiene WAL si el decodificador se detiene, con riesgo de llenar el disco del origen. Además hay que operar y vigilar un servicio nuevo (decodificador y conector). Para un maestro de 105 filas es un costo desproporcionado. |

## Justificación por costo, latencia e impacto sobre el origen

- **Costo:** la reconciliación lee 420 llaves al día, menos que una sola consulta de un informe comercial. La captura por registro tendría un costo fijo de operación que no depende del volumen y que con este tamaño de maestro no se justifica.
- **Latencia:** 6 h es suficiente para una facturación mensual. Lo que causaba el daño no era la demora, sino que **la baja no se detectaba nunca**.
- **Impacto sobre el origen:** cero cambios en el sistema comercial y cero reinicios. La carga adicional son 4 consultas al día sobre el índice de la llave primaria.
- **Cuándo revisaríamos la decisión:** si el maestro creciera hacia los 400 000 suscriptores del acueducto real, la reconciliación leería 1,6 millones de llaves al día, y si el negocio exigiera ver las bajas en minutos (por ejemplo, para cortar el servicio). En ese escenario la captura por registro, con `REPLICA IDENTITY DEFAULT`, ya que para una baja basta con la llave primaria, pasa a ser la opción razonable.

**Verificación de control que acompaña a la decisión:** después de cada barrido, el conteo de suscriptores vivos en el destino (altas − bajas acumuladas) debe ser igual a `SELECT COUNT(*) FROM maestro_suscriptores`. Si difiere, se dispara una alerta.

## Consecuencias

- Durante un máximo de 6 h el destino puede tener suscriptores fantasma. Ningún proceso de cobro debe ejecutarse entre barridos sin correr antes una reconciliación.
- Un suscriptor que se da de alta y de baja entre dos barridos no queda en ninguna de las dos fotos. Se detecta cruzando las altas capturadas por el sondeo con las llaves del barrido siguiente.
- **Datos personales:** si el maestro fuera real y contuviera datos personales, la Ley 1581 de 2012 exige suprimir los datos del titular cuando se da de baja y lo solicita. Detectar la baja dejaría de ser opcional y la latencia aceptable se acortaría, lo que inclinaría la decisión hacia la baja lógica con supresión programada o hacia la captura por registro.

*Este memo no descalifica la decisión de otros equipos: con otro perfil (por ejemplo, `prob_baja_dura` cercana a 0,05 y un maestro grande) otra alternativa bien documentada puede ser la correcta.*
