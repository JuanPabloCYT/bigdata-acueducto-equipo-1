# Memo para Facturación · Estado de medidores incompleto en la corrida de prueba

**De:** Ingeniería de datos, plataforma del acueducto (equipo-1)
**Para:** Área de Facturación
**Asunto:** 19 de 99 medidores no se pudieron confirmar en la corrida de prueba con fallo parcial

---

## 1. Qué pasó

Cada noche, un programa le pide al sistema del proveedor de facturación el estado de los 99 medidores de nuestra instancia: saldo y estado del servicio (activo, en mora o suspendido). El proveedor no los entrega de una sola vez, sino en **páginas** de 16 medidores, como las hojas de un listado, para un total de 7 páginas.

En esta corrida de prueba simulamos una noche en la que el proveedor estaba sobrecargado y pedimos cada página **una sola vez**, sin volver a intentarlo si fallaba. Dos páginas no llegaron:

- **La página 4 (medidores MED0049 a MED0064, 16 medidores).** El proveedor respondió "demasiadas solicitudes, intente más tarde".
- **La página 7 (medidores MED0097 a MED0099, 3 medidores).** La misma respuesta.

Resultado: **80 medidores confirmados y 19 sin confirmar**, repartidos en los cuatro sectores: 5 en S-NORTE, 5 en S-SUR, 5 en S-ESTE y 4 en S-OESTE.

Ninguno de los dos fallos indica un dato malo. Es un fallo **pasajero** del proveedor: si se vuelve a preguntar unos segundos después, normalmente responde. En la operación normal el programa vuelve a preguntar hasta 6 veces por página y espera cada vez un poco más (1, 2, 4, 8 y 16 segundos). En esta prueba quitamos esa protección a propósito, para ver qué recibiría Facturación en el peor caso.

## 2. Qué NO se debe asumir

**Que un medidor no aparezca en el listado de esta noche no significa que esté al día.** Significa que *no sabemos* su estado. Según la fuente, entre esos 19 medidores hay **2 en mora** y **3 suspendidos**, con un saldo total de **$1.314.791,81**. Si Facturación tratara a los ausentes como "sin novedades", podría:

- dejar de cobrar saldos en mora,
- reactivar o facturar normalmente servicios que están suspendidos, o
- usar el estado de la noche anterior como si fuera el de hoy.

Les pedimos que **no tomen decisiones de cobro, corte o reconexión sobre esos 19 medidores** hasta que se confirme su estado. La lista exacta va en el anexo.

## 3. Qué se va a hacer y cómo sabrán que el lote quedó completo

1. **Volvemos a pedir solo las páginas que faltaron** (4 y 7), con la protección normal de hasta 6 intentos por página. Con el nivel de fallos de nuestro proveedor (28 % de las solicitudes fallan en el primer intento), la probabilidad de que una página no llegue después de 6 intentos es de 5 en 10 000, y la de que una corrida completa quede incompleta es de 3 en 1 000.
2. **Cada intento queda registrado** en una bitácora: qué página, a qué hora, qué respondió el proveedor y cuánto tardó. Así cualquiera puede comprobar qué se pidió y qué llegó.
3. **El lote está completo solo cuando se cumplen dos condiciones a la vez:** el programa informa que la corrida terminó como **COMPLETA** y la tabla de estado tiene **99 de 99 medidores**. Les avisaremos cuando ambas se cumplan. Hasta entonces, el lote de esta noche debe tratarse como **provisional**.
4. **Si el proveedor sigue caído** (varias páginas seguidas que agotan sus 6 intentos), no insistiremos más esa noche: reprogramaremos la corrida y se lo comunicaremos con la misma lista de medidores pendientes.

---

### Anexo · Medidores sin confirmar

| Página | Medidores |
|---|---|
| 4 | MED0049, MED0050, MED0051, MED0052, MED0053, MED0054, MED0055, MED0056, MED0057, MED0058, MED0059, MED0060, MED0061, MED0062, MED0063, MED0064 |
| 7 | MED0097, MED0098, MED0099 |

*Cifras de la celda "Reto: cifras para el memo" de `BiD_S13_P4_cuaderno_v2.ipynb`, ejecutada contra el perfil `equipo-1` (corrida con un intento por página, base `acueducto_equipo_1_reto`).*
