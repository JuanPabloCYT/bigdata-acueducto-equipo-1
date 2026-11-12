## 8. Nota para la gerencia (criterio 6 de la rúbrica · Power Humanise)

- **Ruta de la nota en el repositorio:** `T16_refinada_dataframes/nota_gerencia_t16.md` (el mismo texto de esta sección).

**Pregunta de la dirección que responde esta nota:** ¿podemos confiar en la nueva capa de lecturas para el cierre de facturación, y conviene usar programas propios para clasificar la presión del agua?

**Respuesta en una frase:** sí. La nueva capa cuadra al centavo con las tres referencias que ya teníamos, y la clasificación de presión debe hacerse con la herramienta que trae el motor, que da el mismo resultado y en nuestras mediciones fue entre 4 y 11 veces más rápida que un programa propio.

*Términos:* **capa refinada**: la versión limpia y ordenada de las lecturas, de donde salen los cierres. **Reconciliar**: comprobar, grupo por grupo, que dos fuentes independientes dan los mismos totales. **Esquema**: la lista de columnas con su tipo de dato (texto, número, fecha). **Función propia**: un programa escrito por el equipo, que el motor no sabe optimizar. **Partición por día**: guardar cada día en su propia carpeta, para poder rehacer un día sin tocar los demás.

| Cifra propia y de dónde sale (campo de la bitácora o tabla de la evidencia) | Qué significa para el negocio |
|---|---|
| **1 440 de 1 440** grupos iguales a la capa anterior y **480 de 480** iguales a los totales de control, con una diferencia máxima de 0,0 m³ (`detalle_heredada`, `detalle_control`) | El cierre que sale de la nueva capa es el mismo que el actual: no hay riesgo de facturar distinto por el cambio. |
| Reprocesar 6 días con el modo correcto conserva los **114 días restantes**; con el modo incorrecto se **pierden 547 200 lecturas** (tabla `Reproceso de 6 dias`) | Corregir un día con datos malos no pone en riesgo el resto del histórico: el error operativo más costoso queda bloqueado por diseño. |
| Clasificar la presión con la herramienta del motor: **0,066 s** frente a **0,736 s** con un programa propio por fila (11,2 veces), con resultados idénticos (`Variantes de clasificacion`) | Hoy son fracciones de segundo, pero el costo crece con cada medidor: con 100 000 medidores, el programa propio haría 2,4 millones de llamadas por cierre diario. |
| **0** filas con datos inválidos y **0** valores faltantes en 1 002 240 lecturas (`filas_no_conformes`, `nulos_por_columna`) | Si la fuente cambia de formato, el sistema lo detecta y lo cuenta antes de publicar, en lugar de producir un cierre equivocado sin aviso. |

**Recomendación:** adoptar la capa refinada con esquema declarado, partición por día y modo de reproceso que solo reemplaza los días corregidos, y clasificar la presión con la expresión nativa del motor. No se recomiendan funciones propias.

**Riesgo que se acepta:** las mediciones de tiempo se hicieron en un solo computador, con el motor en modo local; los tiempos absolutos pueden variar hasta 3 veces de una ejecución a otra. Decidimos por la diferencia que se repitió en todas las ejecuciones (la columna en memoria), no por una cifra aislada.

**Cifra que cambiaría la recomendación:** que una regla de negocio para clasificar no pudiera escribirse con la herramienta del motor (por ejemplo, un modelo estadístico externo). Entonces se usaría la función vectorizada, que en nuestras mediciones costó 4,6 veces la nativa, frente a las 11,2 veces de la función por fila.

**Lo que no sabemos todavía:** cómo se comporta el tiempo con 100 000 medidores reales, porque la cifra de 2,4 millones de llamadas es un cálculo lineal y no una medición; si la fuente real del acueducto cambia de formato con frecuencia; y cuánto cuesta operar esto en un clúster en lugar de una máquina.

**Dos preguntas difíciles de la dirección y su respuesta.**

1. **"¿Por qué confiar en tres corridas?"** No confiamos en una cifra aislada: en dos ejecuciones completas, el orden entre los dos programas propios **se invirtió** sobre el CSV (1,80 frente a 1,48 s en una; 0,84 frente a 0,94 s en la otra), y por eso no declaramos ganador entre ellos. Lo que sí se repitió en todas las ejecuciones, sin solape de rangos, es que la herramienta del motor es la más rápida, entre 4,4 y 11,2 veces con los datos en memoria. Con tres corridas no podemos dar un intervalo de confianza formal; para eso harían falta decenas de corridas.
2. **"¿Qué pasa si la fuente reordena las columnas un viernes?"** La lectura **falla y se detiene** ("CSV header does not conform") en lugar de cargar datos cruzados. En una prueba con dos columnas intercambiadas, sin esa protección, el consumo de un sector salió en 3 601,5 m³ en lugar de 190,7 m³, sin ningún error. El cierre de ese día se retrasa hasta corregir la fuente, pero nunca se publica un consumo 19 veces mayor. No sabemos con qué frecuencia cambia la fuente real.

**Términos técnicos usados y dónde se definen en la nota:** capa refinada, reconciliar, esquema, función propia y partición por día, en el párrafo "Términos", justo después de la respuesta. Modo local, en el párrafo de riesgo, explicado como "un solo computador".

---

