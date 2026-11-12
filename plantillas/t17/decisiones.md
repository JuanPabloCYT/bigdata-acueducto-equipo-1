# Registro de decisiones · lectura de planes · equipo @@EQUIPO@@

IFPN0025 · Big Data e Ingeniería de Datos · Universidad Ean

Una decisión por sección, con el formato de registro de decisiones de arquitectura (Nygard, 2011): un texto breve con contexto, opciones, evidencia, decisión, alternativa descartada con su costo, consecuencias y la cifra que obligaría a revisarla. Justifiquen **al menos tres** decisiones de esta lista, entre ellas la 1 y la 4, con cifras de **su** bitácora. Una decisión distinta de la de otro equipo no es incorrecta si está bien documentada.

| # | Decisión | Opciones |
|---|---|---|
| 1 | Diseño físico de la capa para la consulta dominante (el cierre diario) | Partición por día, por mes o sin partición; columnas de partición como texto con ceros |
| 2 | Forma de los filtros y tipo de las columnas de partición | Columna de partición, columna derivada, función o `cast` sobre la ruta, rango de `timestamp`; literal del tipo correcto |
| 3 | Qué columnas se leen | `select` explícito o todas |
| 4 | Estrategia de unión y umbral de difusión | Dejar a Catalyst, pista, umbral global, referencia filtrada a la versión vigente |
| 5 | Qué se confía al optimizador | Configuración por omisión, pistas, ejecución adaptativa |
| 6 | Qué evidencia se guarda de un plan | Plan normalizado, métricas y hash del resultado; captura de pantalla; tiempos |
| 7 | Unión con clave sesgada (Nivel 3, opcional) | Catalyst, mezcla forzada, sufijo aleatorio |

## Decisión 1 · Diseño físico de la capa

- **Contexto.**
- **Opciones consideradas.**
- **Evidencia propia** (una cifra de su bitácora, con la línea de donde sale).
- **Decisión.**
- **Alternativa descartada y su costo** (medido o calculado, con el supuesto).
- **Consecuencias.**
- **Cifra que la revisaría.**
- **Revisión de una decisión anterior con la evidencia de hoy** (qué decía la decisión de escritura de la etapa anterior, qué cifra la cambió y qué la reemplaza; o, si ninguna cambió, cuál evaluaron y por qué la mantienen con una cifra).

## Decisión 4 · Estrategia de unión y umbral

- **Contexto.**
- **Opciones consideradas.**
- **Evidencia propia** (plan elegido, bytes de mezcla, tamaño estimado frente al tamaño real de la difusión, resultado idéntico).
- **Decisión.**
- **Alternativa descartada y su costo.**
- **Consecuencias.**
- **Cifra que la revisaría.**

## Decisión 2 · Forma de los filtros y tipos de partición

- **Contexto.**
- **Opciones consideradas.**
- **Evidencia propia.**
- **Decisión.**
- **Alternativa descartada y su costo.**
- **Consecuencias.**
- **Cifra que la revisaría.**

(Agreguen una sección `## Decisión N` por cada decisión adicional que justifiquen.)

## Referencias

Nygard, M. T. (2011, 15 de noviembre). *Documenting architecture decisions*. Cognitect. https://www.cognitect.com/blog/2011/11/15/documenting-architecture-decisions
