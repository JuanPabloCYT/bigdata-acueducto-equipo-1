# Documento de decisión · dónde se transforma y qué se conserva entre capas

Equipo: `@@EQUIPO@@` · Volumen: ampliación del equipo

Este documento responde a la pregunta de la dirección financiera: **¿conviene seguir transformando antes de guardar o guardar primero y transformar adentro?** Se decide por producto, no para todo el pipeline, y cada decisión se sostiene con una cifra de su propia bitácora (`bitacora_capas.jsonl`) y con la cifra que la haría cambiar.

## Las ocho decisiones posibles

Elijan al menos **tres**, entre ellas la 1, la 2 y la 4. Cada una que elijan se escribe como un encabezado `## Decisión N` (N es el número de esta tabla) y sigue el formato de la sección siguiente.

| N | Decisión | Opciones | Criterios para decidir | Consecuencia de decidir mal |
|---|---|---|---|---|
| 1 | Dónde transformar cada producto | ETL por producto · ETL de varias salidas en una pasada · ELT sobre la preparación · híbrido | Productos actuales y futuros, frecuencia de cambio de reglas, necesidad de auditar, sensibilidad del dato, bytes | Pagar relecturas completas, o retener lo que no debía aterrizar |
| 2 | En qué capa vive cada regla | Preparación · consolidada · ambas | Si descarta o reinterpreta, y con qué frecuencia cambia | Reprocesar toda la historia por un cambio menor, o reglas que divergen |
| 3 | Qué se retiene en la preparación | Todo con bandera · cuarentena aparte · descarte | Costo de almacenar, riesgo de no poder explicar, sensibilidad | Datos inexplicables, o almacenamiento sin límite |
| 4 | Cómo se reprocesa | Ventana por partición con sobrescritura dinámica · rehacer toda la capa · sobrescritura estática | Tamaño de la ventana, dependencias, idempotencia | Perder particiones o duplicar filas |
| 5 | Granularidad de la partición de salida | Por día · por mes · sin partición | Ventana típica de reproceso, tamaño y cantidad de archivos | Reescribir un mes por un día, o 120 archivos de 4 filas (unos 2 KB cada uno) |
| 6 | Qué dice el contrato y dónde se verifica | Esquema · grano · invariantes · momento · acción al fallar | Qué necesita quien consume y cuánto cuesta verificar | Alteraciones silenciosas que llegan al tablero |
| 7 | Cómo se expresa y se versiona la transformación | SQL declarativo · DataFrame · función propia · herramienta de modelado | Legibilidad, plan resultante, versionado | Lógica dispersa en cuadernos sin versión |
| 8 | Qué no debe aterrizar (Nivel 3) | Descartar en el borde · cuarentena · retener con bandera | Costo de lamentarse, almacenamiento, norma | Perder evidencia, o retener lo que no debía |

## Formato de cada decisión

    ## Decisión N · título corto
    - Contexto: qué producto o regla y qué cifra de su bitácora lo muestra.
    - Opciones consideradas: al menos dos, con su costo medido o calculado.
    - Decisión: qué eligen.
    - Alternativa descartada y por qué.
    - Costo de la decisión (bytes, archivos, almacenamiento, complejidad).
    - Cifra que la revisaría: qué valor, en qué dirección, cambiaría la decisión.

## Decisión de una etapa anterior revisada con la evidencia de hoy

Elijan una decisión de la capa refinada o de la lectura de planes (por ejemplo, un archivo por día, o `es_confiable` junto a `quality_flag`) y respondan: qué decía, qué cifra de hoy la confirma o la cambia, y qué la reemplaza. Si ninguna cambió, digan cuál evaluaron y por qué la mantienen, con una cifra.

_(Su texto aquí)_

## Decisiones

_(Agreguen aquí los encabezados `## Decisión N` que elijan.)_
