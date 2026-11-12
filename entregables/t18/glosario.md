# Glosario ES/EN · ETL frente a ELT

IFPN0025 · Big Data e Ingeniería de Datos · Universidad Ean · equipo-1

Equivalente en español y definición de una línea para cada término. El glosario es acumulativo: estos ocho se suman a los de T15, T16 y T17.

| Término en inglés | Equivalente en español | Definición |
|---|---|---|
| extract, transform, load (ETL) | extraer, transformar, cargar | Transformar antes de guardar: el tablero heredado lee la cruda (54,4 MB), descarta lo no confiable y guarda solo 480 filas agregadas. |
| extract, load, transform (ELT) | extraer, cargar, transformar | Guardar primero todo y transformar adentro: los productos se calculan con SQL sobre la preparación, leyendo 1,38 MB de columnas. |
| staging table | tabla de preparación | La capa intermedia que conserva todas las lecturas con su bandera (`refinada_spark/`), de donde salen los productos. |
| data contract | contrato de datos | Acuerdo verificable entre capas (columnas, grano, invariantes); `cruda_a_preparacion` falló con 8 352 nulos en el día alterado. |
| idempotent | idempotente | Que repetirlo deja el mismo resultado: dos reprocesos dinámicos de la ventana dieron el mismo hash (`00fc866148`). |
| backfill | reproceso histórico (relleno hacia atrás) | Volver a calcular días ya publicados, por ejemplo la ventana del 2024-01-09 al 2024-01-14, sin tocar el resto. |
| overwrite mode (dynamic and static) | modo de sobrescritura (dinámico y estático) | El dinámico reemplaza solo las particiones que se escriben (6 de 120); el estático borra la tabla entera (perdió 114). |
| lineage | linaje | El rastro de qué capa y qué regla produjo cada cifra: del CSV a `refinada_spark/` y de ahí a `consolidada_elt/` con un SQL versionado. |
