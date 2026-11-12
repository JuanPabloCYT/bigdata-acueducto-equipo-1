# Glosario ES/EN · lectura de planes

IFPN0025 · Big Data e Ingeniería de Datos · Universidad Ean · equipo-1

| Término (inglés) | Equivalente en español | Definición de una línea |
|---|---|---|
| logical plan | plan lógico | Qué se pide, como árbol de operaciones antes de elegir cómo ejecutarlas; Catalyst lo reescribe (por ejemplo, poda `meter_id` en C.3) sin cambiar el resultado. |
| physical plan | plan físico | Cómo se ejecuta: los nodos concretos (`Scan parquet`, `BroadcastHashJoin`, `Exchange`) que leemos con `explain("formatted")`. |
| predicate pushdown | empuje de predicados | Llevar un filtro hasta el lector de Parquet (`PushedFilters`), que lo usa para saltar grupos de filas; en B se empuja `presion_bar > 3,5`, pero no salta ningún archivo. |
| partition pruning | poda de particiones | Descartar carpetas enteras por las columnas de la ruta (`PartitionFilters`): la consulta A abre 1 de 120 archivos. |
| broadcast join | unión por difusión | Copiar la tabla pequeña a cada tarea para unir sin mover la grande: C difunde los 348 medidores y mezcla solo 724 B. |
| sort-merge join | unión por ordenamiento y mezcla | Repartir los dos lados por la clave, ordenarlos y recorrerlos juntos: C-forzada usa 3 `Exchange` y 2 `Sort` y mueve 7,8 MB. |
| shuffle | mezcla | Redistribuir filas entre tareas según una clave (cada `Exchange`); es lo que más cuesta en red y en disco cuando hay varias máquinas. |
| selectivity | selectividad | Fracción de filas que deja pasar un filtro: el día de A conserva 8 352 de 1 002 240 lecturas (0,83 %). |
