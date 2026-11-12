-- Prueba singular: devuelve filas SOLO si hay un problema (una prueba dbt pasa cuando no devuelve filas).
-- Ninguna lectura de la preparacion debe llegar sin consumo: un nulo aqui es un valor que no se pudo leer.
select meter_id, "timestamp"
from {{ source('lago', 'preparacion') }}
where consumo_m3 is null
