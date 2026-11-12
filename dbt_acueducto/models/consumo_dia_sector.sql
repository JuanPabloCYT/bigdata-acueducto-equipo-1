{{ config(
    materialized='external',
    location='/proyecto/_dbt/consumo_dia_sector',
    format='parquet',
    options={'partition_by': 'anio, mes, dia', 'overwrite_or_ignore': true}
) }}
-- Producto de consumo del tablero financiero: una fila por dia y sector (misma regla que el SQL de Spark).
-- La preparacion conserva todas las lecturas; la regla de confiabilidad se aplica aqui.
select
    fecha,
    sector,
    count(*)::bigint as n_lecturas,
    sum(case when quality_flag = 'OK' then 1 else 0 end)::bigint as n_confiables,
    sum(case when quality_flag = 'OK' then 0 else 1 end)::bigint as n_no_confiables,
    sum(consumo_m3) as consumo_total_m3,
    sum(case when quality_flag = 'OK' then consumo_m3 end) as consumo_confiable_m3,
    anio,
    mes,
    dia
from {{ source('lago', 'preparacion') }}
group by anio, mes, dia, fecha, sector
