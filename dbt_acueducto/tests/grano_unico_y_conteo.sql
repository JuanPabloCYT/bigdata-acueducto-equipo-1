-- Prueba singular: devuelve filas SOLO si hay un problema (una prueba dbt pasa cuando no devuelve filas).
-- 1) el grano (fecha, sector) no se repite; 2) n_confiables + n_no_confiables = n_lecturas;
-- 3) la suma de n_lecturas es el numero de filas de la preparacion.
with duplicados as (
    select fecha, sector, count(*) as veces
    from {{ ref('consumo_dia_sector') }}
    group by fecha, sector
    having count(*) > 1
),
incoherentes as (
    select fecha, sector
    from {{ ref('consumo_dia_sector') }}
    where n_confiables + n_no_confiables <> n_lecturas
),
descuadre as (
    select 'conteo' as problema
    where (select sum(n_lecturas) from {{ ref('consumo_dia_sector') }}) <> (select count(*) from {{ source('lago', 'preparacion') }})
)
select 'duplicado' as problema from duplicados
union all select 'incoherente' from incoherentes
union all select problema from descuadre
