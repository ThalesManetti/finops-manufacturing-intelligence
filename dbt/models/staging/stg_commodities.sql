-- =============================================================================
-- Model: stg_commodities
-- Camada: Silver (Staging)
-- Fonte: bronze.raw_commodities
-- Descrição: Índices de commodities limpos e tipados
-- =============================================================================

with source as (
    select * from {{ source('bronze', 'raw_commodities') }}
),

cleaned as (
    select
        cast(mes as date) as mes_referencia,
        extract(year from cast(mes as date)) as ano,
        extract(month from cast(mes as date)) as mes,
        upper(trim(cast(commodity as string))) as commodity,
        round(cast(indice as numeric), 4) as indice,
        _ingested_at

    from source
    where mes is not null
)

select * from cleaned
