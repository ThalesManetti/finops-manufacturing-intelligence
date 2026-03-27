-- =============================================================================
-- Model: stg_clientes
-- Camada: Silver (Staging)
-- Fonte: bronze.raw_clientes
-- Descrição: Cadastro de clientes limpo e padronizado
-- =============================================================================

with source as (
    select * from {{ source('bronze', 'raw_clientes') }}
),

cleaned as (
    select
        cast(cliente_id as string) as cliente_id,
        upper(trim(cast(cliente_nome as string))) as cliente_nome,
        upper(trim(cast(tipo as string))) as tipo_cliente,
        round(cast(peso_volume as numeric), 4) as peso_volume,
        _ingested_at

    from source
    where cliente_id is not null
)

select * from cleaned
