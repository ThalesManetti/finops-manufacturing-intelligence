-- =============================================================================
-- Model: stg_orcamento
-- Camada: Silver (Staging)
-- Fonte: bronze.raw_orcamento
-- Descrição: Orçamento anual limpo e tipado
-- =============================================================================

with source as (
    select * from {{ source('bronze', 'raw_orcamento') }}
),

cleaned as (
    select
        cast(orcamento_id as string) as orcamento_id,
        cast(ano as int64) as ano,
        cast(mes as int64) as mes,
        upper(trim(cast(tipo as string))) as tipo,
        trim(cast(natureza as string)) as natureza,
        upper(trim(cast(centro_custo_tipo as string))) as centro_custo_tipo,
        round(cast(valor_orcado as numeric), 2) as valor_orcado,
        _ingested_at

    from source
    where orcamento_id is not null
)

select * from cleaned
