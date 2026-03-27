-- =============================================================================
-- Model: stg_despesas
-- Camada: Silver (Staging)
-- Fonte: bronze.raw_despesas
-- Descrição: Despesas operacionais limpas e classificadas para DRE
-- =============================================================================

with source as (
    select * from {{ source('bronze', 'raw_despesas') }}
),

cleaned as (
    select
        -- PK
        cast(despesa_id as string) as despesa_id,

        -- Datas
        cast(mes_referencia as date) as mes_referencia,
        extract(year from cast(mes_referencia as date)) as ano,
        extract(month from cast(mes_referencia as date)) as mes,

        -- Classificação (padronização)
        upper(trim(cast(centro_custo_tipo as string))) as centro_custo_tipo,
        trim(cast(natureza as string)) as natureza,
        upper(trim(cast(classificacao_dre as string))) as classificacao_dre,

        -- Valor
        round(cast(valor as numeric), 2) as valor,

        -- Auditoria
        _ingested_at

    from source
    where
        despesa_id is not null
        and cast(valor as numeric) is not null
)

select * from cleaned
