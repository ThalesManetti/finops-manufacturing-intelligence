-- =============================================================================
-- Model: stg_custos
-- Camada: Silver (Staging)
-- Fonte: bronze.raw_custos
-- Descrição: Custos de produção limpos e tipados. CMV por produto/mês.
-- =============================================================================

with source as (
    select * from {{ source('bronze', 'raw_custos') }}
),

cleaned as (
    select
        -- FKs
        cast(produto_id as string) as produto_id,
        cast(linha_producao_id as string) as linha_producao_id,

        -- Datas
        cast(mes_referencia as date) as mes_referencia,
        extract(year from cast(mes_referencia as date)) as ano,
        extract(month from cast(mes_referencia as date)) as mes,

        -- Componentes do custo
        round(cast(custo_materia_prima as numeric), 2) as custo_materia_prima,
        round(cast(custo_mao_obra_direta as numeric), 2) as custo_mao_obra_direta,
        round(cast(custo_indireto_fabricacao as numeric), 2) as custo_indireto_fabricacao,
        round(cast(custo_total_unitario as numeric), 2) as custo_total_unitario,
        round(cast(custo_padrao_unitario as numeric), 2) as custo_padrao_unitario,

        -- Variação e índice
        round(cast(variacao_custo_pct as numeric), 4) as variacao_custo_pct,
        round(cast(indice_commodity as numeric), 4) as indice_commodity,

        -- Auditoria
        _ingested_at

    from source
    where
        produto_id is not null
        and mes_referencia is not null
        and cast(custo_total_unitario as numeric) > 0
)

select * from cleaned
