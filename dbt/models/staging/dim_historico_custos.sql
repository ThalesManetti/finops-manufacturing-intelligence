-- =============================================================================
-- Model: dim_historico_custos
-- Camada: Silver (Staging)
-- Fonte: bronze.raw_historico_custos
-- Descrição: Dimensão de histórico de custos com SCD Type 2
--            Permite consultar o custo vigente em qualquer ponto no tempo
-- Materialização: TABLE (não view, porque SCD2 precisa de persistência)
-- =============================================================================

{{ config(materialized='table') }}

with source as (
    select * from {{ source('bronze', 'raw_historico_custos') }}
),

cleaned as (
    select
        -- PK
        cast(historico_id as string) as historico_id,
        cast(produto_id as string) as produto_id,

        -- Valores de custo
        round(cast(custo_padrao_anterior as numeric), 2) as custo_padrao_anterior,
        round(cast(custo_padrao_novo as numeric), 2) as custo_padrao_novo,

        -- SCD Type 2 — datas de vigência
        cast(data_vigencia_inicio as date) as valid_from,
        cast(data_vigencia_fim as date) as valid_to,

        -- Flag de registro corrente
        cast(is_current as boolean) as is_current,

        -- Metadados
        upper(trim(cast(motivo as string))) as motivo_alteracao,

        -- Variação percentual (calculada)
        case
            when cast(custo_padrao_anterior as numeric) is not null 
                 and cast(custo_padrao_anterior as numeric) > 0
            then round(
                (cast(custo_padrao_novo as numeric) - cast(custo_padrao_anterior as numeric))
                / cast(custo_padrao_anterior as numeric),
                4
            )
            else null
        end as variacao_pct,

        -- Auditoria
        _ingested_at

    from source
    where historico_id is not null
)

select * from cleaned
