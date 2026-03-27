-- =============================================================================
-- Model: mart_orcado_realizado
-- Camada: Gold (Marts)
-- Fonte: stg_orcamento + stg_despesas
-- Descrição: Cruzamento orçado vs realizado por natureza e mês.
--            Calcula variância absoluta e percentual.
--            Flag para desvios acima de 10%.
-- =============================================================================

{{ config(
    materialized='table',
    tags=['gold', 'orcamento', 'finance']
) }}

with orcamento as (
    select
        ano,
        mes,
        tipo,
        natureza,
        centro_custo_tipo,
        valor_orcado
    from {{ ref('stg_orcamento') }}
),

realizado as (
    select
        ano,
        mes,
        classificacao_dre as tipo,
        natureza,
        centro_custo_tipo,
        sum(valor) as valor_realizado
    from {{ ref('stg_despesas') }}
    group by ano, mes, classificacao_dre, natureza, centro_custo_tipo
),

comparativo as (
    select
        coalesce(o.ano, r.ano) as ano,
        coalesce(o.mes, r.mes) as mes,
        coalesce(o.natureza, r.natureza) as natureza,
        coalesce(o.tipo, r.tipo) as tipo,
        coalesce(o.centro_custo_tipo, r.centro_custo_tipo) as centro_custo_tipo,

        -- Valores
        round(coalesce(o.valor_orcado, 0), 2) as valor_orcado,
        round(coalesce(r.valor_realizado, 0), 2) as valor_realizado,

        -- Variância absoluta (positivo = gastou mais que o orçado)
        round(coalesce(r.valor_realizado, 0) - coalesce(o.valor_orcado, 0), 2) as variancia_absoluta,

        -- Variância percentual
        round(
            safe_divide(
                coalesce(r.valor_realizado, 0) - coalesce(o.valor_orcado, 0),
                nullif(o.valor_orcado, 0)
            ) * 100,
            2
        ) as variancia_pct,

        -- Atingimento (% do orçado que foi realizado)
        round(
            safe_divide(r.valor_realizado, nullif(o.valor_orcado, 0)) * 100,
            2
        ) as atingimento_pct

    from orcamento o
    full outer join realizado r
        on o.ano = r.ano
        and o.mes = r.mes
        and o.natureza = r.natureza
),

-- Adicionar flags
final as (
    select
        *,

        -- Flag de desvio
        case
            when abs(variancia_pct) > 20 then 'DESVIO_CRITICO'
            when abs(variancia_pct) > 10 then 'DESVIO_ALERTA'
            when abs(variancia_pct) > 5 then 'DESVIO_MODERADO'
            else 'DENTRO_ORCAMENTO'
        end as status_desvio,

        -- Direção do desvio
        case
            when variancia_absoluta > 0 then 'ACIMA_ORCADO'
            when variancia_absoluta < 0 then 'ABAIXO_ORCADO'
            else 'NO_ORCADO'
        end as direcao_desvio

    from comparativo
)

select * from final
order by ano, mes, natureza
