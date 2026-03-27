-- =============================================================================
-- Model: mart_kpis_executivo
-- Camada: Gold (Marts)
-- Fonte: mart_dre_mensal + mart_margem_produto
-- Descrição: KPIs consolidados para o dashboard executivo.
--            Inclui evolução MoM (mês a mês) e YoY (ano a ano).
--            Otimizado para consumo do Power BI.
-- =============================================================================

{{ config(
    materialized='table',
    tags=['gold', 'kpi', 'dashboard']
) }}

with dre as (
    select * from {{ ref('mart_dre_mensal') }}
),

-- KPIs mensais com evolução temporal
kpis_mensais as (
    select
        ano,
        mes,
        data_referencia,

        -- ═══════════════════════════════════════════
        -- KPIs ABSOLUTOS
        -- ═══════════════════════════════════════════
        receita_bruta,
        receita_liquida,
        cmv,
        lucro_bruto,
        ebitda,
        ebit,
        lucro_liquido,

        -- ═══════════════════════════════════════════
        -- MARGENS
        -- ═══════════════════════════════════════════
        margem_bruta_pct,
        margem_ebitda_pct,
        margem_ebit_pct,
        margem_liquida_pct,

        -- ═══════════════════════════════════════════
        -- OPERACIONAL
        -- ═══════════════════════════════════════════
        qtd_vendas,
        qtd_pecas_vendidas,
        ticket_medio,
        total_despesas_operacionais,

        -- ═══════════════════════════════════════════
        -- EVOLUÇÃO MoM (mês anterior)
        -- ═══════════════════════════════════════════
        lag(receita_liquida) over (order by ano, mes) as receita_liquida_mes_anterior,
        round(
            safe_divide(
                receita_liquida - lag(receita_liquida) over (order by ano, mes),
                lag(receita_liquida) over (order by ano, mes)
            ) * 100,
            2
        ) as receita_mom_pct,

        lag(lucro_bruto) over (order by ano, mes) as lucro_bruto_mes_anterior,
        round(
            safe_divide(
                lucro_bruto - lag(lucro_bruto) over (order by ano, mes),
                abs(lag(lucro_bruto) over (order by ano, mes))
            ) * 100,
            2
        ) as lucro_bruto_mom_pct,

        lag(ebitda) over (order by ano, mes) as ebitda_mes_anterior,
        round(
            safe_divide(
                ebitda - lag(ebitda) over (order by ano, mes),
                abs(lag(ebitda) over (order by ano, mes))
            ) * 100,
            2
        ) as ebitda_mom_pct,

        -- ═══════════════════════════════════════════
        -- EVOLUÇÃO YoY (mesmo mês, ano anterior)
        -- ═══════════════════════════════════════════
        lag(receita_liquida, 12) over (order by ano, mes) as receita_liquida_ano_anterior,
        round(
            safe_divide(
                receita_liquida - lag(receita_liquida, 12) over (order by ano, mes),
                lag(receita_liquida, 12) over (order by ano, mes)
            ) * 100,
            2
        ) as receita_yoy_pct,

        lag(lucro_bruto, 12) over (order by ano, mes) as lucro_bruto_ano_anterior,
        round(
            safe_divide(
                lucro_bruto - lag(lucro_bruto, 12) over (order by ano, mes),
                abs(lag(lucro_bruto, 12) over (order by ano, mes))
            ) * 100,
            2
        ) as lucro_bruto_yoy_pct,

        -- ═══════════════════════════════════════════
        -- ACUMULADO NO ANO (YTD)
        -- ═══════════════════════════════════════════
        sum(receita_liquida) over (partition by ano order by mes) as receita_liquida_ytd,
        sum(lucro_bruto) over (partition by ano order by mes) as lucro_bruto_ytd,
        sum(ebitda) over (partition by ano order by mes) as ebitda_ytd,
        sum(lucro_liquido) over (partition by ano order by mes) as lucro_liquido_ytd

    from dre
)

select * from kpis_mensais
order by ano, mes
