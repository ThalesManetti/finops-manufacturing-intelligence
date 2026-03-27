-- =============================================================================
-- Model: mart_dre_mensal
-- Camada: Gold (Marts)
-- Fonte: int_receita_liquida + int_cmv_mensal + int_despesas_dre
-- Descrição: DRE (Demonstração do Resultado do Exercício) completa
--            por mês/ano. Segue o padrão contábil brasileiro.
--            Desde Receita Bruta até Lucro Líquido.
-- =============================================================================

{{ config(
    materialized='table',
    tags=['gold', 'dre', 'finance']
) }}

with receita as (
    select
        ano,
        mes,
        -- Agregar todas as linhas de produção
        sum(receita_bruta) as receita_bruta,
        sum(total_descontos) as total_descontos,
        sum(total_devolucoes) as total_devolucoes,
        sum(receita_liquida) as receita_liquida,
        sum(qtd_vendas) as qtd_vendas,
        sum(qtd_pecas_vendidas) as qtd_pecas_vendidas
    from {{ ref('int_receita_liquida') }}
    group by ano, mes
),

cmv as (
    select
        ano,
        mes,
        sum(cmv_total) as cmv_total,
        sum(cmv_materia_prima) as cmv_materia_prima,
        sum(cmv_mao_obra) as cmv_mao_obra,
        sum(cmv_cif) as cmv_cif
    from {{ ref('int_cmv_mensal') }}
    group by ano, mes
),

despesas as (
    select * from {{ ref('int_despesas_dre') }}
),

-- Montar a DRE completa
dre as (
    select
        r.ano,
        r.mes,
        cast(concat(cast(r.ano as string), '-', lpad(cast(r.mes as string), 2, '0'), '-01') as date) as data_referencia,

        -- ═══════════════════════════════════════════
        -- RECEITA
        -- ═══════════════════════════════════════════
        round(r.receita_bruta, 2) as receita_bruta,
        round(r.total_descontos, 2) as descontos,
        round(r.total_devolucoes, 2) as devolucoes,
        round(r.receita_liquida, 2) as receita_liquida,

        -- ═══════════════════════════════════════════
        -- CMV
        -- ═══════════════════════════════════════════
        round(c.cmv_total, 2) as cmv,
        round(c.cmv_materia_prima, 2) as cmv_materia_prima,
        round(c.cmv_mao_obra, 2) as cmv_mao_obra,
        round(c.cmv_cif, 2) as cmv_cif,

        -- ═══════════════════════════════════════════
        -- LUCRO BRUTO
        -- ═══════════════════════════════════════════
        round(r.receita_liquida - coalesce(c.cmv_total, 0), 2) as lucro_bruto,

        -- Margem Bruta %
        round(
            safe_divide(r.receita_liquida - coalesce(c.cmv_total, 0), r.receita_liquida) * 100,
            2
        ) as margem_bruta_pct,

        -- ═══════════════════════════════════════════
        -- DESPESAS OPERACIONAIS
        -- ═══════════════════════════════════════════
        round(coalesce(d.despesa_vendas, 0), 2) as despesa_vendas,
        round(coalesce(d.despesa_admin, 0), 2) as despesa_admin,
        round(coalesce(d.despesa_fabrica, 0), 2) as despesa_fabrica,
        round(coalesce(d.total_despesas_operacionais, 0), 2) as total_despesas_operacionais,

        -- ═══════════════════════════════════════════
        -- EBITDA
        -- ═══════════════════════════════════════════
        round(
            r.receita_liquida - coalesce(c.cmv_total, 0) - coalesce(d.total_despesas_operacionais, 0),
            2
        ) as ebitda,

        -- Margem EBITDA %
        round(
            safe_divide(
                r.receita_liquida - coalesce(c.cmv_total, 0) - coalesce(d.total_despesas_operacionais, 0),
                r.receita_liquida
            ) * 100,
            2
        ) as margem_ebitda_pct,

        -- ═══════════════════════════════════════════
        -- DEPRECIAÇÃO → EBIT
        -- ═══════════════════════════════════════════
        round(coalesce(d.depreciacao, 0), 2) as depreciacao,

        round(
            r.receita_liquida - coalesce(c.cmv_total, 0) 
            - coalesce(d.total_despesas_operacionais, 0) 
            - coalesce(d.depreciacao, 0),
            2
        ) as ebit,

        -- Margem EBIT %
        round(
            safe_divide(
                r.receita_liquida - coalesce(c.cmv_total, 0) 
                - coalesce(d.total_despesas_operacionais, 0) 
                - coalesce(d.depreciacao, 0),
                r.receita_liquida
            ) * 100,
            2
        ) as margem_ebit_pct,

        -- ═══════════════════════════════════════════
        -- RESULTADO FINANCEIRO → LAIR
        -- ═══════════════════════════════════════════
        round(coalesce(d.resultado_financeiro, 0), 2) as resultado_financeiro,

        round(
            r.receita_liquida - coalesce(c.cmv_total, 0) 
            - coalesce(d.total_despesas_operacionais, 0) 
            - coalesce(d.depreciacao, 0) 
            - coalesce(d.resultado_financeiro, 0),
            2
        ) as lucro_antes_ir,

        -- ═══════════════════════════════════════════
        -- IR/CSLL → LUCRO LÍQUIDO
        -- ═══════════════════════════════════════════
        round(
            greatest(
                (r.receita_liquida - coalesce(c.cmv_total, 0) 
                - coalesce(d.total_despesas_operacionais, 0) 
                - coalesce(d.depreciacao, 0) 
                - coalesce(d.resultado_financeiro, 0)) * {{ var('aliquota_ir_csll') }},
                0
            ),
            2
        ) as ir_csll,

        round(
            (r.receita_liquida - coalesce(c.cmv_total, 0) 
            - coalesce(d.total_despesas_operacionais, 0) 
            - coalesce(d.depreciacao, 0) 
            - coalesce(d.resultado_financeiro, 0))
            - greatest(
                (r.receita_liquida - coalesce(c.cmv_total, 0) 
                - coalesce(d.total_despesas_operacionais, 0) 
                - coalesce(d.depreciacao, 0) 
                - coalesce(d.resultado_financeiro, 0)) * {{ var('aliquota_ir_csll') }},
                0
            ),
            2
        ) as lucro_liquido,

        -- Margem Líquida %
        round(
            safe_divide(
                (r.receita_liquida - coalesce(c.cmv_total, 0) 
                - coalesce(d.total_despesas_operacionais, 0) 
                - coalesce(d.depreciacao, 0) 
                - coalesce(d.resultado_financeiro, 0))
                - greatest(
                    (r.receita_liquida - coalesce(c.cmv_total, 0) 
                    - coalesce(d.total_despesas_operacionais, 0) 
                    - coalesce(d.depreciacao, 0) 
                    - coalesce(d.resultado_financeiro, 0)) * {{ var('aliquota_ir_csll') }},
                    0
                ),
                r.receita_liquida
            ) * 100,
            2
        ) as margem_liquida_pct,

        -- ═══════════════════════════════════════════
        -- MÉTRICAS AUXILIARES
        -- ═══════════════════════════════════════════
        r.qtd_vendas,
        r.qtd_pecas_vendidas,
        round(safe_divide(r.receita_bruta, r.qtd_vendas), 2) as ticket_medio

    from receita r
    left join cmv c on r.ano = c.ano and r.mes = c.mes
    left join despesas d on r.ano = d.ano and r.mes = d.mes
)

select * from dre
order by ano, mes
