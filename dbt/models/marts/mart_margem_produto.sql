-- =============================================================================
-- Model: mart_margem_produto
-- Camada: Gold (Marts)
-- Fonte: stg_vendas + stg_custos + stg_produtos
-- Descrição: Margem bruta e contribuição por produto.
--            Identifica produtos que destroem margem (margem negativa).
--            Ranking de rentabilidade.
-- =============================================================================

{{ config(
    materialized='table',
    tags=['gold', 'margem', 'finance']
) }}

with vendas as (
    select
        ano,
        mes,
        produto_id,
        linha_producao_id,
        sum(quantidade) as qtd_vendida,
        sum(valor_bruto) as receita_bruta,
        sum(valor_liquido) as receita_liquida
    from {{ ref('stg_vendas') }}
    group by ano, mes, produto_id, linha_producao_id
),

custos as (
    select
        produto_id,
        ano,
        mes,
        custo_total_unitario
    from {{ ref('stg_custos') }}
),

produtos as (
    select
        produto_id,
        produto_nome,
        linha_producao_nome,
        custo_padrao_unitario,
        preco_venda_unitario
    from {{ ref('stg_produtos') }}
),

margem_por_produto as (
    select
        v.ano,
        v.mes,
        v.produto_id,
        p.produto_nome,
        v.linha_producao_id,
        p.linha_producao_nome,

        -- Volumes
        v.qtd_vendida,
        v.receita_bruta,
        v.receita_liquida,

        -- CMV real
        c.custo_total_unitario as custo_unitario_real,
        round(v.qtd_vendida * c.custo_total_unitario, 2) as cmv_produto,

        -- Margem bruta
        round(v.receita_liquida - (v.qtd_vendida * c.custo_total_unitario), 2) as margem_bruta,

        -- Margem bruta %
        round(
            safe_divide(
                v.receita_liquida - (v.qtd_vendida * c.custo_total_unitario),
                v.receita_liquida
            ) * 100,
            2
        ) as margem_bruta_pct,

        -- Margem de contribuição unitária
        round(
            safe_divide(v.receita_liquida, v.qtd_vendida) - c.custo_total_unitario,
            2
        ) as margem_contribuicao_unitaria,

        -- Custo padrão vs real
        p.custo_padrao_unitario,
        round(
            safe_divide(c.custo_total_unitario - p.custo_padrao_unitario, p.custo_padrao_unitario) * 100,
            2
        ) as variacao_custo_vs_padrao_pct

    from vendas v
    left join custos c
        on v.produto_id = c.produto_id
        and v.ano = c.ano
        and v.mes = c.mes
    left join produtos p
        on v.produto_id = p.produto_id
    where c.custo_total_unitario is not null
),

-- Adicionar flags e ranking
final as (
    select
        *,

        -- Flag: produto destrói margem?
        case
            when margem_bruta < 0 then 'MARGEM_NEGATIVA'
            when margem_bruta_pct < {{ var('margem_threshold') }} * 100 then 'ABAIXO_THRESHOLD'
            else 'SAUDAVEL'
        end as status_margem,

        -- Ranking de margem no mês (1 = mais rentável)
        row_number() over (
            partition by ano, mes
            order by margem_bruta desc
        ) as ranking_margem_mes,

        -- Participação na receita total do mês
        round(
            safe_divide(receita_liquida, sum(receita_liquida) over (partition by ano, mes)) * 100,
            2
        ) as pct_receita_mes

    from margem_por_produto
)

select * from final
order by ano, mes, ranking_margem_mes
