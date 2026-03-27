-- =============================================================================
-- Model: int_receita_liquida
-- Camada: Intermediate
-- Fonte: stg_vendas
-- Descrição: Calcula receita bruta, descontos, devoluções e receita líquida
--            agregados por mês, ano e linha de produção.
-- =============================================================================

with vendas as (
    select * from {{ ref('stg_vendas') }}
),

receita_mensal as (
    select
        ano,
        mes,
        linha_producao_id,

        -- Receita Bruta
        sum(valor_bruto) as receita_bruta,

        -- Deduções
        sum(valor_desconto) as total_descontos,
        sum(devolucao_valor) as total_devolucoes,

        -- Receita Líquida = Bruta - Descontos - Devoluções
        sum(valor_bruto) - sum(valor_desconto) - sum(devolucao_valor) as receita_liquida,

        -- Métricas auxiliares
        count(distinct venda_id) as qtd_vendas,
        sum(quantidade) as qtd_pecas_vendidas,
        count(distinct produto_id) as qtd_produtos_vendidos,
        count(distinct cliente_id) as qtd_clientes

    from vendas
    group by ano, mes, linha_producao_id
)

select * from receita_mensal
