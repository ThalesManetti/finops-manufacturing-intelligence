-- =============================================================================
-- Model: int_despesas_dre
-- Camada: Intermediate
-- Fonte: stg_despesas
-- Descrição: Despesas agregadas por mês e classificação DRE.
--            Pivoteia as classificações para facilitar a montagem da DRE.
-- =============================================================================

with despesas as (
    select * from {{ ref('stg_despesas') }}
),

despesas_pivot as (
    select
        ano,
        mes,

        -- Despesas por classificação DRE (pivot manual)
        sum(case when classificacao_dre = 'DESPESA_FABRICA' then valor else 0 end) as despesa_fabrica,
        sum(case when classificacao_dre = 'DESPESA_VENDAS' then valor else 0 end) as despesa_vendas,
        sum(case when classificacao_dre = 'DESPESA_ADMIN' then valor else 0 end) as despesa_admin,
        sum(case when classificacao_dre = 'DEPRECIACAO' then valor else 0 end) as depreciacao,
        sum(case when classificacao_dre = 'RESULTADO_FINANCEIRO' then valor else 0 end) as resultado_financeiro,

        -- Totais
        sum(case when classificacao_dre in ('DESPESA_FABRICA', 'DESPESA_VENDAS', 'DESPESA_ADMIN')
            then valor else 0 end) as total_despesas_operacionais,

        sum(valor) as total_despesas

    from despesas
    group by ano, mes
)

select * from despesas_pivot
