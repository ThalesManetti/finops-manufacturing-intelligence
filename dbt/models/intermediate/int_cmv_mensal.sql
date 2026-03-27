-- =============================================================================
-- Model: int_cmv_mensal
-- Camada: Intermediate
-- Fonte: stg_vendas + stg_custos
-- Descrição: Calcula o Custo da Mercadoria Vendida (CMV) real mensal.
--            CMV = custo unitário real × quantidade vendida.
-- =============================================================================

with vendas as (
    select
        venda_id,
        ano,
        mes,
        produto_id,
        linha_producao_id,
        quantidade,
        data_emissao
    from {{ ref('stg_vendas') }}
),

custos as (
    select
        produto_id,
        mes_referencia,
        ano,
        mes,
        custo_total_unitario,
        custo_materia_prima,
        custo_mao_obra_direta,
        custo_indireto_fabricacao
    from {{ ref('stg_custos') }}
),

-- Juntar vendas com custos do mesmo produto/mês
vendas_com_custo as (
    select
        v.ano,
        v.mes,
        v.linha_producao_id,
        v.produto_id,
        v.quantidade,

        -- Custo unitário real do mês
        c.custo_total_unitario,

        -- CMV da transação = quantidade × custo real
        v.quantidade * c.custo_total_unitario as cmv_transacao,

        -- Componentes do CMV (para análise detalhada)
        v.quantidade * c.custo_materia_prima as cmv_materia_prima,
        v.quantidade * c.custo_mao_obra_direta as cmv_mao_obra,
        v.quantidade * c.custo_indireto_fabricacao as cmv_cif

    from vendas v
    left join custos c
        on v.produto_id = c.produto_id
        and v.ano = c.ano
        and v.mes = c.mes
),

-- Agregar por mês e linha de produção
cmv_mensal as (
    select
        ano,
        mes,
        linha_producao_id,

        -- CMV total
        round(sum(cmv_transacao), 2) as cmv_total,

        -- Componentes do CMV
        round(sum(cmv_materia_prima), 2) as cmv_materia_prima,
        round(sum(cmv_mao_obra), 2) as cmv_mao_obra,
        round(sum(cmv_cif), 2) as cmv_cif,

        -- Métricas auxiliares
        sum(quantidade) as qtd_total_vendida,
        count(distinct produto_id) as qtd_produtos

    from vendas_com_custo
    where custo_total_unitario is not null
    group by ano, mes, linha_producao_id
)

select * from cmv_mensal
