-- =============================================================================
-- Model: stg_vendas
-- Camada: Silver (Staging)
-- Fonte: bronze.raw_vendas
-- Descrição: Vendas limpas, tipadas e padronizadas
-- =============================================================================

with source as (
    select * from {{ source('bronze', 'raw_vendas') }}
),

cleaned as (
    select
        -- PKs e FKs
        cast(venda_id as string) as venda_id,
        cast(nota_fiscal as string) as nota_fiscal,
        cast(produto_id as string) as produto_id,
        cast(cliente_id as string) as cliente_id,
        cast(linha_producao_id as string) as linha_producao_id,

        -- Datas
        cast(data_emissao as date) as data_emissao,
        extract(year from cast(data_emissao as date)) as ano,
        extract(month from cast(data_emissao as date)) as mes,

        -- Cliente (padronização)
        upper(trim(cast(cliente_nome as string))) as cliente_nome,
        upper(trim(cast(tipo_cliente as string))) as tipo_cliente,

        -- Valores numéricos
        cast(quantidade as int64) as quantidade,
        round(cast(preco_unitario as numeric), 2) as preco_unitario,
        round(cast(valor_bruto as numeric), 2) as valor_bruto,
        round(cast(desconto_percentual as numeric), 4) as desconto_percentual,
        round(cast(valor_desconto as numeric), 2) as valor_desconto,
        cast(coalesce(devolucao_quantidade, 0) as int64) as devolucao_quantidade,
        round(cast(coalesce(devolucao_valor, 0) as numeric), 2) as devolucao_valor,
        round(cast(valor_liquido as numeric), 2) as valor_liquido,

        -- Auditoria
        _ingested_at

    from source
    where 
        -- Remover registros com PK nula
        venda_id is not null
        -- Remover valores negativos inválidos
        and cast(quantidade as int64) > 0
        and cast(valor_bruto as numeric) > 0
)

select * from cleaned
