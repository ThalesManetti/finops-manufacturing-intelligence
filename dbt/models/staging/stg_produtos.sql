-- =============================================================================
-- Model: stg_produtos
-- Camada: Silver (Staging)
-- Fonte: bronze.raw_produtos
-- Descrição: Cadastro de produtos limpo e padronizado
-- =============================================================================

with source as (
    select * from {{ source('bronze', 'raw_produtos') }}
),

cleaned as (
    select
        -- PK
        cast(produto_id as string) as produto_id,

        -- Atributos (padronização)
        upper(trim(cast(produto_nome as string))) as produto_nome,
        cast(linha_producao_id as string) as linha_producao_id,
        upper(trim(cast(linha_producao_nome as string))) as linha_producao_nome,
        upper(trim(cast(unidade_medida as string))) as unidade_medida,
        upper(trim(cast(status as string))) as status,
        cast(ncm as string) as ncm,

        -- Valores
        round(cast(custo_padrao_unitario as numeric), 2) as custo_padrao_unitario,
        round(cast(preco_venda_unitario as numeric), 2) as preco_venda_unitario,
        round(cast(peso_kg as numeric), 2) as peso_kg,

        -- Margem padrão (calculada)
        round(
            (cast(preco_venda_unitario as numeric) - cast(custo_padrao_unitario as numeric)) 
            / nullif(cast(preco_venda_unitario as numeric), 0),
            4
        ) as margem_padrao_pct,

        -- Data
        cast(data_cadastro as date) as data_cadastro,

        -- Auditoria
        _ingested_at

    from source
    where produto_id is not null
)

select * from cleaned
