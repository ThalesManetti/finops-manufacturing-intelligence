-- =============================================================================
-- FinOps Manufacturing Intelligence — Validação da Camada Bronze
-- =============================================================================
-- Execute no BigQuery Console ou via bq query
-- Projeto: finops-manufacturing-2026
-- Dataset: bronze_manufacturing
-- =============================================================================

-- ─────────────────────────────────────────────
-- 1. CONTAGEM DE REGISTROS POR TABELA
-- ─────────────────────────────────────────────
SELECT 'raw_vendas' AS tabela, COUNT(*) AS registros FROM `finops-manufacturing-2026.bronze_manufacturing.raw_vendas`
UNION ALL
SELECT 'raw_custos', COUNT(*) FROM `finops-manufacturing-2026.bronze_manufacturing.raw_custos`
UNION ALL
SELECT 'raw_despesas', COUNT(*) FROM `finops-manufacturing-2026.bronze_manufacturing.raw_despesas`
UNION ALL
SELECT 'raw_orcamento', COUNT(*) FROM `finops-manufacturing-2026.bronze_manufacturing.raw_orcamento`
UNION ALL
SELECT 'raw_produtos', COUNT(*) FROM `finops-manufacturing-2026.bronze_manufacturing.raw_produtos`
UNION ALL
SELECT 'raw_centros_custo', COUNT(*) FROM `finops-manufacturing-2026.bronze_manufacturing.raw_centros_custo`
UNION ALL
SELECT 'raw_clientes', COUNT(*) FROM `finops-manufacturing-2026.bronze_manufacturing.raw_clientes`
UNION ALL
SELECT 'raw_historico_custos', COUNT(*) FROM `finops-manufacturing-2026.bronze_manufacturing.raw_historico_custos`
UNION ALL
SELECT 'raw_commodities', COUNT(*) FROM `finops-manufacturing-2026.bronze_manufacturing.raw_commodities`
ORDER BY tabela;


-- ─────────────────────────────────────────────
-- 2. VERIFICAR NULOS EM CHAVES PRIMÁRIAS
-- ─────────────────────────────────────────────
SELECT 'raw_vendas.venda_id' AS campo, COUNTIF(venda_id IS NULL) AS nulos
FROM `finops-manufacturing-2026.bronze_manufacturing.raw_vendas`
UNION ALL
SELECT 'raw_custos.produto_id', COUNTIF(produto_id IS NULL)
FROM `finops-manufacturing-2026.bronze_manufacturing.raw_custos`
UNION ALL
SELECT 'raw_despesas.despesa_id', COUNTIF(despesa_id IS NULL)
FROM `finops-manufacturing-2026.bronze_manufacturing.raw_despesas`
UNION ALL
SELECT 'raw_produtos.produto_id', COUNTIF(produto_id IS NULL)
FROM `finops-manufacturing-2026.bronze_manufacturing.raw_produtos`
UNION ALL
SELECT 'raw_clientes.cliente_id', COUNTIF(cliente_id IS NULL)
FROM `finops-manufacturing-2026.bronze_manufacturing.raw_clientes`;


-- ─────────────────────────────────────────────
-- 3. VERIFICAR COLUNA _ingested_at
-- ─────────────────────────────────────────────
SELECT 'raw_vendas' AS tabela, MIN(_ingested_at) AS primeira_carga, MAX(_ingested_at) AS ultima_carga
FROM `finops-manufacturing-2026.bronze_manufacturing.raw_vendas`
UNION ALL
SELECT 'raw_custos', MIN(_ingested_at), MAX(_ingested_at)
FROM `finops-manufacturing-2026.bronze_manufacturing.raw_custos`
UNION ALL
SELECT 'raw_despesas', MIN(_ingested_at), MAX(_ingested_at)
FROM `finops-manufacturing-2026.bronze_manufacturing.raw_despesas`;


-- ─────────────────────────────────────────────
-- 4. AMOSTRA DE DADOS (SPOT CHECK)
-- ─────────────────────────────────────────────
-- Vendas: verificar range de datas e valores
SELECT
    MIN(data_emissao) AS data_min,
    MAX(data_emissao) AS data_max,
    COUNT(DISTINCT produto_id) AS produtos_distintos,
    COUNT(DISTINCT cliente_id) AS clientes_distintos,
    SUM(valor_bruto) AS receita_bruta_total,
    AVG(valor_bruto) AS ticket_medio
FROM `finops-manufacturing-2026.bronze_manufacturing.raw_vendas`;

-- Custos: verificar range de custos
SELECT
    MIN(mes_referencia) AS mes_min,
    MAX(mes_referencia) AS mes_max,
    AVG(custo_total_unitario) AS custo_medio,
    MIN(custo_total_unitario) AS custo_min,
    MAX(custo_total_unitario) AS custo_max
FROM `finops-manufacturing-2026.bronze_manufacturing.raw_custos`;


-- ─────────────────────────────────────────────
-- 5. VERIFICAR DUPLICATAS EM PKs
-- ─────────────────────────────────────────────
SELECT 'raw_vendas' AS tabela, venda_id AS pk, COUNT(*) AS duplicatas
FROM `finops-manufacturing-2026.bronze_manufacturing.raw_vendas`
GROUP BY venda_id
HAVING COUNT(*) > 1
LIMIT 5;

SELECT 'raw_produtos' AS tabela, produto_id AS pk, COUNT(*) AS duplicatas
FROM `finops-manufacturing-2026.bronze_manufacturing.raw_produtos`
GROUP BY produto_id
HAVING COUNT(*) > 1
LIMIT 5;
