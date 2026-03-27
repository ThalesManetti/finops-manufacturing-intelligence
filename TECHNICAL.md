# TECHNICAL.md — FinOps Manufacturing Intelligence

Documentação técnica completa do projeto. Para a visão de negócio e resultados, veja o [README.md](README.md).

---

## 🏗️ Arquitetura — Medallion (Bronze → Silver → Gold)

```
generate_data.py
      ↓
[Parquet Files]  Upload via upload_gcs.ps1
      ↓
[GCS - Google Cloud Storage]  gs://finops-manufacturing-datalake-2026/raw/
      ↓
[BRONZE]  Raw Data             → BigQuery: bronze_manufacturing (9 tabelas raw_*)
      ↓
[SILVER]  Clean + Calc         → BigQuery: silver_manufacturing (stg_* + int_* views)
      ↓
[GOLD]    Marts Financeiros    → BigQuery: gold_finance (mart_* tables)
      ↓
Dashboard Financeiro.pbix  ← Power BI
```

---

## 📁 Estrutura de Pastas

```
FinOps Manufacturing Intelligence/
│
├── README.md                              ← Case study (negócio + resultados)
├── TECHNICAL.md                           ← Este arquivo
├── generate_data.py                       ← ⭐ Geração de 36 meses de dados sintéticos
├── environment.yml                        ← Ambiente Conda (Python 3.11)
├── SETUP_AMBIENTE.txt                     ← Guia de setup Windows
├── setup_gcp.ps1                          ← Inicialização do projeto GCP
├── upload_gcs.ps1                         ← Upload dos Parquet para o GCS
├── Dashboard Financeiro.pbix              ← Dashboard Power BI
│
├── airflow/                               ← Orquestração (Docker)
│   ├── dags/
│   │   └── dag_ingestao_bronze.py         ← ⭐ DAG principal: GCS → BigQuery Bronze
│   ├── scripts/
│   │   └── validate_bronze.sql            ← Validações pós-carga
│   ├── credentials/                       ← Service Account GCP (gitignored)
│   ├── logs/                              ← Logs de execução (gitignored)
│   ├── plugins/                           ← Operators/hooks customizados
│   ├── docker-compose.yml                 ← LocalExecutor + PostgreSQL metadata
│   └── Dockerfile                         ← Airflow 2.8.1 + GCP providers
│
├── dbt/                                   ← Transformações de dados
│   ├── models/
│   │   ├── staging/                       ← Silver: limpeza e padronização
│   │   │   ├── stg_vendas.sql
│   │   │   ├── stg_custos.sql
│   │   │   ├── stg_despesas.sql
│   │   │   ├── stg_orcamento.sql
│   │   │   ├── stg_produtos.sql
│   │   │   ├── stg_clientes.sql
│   │   │   ├── stg_commodities.sql
│   │   │   ├── dim_historico_custos.sql   ← SCD Type 2
│   │   │   ├── sources.yml
│   │   │   └── schema.yml
│   │   ├── intermediate/                  ← Silver: cálculos agregados
│   │   │   ├── int_receita_liquida.sql
│   │   │   ├── int_cmv_mensal.sql
│   │   │   ├── int_despesas_dre.sql
│   │   │   └── schema.yml
│   │   └── marts/                         ← Gold: tabelas para consumo final
│   │       ├── mart_dre_mensal.sql        ← ⭐ DRE completo (P&L)
│   │       ├── mart_kpis_executivo.sql    ← KPIs executivos
│   │       ├── mart_margem_produto.sql    ← Margem por produto
│   │       ├── mart_orcado_realizado.sql  ← Budget vs Realizado
│   │       └── schema.yml
│   ├── macros/
│   │   └── generate_schema_name.sql       ← Convenção de nomes de schema
│   ├── tests/                             ← Testes de qualidade de dados
│   ├── seeds/                             ← Dados de referência estáticos
│   ├── snapshots/                         ← Capturas SCD Type 2
│   ├── dbt_project.yml                    ← Config: materializações e variáveis
│   └── profiles.yml                       ← Conexão BigQuery
│
├── csv/                                   ← Extratos CSV gerados
├── parquet/                               ← Arquivos Parquet (data lake local)
└── Documentos/                            ← Guias de implementação (Word)
    ├── finops_plano_implementacao.docx
    └── fase1_guia_implementacao.docx
```

---

## 🗄️ Modelagem de Dados (BigQuery — Medallion)

```
BRONZE  (dataset: bronze_manufacturing)
  ├── raw_vendas              (~9.150 transações — particionada por data_emissao/MONTH)
  ├── raw_custos              (~1.764 registros mensais — particionada por mes_referencia)
  ├── raw_despesas            (~864 despesas mensais — particionada por mes_referencia)
  ├── raw_orcamento           (~900 registros de orçamento anual)
  ├── raw_produtos            (50 autopeças — cadastro mestre)
  ├── raw_clientes            (24 clientes — 9 OEMs + 15 distribuidoras)
  ├── raw_centros_custo       (14 centros de custo)
  ├── raw_historico_custos    (~381 registros SCD Type 2)
  └── raw_commodities         (144 índices mensais — aço, alumínio, polímero)
  * Todas as tabelas possuem coluna _ingested_at (auditoria de carga)

SILVER  (dataset: silver_manufacturing — views)
  Staging
  ├── stg_vendas              → limpeza, tipos, cast
  ├── stg_custos              → normalização CMV
  ├── stg_despesas            → classificação DRE
  ├── stg_orcamento           → budget por natureza
  ├── stg_produtos            → enriquecimento linha/família
  ├── stg_clientes            → segmentação OEM/distribuidor
  ├── stg_commodities         → índices normalizados
  └── dim_historico_custos    → SCD Type 2 (vigência de custos)
  Intermediate
  ├── int_receita_liquida     → receita bruta → descontos → devoluções → receita líquida
  ├── int_cmv_mensal          → CMV por componente (matéria-prima, mão de obra, CIF)
  └── int_despesas_dre        → despesas agrupadas por categoria DRE

GOLD  (dataset: gold_finance — tables)
  ├── mart_dre_mensal         → ⭐ DRE completo — Receita Bruta até Lucro Líquido
  ├── mart_kpis_executivo     → KPIs: receita, EBITDA, margens, volume
  ├── mart_margem_produto     → margem por produto com impacto de commodity
  └── mart_orcado_realizado   → Budget vs Realizado por linha de produto
```

---

## ⚙️ dbt — Modelos e Variáveis de Projeto

### Materializações por Camada

| Camada | Materialização | Schema BigQuery |
|--------|---------------|-----------------|
| Staging (Silver) | `view` | `silver_manufacturing` |
| Intermediate (Silver) | `view` | `silver_manufacturing` |
| Marts (Gold) | `table` | `gold_finance` |

### Variáveis Globais (`dbt_project.yml`)

| Variável | Valor | Descrição |
|----------|-------|-----------|
| `aliquota_ir_csll` | `0.34` | Alíquota efetiva IR + CSLL (Lucro Real) |
| `margem_threshold` | `0.15` | Threshold de margem para alertas (15%) |

### Estrutura da DRE (`mart_dre_mensal`)

```
Receita Bruta
  (-) Descontos
  (-) Devoluções
= Receita Líquida
  (-) CMV  [Matéria-Prima + Mão de Obra + CIF]
= Lucro Bruto  →  Margem Bruta %
  (-) Despesas Operacionais  [Vendas + Admin + Fábrica]
= EBITDA  →  Margem EBITDA %
  (-) Depreciação
= EBIT  →  Margem EBIT %
  (-) Resultado Financeiro
= LAIR (Lucro Antes do IR)
  (-) IR + CSLL (34%)
= Lucro Líquido  →  Margem Líquida %
```

---

## 🔄 Airflow — DAG de Ingestão Bronze

**DAG ID:** `dag_ingestao_bronze`
**Schedule:** `@daily` | **Retries:** 2 (intervalo: 2 min)

```
[inicio]
    ↓ (paralelo)
┌─────────────────────────────────┐
│ ingerir_raw_vendas              │
│ ingerir_raw_custos              │
│ ingerir_raw_despesas            │
│ ingerir_raw_orcamento           │  ← 9 tasks em paralelo
│ ingerir_raw_produtos            │
│ ingerir_raw_centros_custo       │
│ ingerir_raw_clientes            │
│ ingerir_raw_historico_custos    │
│ ingerir_raw_commodities         │
└─────────────────────────────────┘
    ↓
[validar_camada_bronze]   ← contagem de registros + _ingested_at
    ↓
[fim]
```

**Regras de carga (Bronze):**
- `WRITE_TRUNCATE` — idempotência garantida (re-execução segura)
- Particionamento por `MONTH` nas tabelas transacionais
- Clustering por `produto_id` / `cliente_id` para otimização de queries
- Coluna `_ingested_at` adicionada em todas as tabelas

---

## 📊 Power BI — Dashboard Financeiro

**Arquivo:** `Dashboard Financeiro.pbix`

### Medidas Principais (Gold Layer → Power BI)

| Medida | Fonte | Descrição |
|--------|-------|-----------|
| `Receita Líquida` | `mart_dre_mensal` | Receita após descontos e devoluções |
| `EBITDA` | `mart_dre_mensal` | Resultado operacional antes de D&A |
| `Margem EBITDA %` | `mart_dre_mensal` | EBITDA / Receita Líquida |
| `Lucro Líquido` | `mart_dre_mensal` | Resultado após IR/CSLL (34%) |
| `Budget vs Realizado` | `mart_orcado_realizado` | Desvio % do orçamento |
| `Margem por Produto` | `mart_margem_produto` | Lucratividade unitária |
| `KPIs Executivos` | `mart_kpis_executivo` | Painel consolidado de métricas |

---

## 🚀 Como Executar

### Pré-requisitos

```bash
# 1. Criar e ativar ambiente Conda
conda env create -f environment.yml
conda activate finops

# 2. Configurar credenciais GCP
# Copiar service account JSON para airflow/credentials/
# Exportar variável de ambiente:
export GOOGLE_APPLICATION_CREDENTIALS="airflow/credentials/service-account.json"
```

### Pipeline completo (primeira vez)

```bash
# 1. Gerar dados sintéticos (36 meses)
python generate_data.py
# Output: csv/ e parquet/

# 2. Inicializar recursos GCP (datasets BigQuery + bucket GCS)
powershell -File setup_gcp.ps1

# 3. Upload dos Parquet para o GCS
powershell -File upload_gcs.ps1

# 4. Subir o Airflow (Docker)
cd airflow
docker compose up -d
# Acessar: http://localhost:8080  (admin / admin)
# Ativar e executar: dag_ingestao_bronze

# 5. Executar transformações dbt
cd ../dbt
cp profiles.yml ~/.dbt/profiles.yml
dbt run        # Executar todos os modelos
dbt test       # Rodar testes de qualidade
dbt docs generate && dbt docs serve   # Documentação interativa
```

### Atualização incremental

```bash
# Só transformações (sem re-ingerir)
cd dbt
dbt run --select marts   # Apenas Gold layer

# Re-ingerir + transformar
# Acionar dag_ingestao_bronze no Airflow → dbt run
```

### Atualizar o Dashboard

```
1. Abrir Dashboard Financeiro.pbix no Power BI Desktop
2. Home → Refresh
3. Navegar pelas páginas do relatório
```

---

## ⚠️ Limitações Conhecidas

**Dados Sintéticos:**
- Os dados são gerados via `generate_data.py` (Faker + lógica de negócio customizada)
- Sazonalidade automotiva simulada, mas não espelha comportamento real de mercado
- Commodity pricing sintético — não reflete cotações reais de aço, alumínio e polímero

**Infraestrutura GCP:**
- O projeto usa uma conta GCP de desenvolvimento — limites de quota do BigQuery se aplicam
- Airflow roda em LocalExecutor (Docker local) — não apto para produção distribuída
- `WRITE_TRUNCATE` na Bronze garante idempotência, mas sem histórico de cargas anteriores

**dbt:**
- `profiles.yml` não é versionado (contém credenciais) — deve ser configurado localmente em `~/.dbt/`
- Modelos Gold materializados como `table` — re-execução completa a cada `dbt run`
- Sem incremental strategy implementada (viável em produção com `is_incremental()`)

---

## 📈 Roadmap

- [x] Geração de dados sintéticos (36 meses, domínio automotivo)
- [x] Pipeline de ingestão Airflow: GCS → BigQuery Bronze
- [x] Transformações dbt: Bronze → Silver → Gold
- [x] DRE completo (Receita Bruta → Lucro Líquido)
- [x] Marts: KPIs Executivos, Margem por Produto, Budget vs Realizado
- [x] Dashboard Power BI
- [x] SCD Type 2 para histórico de custos
- [ ] Alertas de margem abaixo do threshold (Slack webhook)
- [ ] Modelo incremental no dbt (evitar full-refresh na Gold)
- [ ] Publicar dashboard no Power BI Service
- [ ] Testes de qualidade com Great Expectations
- [ ] Deploy do Airflow no Cloud Composer (GCP managed)
- [ ] Particionamento incremental na Silver (evitar re-processamento histórico)

---

*Documentação técnica por [Thales Manetti](https://www.linkedin.com/in/thalesmanetti/)*
