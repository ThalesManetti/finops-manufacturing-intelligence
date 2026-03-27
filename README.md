<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:0A0F1E,50:00E5FF,100:FF6B2B&height=180&section=header&text=FinOps%20Manufacturing%20Intelligence&fontSize=34&fontColor=ffffff&fontAlignY=38&desc=Power%20BI%20%7C%20BigQuery%20%7C%20dbt%20%7C%20Airflow%20%7C%20GCP%20%7C%20Case%20Study%20Financeiro&descAlignY=58&descSize=15&animation=fadeIn" width="100%"/>

</div>

<br/>

<div align="center">

![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=for-the-badge&logo=googlebigquery&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![GCP](https://img.shields.io/badge/GCP-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)
![Status](https://img.shields.io/badge/Status-Conclu%C3%ADdo-2EA44F?style=for-the-badge)
![Nível](https://img.shields.io/badge/N%C3%ADvel-Pleno-FF6B2B?style=for-the-badge)

</div>

<div align="center">

🔗 **[Acessar Dashboard no Power BI Service](https://app.powerbi.com/reportEmbed?reportId=100c53cd-c31b-4b16-9ead-e7425ae161da&autoAuth=true&ctid=57d2ea99-01a6-4ab8-86bb-75c0642ef771)**

</div>

---

## 🎯 O Problema

Uma indústria de autopeças **Tier 2** — fornecedora direta de montadoras como **Stellantis, Volkswagen e Toyota** — operava com dados financeiros dispersos em diferentes sistemas: vendas, custos de produção, despesas operacionais e orçamento não se conversavam.

A diretoria financeira tomava decisões no escuro:

- Quais produtos estão destruindo margem?
- Onde estão os maiores desvios orçamentários?
- Qual é a rentabilidade real da operação mês a mês?

**A solução:** pipeline de dados completo — da ingestão ao dashboard — unificando toda a operação financeira em uma DRE interativa, análise de margem por produto e controle orçado vs. realizado. Tudo atualizado automaticamente via **Apache Airflow + dbt + BigQuery**.

> *Público-alvo: CFO, CEO e equipe de Controladoria.*

---

## 📊 Dados do Projeto

| Dimensão | Detalhe |
|----------|---------|
| **Origem** | Dados sintéticos gerados via Python (Pandas + Faker) |
| **Período** | 3 anos de operação — 2022 a 2024 |
| **Transações de vendas** | ~9.150 registros |
| **Registros de custos** | 1.764 |
| **Registros de despesas** | 864 |
| **Linhas de orçamento** | 900 |
| **Produtos** | 50 autopeças |
| **Clientes** | 24 (montadoras e Tier 1) |
| **Centros de custo** | 14 |
| **Histórico de custos (SCD Type 2)** | 381 registros |
| **Índices de commodities** | 144 |

> ⚠️ Dados 100% sintéticos, gerados para simular um cenário realista de indústria de autopeças Tier 2 no Brasil.

---

## 🏗️ Arquitetura — Pipeline Completo

```
Geração dos Dados (Python / Faker)
         ↓
[BRONZE]  Raw Data no BigQuery       ← Ingestão via Apache Airflow + Docker
         ↓
[SILVER]  Dados Limpos (dbt)         ← Tipagem, UPPER/TRIM, deduplicação, nulos
         ↓                              75 testes de qualidade automatizados
[GOLD]    Marts Analíticos (dbt)     ← 4 tabelas fato/mart + dimensões
         ↓
   Dashboard Power BI                ← Conectado ao BigQuery
```

**Camada Gold — Modelo Dimensional:**

| Tabela | Descrição |
|--------|-----------|
| `mart_dre_mensal` | DRE completa mês a mês (receita bruta → lucro líquido) |
| `mart_margem_produto` | Margem por produto com histórico via SCD Type 2 |
| `mart_orcado_realizado` | Variância orçado vs. realizado por natureza de despesa |
| `mart_kpis_executivo` | KPIs consolidados para visão C-level |
| `stg_produtos` | Dimensão de 50 autopeças com linha de produção |
| `stg_clientes` | Dimensão de 24 clientes |

---

## 📐 KPIs e Métricas

| KPI | Por que importa |
|-----|----------------|
| **Receita Líquida** | Faturamento real descontado devoluções e descontos |
| **Margem Bruta (%)** | Eficiência produtiva — saudável entre 35–50% para Tier 2 |
| **EBITDA** | Capacidade de geração de caixa operacional |
| **Lucro Líquido / Margem Líquida** | Saúde financeira real após IR/CSLL (34%) |
| **Ticket Médio** | Queda indica mudança no mix ou pressão de preço das montadoras |
| **Variância Orçado vs. Realizado** | Flag automático: >10% = alerta · >20% = crítico |
| **Produtos com Margem Negativa** | Contagem de produtos que destroem valor — ação imediata |
| **Evolução MoM / YoY** | Tendência eliminando efeito de sazonalidade (paradas dez/jan) |

---

## 🖥️ Dashboard Power BI — 3 Páginas

Cada página responde uma pergunta estratégica do C-level:

| Página | O que mostra |
|--------|-------------|
| 📊 **DRE Executiva** | Waterfall da DRE · evolução mensal de 36 meses · tabela com conditional formatting |
| 🔍 **Análise de Margem** | Scatter margem × volume · Top 10 produtos · heatmap por linha de produção |
| 🎯 **Orçado vs. Realizado** | Clustered bar por despesa · tabela semáforo (OK/Alerta/Crítico) · gauge charts |

---

## 💡 Principais Insights

### 1. Crescimento de receita com lucro negativo — o problema está nas despesas

A empresa cresceu **+11,9% em receita** em 2024, com margem bruta saudável (~44,8%). Porém o **lucro líquido foi negativo em R$ 12,9M**. O problema não está nos produtos — está nas despesas operacionais, que consomem mais que o dobro do lucro bruto. Estrutura de custos fixos superdimensionada para o nível de faturamento atual.

📌 Auditoria nas naturezas com desvio orçamentário crítico — especialmente **Manutenção Industrial (+20%)** e **Energia Elétrica (+15%)**.

### 2. Sazonalidade severa derruba resultado em dezembro e janeiro

Paradas coletivas das montadoras causam **queda de ~55% na receita** em dez/jan, enquanto as despesas fixas permanecem constantes. Resultado: margem EBITDA de **-336% a -418%** nos piores meses do ano.

📌 Política de férias coletivas alinhada às paradas + contratos flexíveis de energia e manutenção para reduzir custo fixo nos meses de baixa.

### 3. Estampados e Estruturais — a linha mais exposta a commodities

Dos 50 produtos, **49 têm margem saudável (≥15%)** — nenhum com margem negativa. Porém a linha **Estampados e Estruturais** tem margem de **38,9%**, contra **45,8%** de Montagem e Subconjuntos — diferença de **6,9 pontos percentuais** explicada pela maior exposição a aço e alumínio.

📌 Renegociação de contratos de matéria-prima ou implementação de hedge de commodities para proteger a margem nessa linha.

---

## 📸 Screenshots do Dashboard

**Capa**
![Capa](assets/images/FinOps-1.png)

---

**Página 1 — DRE Executiva**
![DRE Executiva](assets/images/FinOps-2.png)

---

**Página 2 — Análise de Margem**
![Análise de Margem](assets/images/FinOps-3.png)

---

**Página 3 — Orçado vs. Realizado**
![Orçado vs Realizado](assets/images/FinOps-4.png)

---

## 🛠️ Stack

```
Visualização        →  Power BI Desktop
Data Warehouse      →  Google BigQuery (GCP)
Transformação       →  dbt (75 testes de qualidade automatizados)
Orquestração        →  Apache Airflow (Docker)
Infraestrutura      →  Docker + GCP
Geração de Dados    →  Python 3.11 · Pandas · Faker
Modelagem           →  Arquitetura Medallion (Bronze → Silver → Gold)
                       Modelo Dimensional (4 fatos/marts + 2 dimensões)
                       SCD Type 2 (histórico de custos)
```

📄 Para detalhes técnicos completos (DAGs do Airflow, modelos dbt, queries BigQuery, como executar), veja o [TECHNICAL.md](TECHNICAL.md).

---

<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:FF6B2B,50:00E5FF,100:0A0F1E&height=100&section=footer" width="100%"/>

**Thales Manetti** · [LinkedIn](https://www.linkedin.com/in/thalesmanetti/) · [Portfólio](https://thalesmanetti.github.io)

</div>
