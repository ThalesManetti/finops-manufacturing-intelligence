# FinOps Manufacturing Intelligence

Pipeline de analytics financeiro completo para manufatura — do dado bruto ao dashboard executivo, usando a stack moderna de dados: **Airflow + dbt + BigQuery + Power BI**.

Para a documentação técnica detalhada (arquitetura, modelos, como executar), veja o [TECHNICAL.md](TECHNICAL.md).

---

## 🎯 Contexto de Negócio

Fabricante de autopeças (fornecedor Tier 2) com necessidade de visibilidade financeira consolidada: **quanto custou produzir, quanto foi vendido, qual a margem por produto, e onde estamos em relação ao orçamento.**

O projeto implementa uma plataforma de FinOps que responde a perguntas como:
- Qual foi o EBITDA do mês? E a margem líquida após IR/CSLL?
- Quais produtos têm margem abaixo do threshold de 15%?
- Onde estamos em relação ao budget? Quais linhas estão acima/abaixo do previsto?
- Como a variação de commodities (aço, alumínio, polímero) impacta o CMV?

---

## 🏗️ Arquitetura

```
Dados Sintéticos (Python)
        ↓
  GCS (Data Lake)
        ↓
  Airflow (Ingestão)
        ↓
  BigQuery Bronze  →  dbt Silver  →  dbt Gold
                                          ↓
                                    Power BI Dashboard
```

Arquitetura **Medallion** (Bronze → Silver → Gold) com orquestração via Airflow e transformações via dbt.

---

## 📊 Resultados — Dashboard Financeiro

| Mart | Descrição |
|------|-----------|
| `mart_dre_mensal` | DRE completo — Receita Bruta até Lucro Líquido |
| `mart_kpis_executivo` | Painel de KPIs: receita, EBITDA, margens, volume |
| `mart_margem_produto` | Lucratividade por produto com exposição a commodities |
| `mart_orcado_realizado` | Budget vs Realizado por linha de produto |

**Cobertura:** 36 meses de dados (Jan/2022 – Dez/2024), 50 produtos, 4 linhas de produção, 24 clientes.

---

## 🛠️ Stack Tecnológica

| Camada | Tecnologia |
|--------|-----------|
| Linguagem | Python 3.11 |
| Orquestração | Apache Airflow 2.8.1 (Docker) |
| Data Lake | Google Cloud Storage |
| Data Warehouse | Google BigQuery |
| Transformação | dbt 1.7 + dbt-bigquery |
| Visualização | Power BI |
| Ambiente | Conda |

---

## ⚡ Quick Start

```bash
# 1. Criar ambiente
conda env create -f environment.yml
conda activate finops

# 2. Gerar dados + fazer upload para GCS
python generate_data.py
powershell -File setup_gcp.ps1
powershell -File upload_gcs.ps1

# 3. Ingestão via Airflow
cd airflow && docker compose up -d
# http://localhost:8080 → ativar dag_ingestao_bronze

# 4. Transformações dbt
cd dbt && dbt run && dbt test
```

Veja o [TECHNICAL.md](TECHNICAL.md) para o guia completo de setup.

---

*Por [Thales Manetti](https://www.linkedin.com/in/thalesmanetti/)*
