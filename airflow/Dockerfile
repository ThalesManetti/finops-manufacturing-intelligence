# =============================================================================
# FinOps Manufacturing Intelligence — Dockerfile Airflow
# =============================================================================
# Imagem customizada do Apache Airflow com dependências para GCP
# (BigQuery, Cloud Storage) e providers necessários.
# =============================================================================

FROM apache/airflow:2.8.1-python3.11

# Instalar dependências Python adicionais
USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-google==10.13.1 \
    google-cloud-bigquery>=3.14 \
    google-cloud-storage>=2.14 \
    pyarrow>=14.0 \
    pandas>=2.1 \
    db-dtypes>=1.2

# Copiar a chave do service account (será montada via volume, mas
# deixamos aqui como fallback)
# NOTA: Em produção, use Secrets Manager ou Workload Identity
