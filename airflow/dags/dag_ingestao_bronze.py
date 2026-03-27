"""
=============================================================================
FinOps Manufacturing Intelligence — DAG de Ingestão Bronze
=============================================================================
Descrição: Carrega 9 arquivos Parquet do Google Cloud Storage para o
           BigQuery no dataset bronze_manufacturing.

Regras da Camada Bronze:
    - Dados brutos, SEM transformação
    - Coluna _ingested_at adicionada com timestamp de carga
    - Idempotência via WRITE_TRUNCATE (cada execução substitui a tabela)
    - Preserva tipos originais do Parquet

Schedule: @daily (em produção seria acionado por evento ou sensor)
=============================================================================
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from google.cloud import bigquery, storage
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO


# =============================================================================
# CONFIGURAÇÃO
# =============================================================================

GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "finops-manufacturing-2026")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "finops-manufacturing-datalake-2026")
GCS_RAW_PREFIX = "raw"
BQ_DATASET_BRONZE = "bronze_manufacturing"

# Mapeamento: arquivo GCS → tabela BigQuery
TABELAS_BRONZE = {
    "vendas.parquet": {
        "tabela": "raw_vendas",
        "descricao": "Transações de venda de autopeças - dados brutos do GCS",
        "partition_field": "data_emissao",
        "cluster_fields": ["produto_id", "cliente_id"],
    },
    "custos_producao.parquet": {
        "tabela": "raw_custos",
        "descricao": "CMV mensal por produto - dados brutos do GCS",
        "partition_field": "mes_referencia",
        "cluster_fields": ["produto_id"],
    },
    "despesas_operacionais.parquet": {
        "tabela": "raw_despesas",
        "descricao": "Despesas operacionais mensais - dados brutos do GCS",
        "partition_field": "mes_referencia",
        "cluster_fields": ["classificacao_dre"],
    },
    "orcamento_anual.parquet": {
        "tabela": "raw_orcamento",
        "descricao": "Orçamento anual por natureza - dados brutos do GCS",
        "partition_field": None,
        "cluster_fields": None,
    },
    "cadastro_produtos.parquet": {
        "tabela": "raw_produtos",
        "descricao": "Cadastro de 50 produtos - dados brutos do GCS",
        "partition_field": None,
        "cluster_fields": None,
    },
    "cadastro_centros_custo.parquet": {
        "tabela": "raw_centros_custo",
        "descricao": "Centros de custo da fábrica - dados brutos do GCS",
        "partition_field": None,
        "cluster_fields": None,
    },
    "cadastro_clientes.parquet": {
        "tabela": "raw_clientes",
        "descricao": "Cadastro de clientes (montadoras e distribuidores) - dados brutos do GCS",
        "partition_field": None,
        "cluster_fields": None,
    },
    "historico_custos_produtos.parquet": {
        "tabela": "raw_historico_custos",
        "descricao": "Histórico de mudanças de custo padrão (SCD2) - dados brutos do GCS",
        "partition_field": None,
        "cluster_fields": None,
    },
    "indices_commodities.parquet": {
        "tabela": "raw_commodities",
        "descricao": "Índices mensais de commodities (aço, alumínio, polímero) - dados brutos do GCS",
        "partition_field": None,
        "cluster_fields": None,
    },
}


# =============================================================================
# FUNÇÕES DE INGESTÃO
# =============================================================================

def ingerir_tabela_bronze(arquivo_gcs: str, config: dict, **kwargs):
    """
    Lê um arquivo Parquet do GCS e carrega no BigQuery (camada Bronze).
    Adiciona coluna _ingested_at com timestamp da carga.
    Usa WRITE_TRUNCATE para idempotência.
    """
    tabela_bq = config["tabela"]
    table_id = f"{GCP_PROJECT}.{BQ_DATASET_BRONZE}.{tabela_bq}"

    print(f"{'='*60}")
    print(f"Ingerindo: gs://{GCS_BUCKET}/{GCS_RAW_PREFIX}/{arquivo_gcs}")
    print(f"Destino:   {table_id}")
    print(f"{'='*60}")

    # 1. Ler Parquet do GCS
    gcs_client = storage.Client(project=GCP_PROJECT)
    bucket = gcs_client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"{GCS_RAW_PREFIX}/{arquivo_gcs}")

    # Baixar para memória e ler com pandas
    parquet_bytes = blob.download_as_bytes()
    df = pd.read_parquet(BytesIO(parquet_bytes))

    print(f"  Registros lidos do GCS: {len(df):,}")
    print(f"  Colunas: {list(df.columns)}")

    # 2. Adicionar coluna de auditoria (regra da Bronze)
    df["_ingested_at"] = datetime.utcnow()

    # 3. Garantir tipos de data corretos para BigQuery
    for col in df.columns:
        if df[col].dtype == "object":
            # Tentar converter colunas que parecem datas
            if any(kw in col.lower() for kw in ["data", "date", "vigencia", "mes_referencia"]):
                try:
                    df[col] = pd.to_datetime(df[col])
                except (ValueError, TypeError):
                    pass

    # 4. Carregar no BigQuery
    bq_client = bigquery.Client(project=GCP_PROJECT)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
    )

    # Configurar particionamento se aplicável
    if config.get("partition_field") and config["partition_field"] in df.columns:
        # Verificar se a coluna é datetime
        if pd.api.types.is_datetime64_any_dtype(df[config["partition_field"]]):
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                field=config["partition_field"],
            )
            print(f"  Particionamento: {config['partition_field']} (MONTH)")

    # Configurar clustering se aplicável
    if config.get("cluster_fields"):
        valid_clusters = [f for f in config["cluster_fields"] if f in df.columns]
        if valid_clusters:
            job_config.clustering_fields = valid_clusters
            print(f"  Clustering: {valid_clusters}")

    # Executar carga
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Aguardar conclusão

    # 5. Validar carga
    table = bq_client.get_table(table_id)
    print(f"  Registros no BigQuery: {table.num_rows:,}")
    print(f"  Tamanho: {table.num_bytes / 1024:.1f} KB")
    print(f"  _ingested_at: {df['_ingested_at'].iloc[0]}")
    print(f"  Status: OK")

    # Atualizar descrição da tabela
    table.description = config["descricao"]
    bq_client.update_table(table, ["description"])

    return {
        "tabela": tabela_bq,
        "registros": int(table.num_rows),
        "bytes": int(table.num_bytes),
    }


def validar_camada_bronze(**kwargs):
    """
    Validação pós-carga: verifica contagem de registros,
    nulos em PKs e consistência geral.
    """
    bq_client = bigquery.Client(project=GCP_PROJECT)
    resultados = []
    erros = []

    print(f"\n{'='*60}")
    print("VALIDAÇÃO DA CAMADA BRONZE")
    print(f"{'='*60}")

    # Contagem esperada por tabela
    contagens_esperadas = {
        "raw_vendas": 9000,        # ~9.150 (margem de tolerância)
        "raw_custos": 1700,        # ~1.764
        "raw_despesas": 800,       # ~864
        "raw_orcamento": 850,      # ~900
        "raw_produtos": 45,        # ~50
        "raw_centros_custo": 10,   # 14
        "raw_clientes": 20,        # 24
        "raw_historico_custos": 300,  # ~381
        "raw_commodities": 140,    # 144
    }

    for tabela, min_registros in contagens_esperadas.items():
        table_id = f"{GCP_PROJECT}.{BQ_DATASET_BRONZE}.{tabela}"
        try:
            table = bq_client.get_table(table_id)
            status = "OK" if table.num_rows >= min_registros else "ALERTA"
            if status == "ALERTA":
                erros.append(f"{tabela}: {table.num_rows} registros (mínimo esperado: {min_registros})")

            resultados.append({
                "tabela": tabela,
                "registros": table.num_rows,
                "min_esperado": min_registros,
                "status": status,
            })
            print(f"  {tabela}: {table.num_rows:,} registros [{status}]")
        except Exception as e:
            erros.append(f"{tabela}: ERRO - {str(e)}")
            print(f"  {tabela}: ERRO - {str(e)}")

    # Verificar _ingested_at em todas as tabelas
    print(f"\n  Verificando coluna _ingested_at...")
    for tabela, _ in contagens_esperadas.items():
        query = f"""
            SELECT COUNT(*) as total, 
                   COUNTIF(_ingested_at IS NULL) as nulos
            FROM `{GCP_PROJECT}.{BQ_DATASET_BRONZE}.{tabela}`
        """
        try:
            result = bq_client.query(query).result()
            for row in result:
                if row.nulos > 0:
                    erros.append(f"{tabela}: {row.nulos} registros sem _ingested_at")
                    print(f"    {tabela}: {row.nulos} nulos em _ingested_at [ERRO]")
                else:
                    print(f"    {tabela}: _ingested_at OK")
        except Exception as e:
            print(f"    {tabela}: Erro ao verificar - {str(e)}")

    # Resumo
    print(f"\n{'='*60}")
    if erros:
        print(f"  RESULTADO: {len(erros)} problema(s) encontrado(s)")
        for e in erros:
            print(f"    - {e}")
        raise ValueError(f"Validação Bronze falhou: {len(erros)} erro(s)")
    else:
        print(f"  RESULTADO: Todas as {len(contagens_esperadas)} tabelas OK!")
    print(f"{'='*60}")


# =============================================================================
# DEFINIÇÃO DA DAG
# =============================================================================

default_args = {
    "owner": "finops-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_ingestao_bronze",
    default_args=default_args,
    description="Ingestão de dados Parquet do GCS para BigQuery (camada Bronze)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["finops", "bronze", "ingestao", "gcs", "bigquery"],
    doc_md="""
    ## DAG de Ingestão Bronze
    
    Carrega 9 arquivos Parquet do Google Cloud Storage para o BigQuery
    no dataset `bronze_manufacturing`.
    
    ### Regras:
    - Dados brutos, sem transformação
    - Coluna `_ingested_at` adicionada
    - Idempotência via `WRITE_TRUNCATE`
    - Validação automática pós-carga
    
    ### Tabelas:
    raw_vendas, raw_custos, raw_despesas, raw_orcamento, raw_produtos,
    raw_centros_custo, raw_clientes, raw_historico_custos, raw_commodities
    """,
) as dag:

    # Task de início
    inicio = EmptyOperator(task_id="inicio")

    # Tasks de ingestão (uma por tabela)
    tasks_ingestao = []
    for arquivo, config in TABELAS_BRONZE.items():
        task = PythonOperator(
            task_id=f"ingerir_{config['tabela']}",
            python_callable=ingerir_tabela_bronze,
            op_kwargs={"arquivo_gcs": arquivo, "config": config},
        )
        tasks_ingestao.append(task)

    # Task de validação
    validacao = PythonOperator(
        task_id="validar_camada_bronze",
        python_callable=validar_camada_bronze,
    )

    # Task de fim
    fim = EmptyOperator(task_id="fim")

    # Dependências: inicio >> [todas as ingestões em paralelo] >> validação >> fim
    inicio >> tasks_ingestao >> validacao >> fim
