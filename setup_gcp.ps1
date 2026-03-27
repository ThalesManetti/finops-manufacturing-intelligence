# =============================================================================
# FinOps Manufacturing Intelligence - Setup GCP (Fase 1)
# Script PowerShell para Windows
# =============================================================================
# Uso: Abra PowerShell como Administrador e execute:
#   .\setup_gcp.ps1
#
# Pré-requisitos:
#   - Google Cloud CLI (gcloud) instalado
#   - Autenticação feita (gcloud init)
# =============================================================================

# --- CONFIGURAÇÃO (edite se necessário) ---
$PROJECT_ID = "finops-manufacturing"
$REGION = "us-central1"
$BUCKET_NAME = "finops-manufacturing-datalake"
$SA_NAME = "finops-pipeline"
$SA_EMAIL = "$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
$CREDENTIALS_DIR = "credentials"
$KEY_FILE = "$CREDENTIALS_DIR\gcp-service-account.json"

Write-Host "=" * 70 -ForegroundColor Cyan
Write-Host " FinOps Manufacturing - Setup GCP (Fase 1)" -ForegroundColor Cyan
Write-Host "=" * 70 -ForegroundColor Cyan

# --- VERIFICAÇÕES INICIAIS ---
Write-Host "`n[0/8] Verificando pré-requisitos..." -ForegroundColor Yellod

# Verificar gcloud
if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
    Write-Host "ERRO: gcloud CLI não encontrado. Instale em: https://cloud.google.com/sdk/docs/install" -ForegroundColor Red
    exit 1
}
Write-Host "  OK - gcloud CLI encontrado" -ForegroundColor Green

# Verificar autenticação
$authAccount = gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>$null
if (-not $authAccount) {
    Write-Host "  Nenhuma conta autenticada. Executando gcloud init..." -ForegroundColor Yellow
    gcloud init
}
Write-Host "  OK - Autenticado como: $authAccount" -ForegroundColor Green

# --- ETAPA 1.1: PROJETO E APIs ---
Write-Host "`n[1/8] Selecionando projeto $PROJECT_ID..." -ForegroundColor Yellow
gcloud config set project $PROJECT_ID 2>$null

# Verificar se projeto existe
$projectExists = gcloud projects describe $PROJECT_ID --format="value(projectId)" 2>$null
if (-not $projectExists) {
    Write-Host "  Projeto não encontrado. Criando..." -ForegroundColor Yellow
    gcloud projects create $PROJECT_ID --name="FinOps Manufacturing"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ERRO: Não foi possível criar o projeto. Verifique se o ID é único." -ForegroundColor Red
        Write-Host "  Tente: gcloud projects create finops-manufacturing-2026" -ForegroundColor Yellow
        exit 1
    }
}
Write-Host "  OK - Projeto: $PROJECT_ID" -ForegroundColor Green

Write-Host "`n[2/8] Ativando APIs necessárias..." -ForegroundColor Yellow
$apis = @(
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com"
)
foreach ($api in $apis) {
    Write-Host "  Ativando $api..." -NoNewline
    gcloud services enable $api --project=$PROJECT_ID 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host " OK" -ForegroundColor Green
    } else {
        Write-Host " ERRO (verifique billing)" -ForegroundColor Red
    }
}

# --- ETAPA 1.1.4: SERVICE ACCOUNT ---
Write-Host "`n[3/8] Criando service account..." -ForegroundColor Yellow
$saExists = gcloud iam service-accounts describe $SA_EMAIL --project=$PROJECT_ID 2>$null
if (-not $saExists) {
    gcloud iam service-accounts create $SA_NAME `
        --display-name="FinOps Pipeline Service Account" `
        --description="Service account para Airflow, dbt e pipeline de dados" `
        --project=$PROJECT_ID
}
Write-Host "  OK - Service Account: $SA_EMAIL" -ForegroundColor Green

Write-Host "`n[4/8] Atribuindo permissões..." -ForegroundColor Yellow
$roles = @(
    "roles/bigquery.admin",
    "roles/storage.admin"
)
foreach ($role in $roles) {
    Write-Host "  Atribuindo $role..." -NoNewline
    gcloud projects add-iam-policy-binding $PROJECT_ID `
        --member="serviceAccount:$SA_EMAIL" `
        --role=$role `
        --quiet 2>$null | Out-Null
    Write-Host " OK" -ForegroundColor Green
}

Write-Host "`n[5/8] Gerando chave JSON..." -ForegroundColor Yellow
if (-not (Test-Path $CREDENTIALS_DIR)) {
    New-Item -ItemType Directory -Path $CREDENTIALS_DIR | Out-Null
}
if (-not (Test-Path $KEY_FILE)) {
    gcloud iam service-accounts keys create $KEY_FILE `
        --iam-account=$SA_EMAIL
    Write-Host "  OK - Chave salva em: $KEY_FILE" -ForegroundColor Green
} else {
    Write-Host "  Chave já existe em: $KEY_FILE" -ForegroundColor Yellow
}

# Configurar variável de ambiente
$fullPath = (Resolve-Path $KEY_FILE).Path
$env:GOOGLE_APPLICATION_CREDENTIALS = $fullPath
Write-Host "  GOOGLE_APPLICATION_CREDENTIALS = $fullPath" -ForegroundColor Green

# --- ETAPA 1.2: BUCKET GCS ---
Write-Host "`n[6/8] Criando bucket GCS..." -ForegroundColor Yellow
$bucketExists = gsutil ls "gs://$BUCKET_NAME/" 2>$null
if (-not $bucketExists) {
    gsutil mb -p $PROJECT_ID -l $REGION -c standard "gs://$BUCKET_NAME/"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  OK - Bucket criado: gs://$BUCKET_NAME/" -ForegroundColor Green
    } else {
        Write-Host "  ERRO: Nome pode já estar em uso. Tente: $BUCKET_NAME-2026" -ForegroundColor Red
    }
} else {
    Write-Host "  Bucket já existe: gs://$BUCKET_NAME/" -ForegroundColor Yellow
}

# Ativar versionamento
gsutil versioning set on "gs://$BUCKET_NAME/" 2>$null
Write-Host "  Versionamento ativado" -ForegroundColor Green

# --- ETAPA 1.5: DATASETS BIGQUERY ---
Write-Host "`n[7/8] Criando datasets no BigQuery..." -ForegroundColor Yellow
$datasets = @{
    "bronze_manufacturing" = "Camada Bronze - Dados brutos do GCS sem transformação"
    "silver_manufacturing" = "Camada Silver - Dados limpos, tipados e padronizados (dbt)"
    "gold_finance"         = "Camada Gold - Models de negócio: DRE, margem, KPIs (dbt)"
}
foreach ($ds in $datasets.GetEnumerator()) {
    $dsExists = bq show "${PROJECT_ID}:$($ds.Key)" 2>$null
    if (-not $dsExists) {
        bq mk --dataset `
            --description="$($ds.Value)" `
            --location=US `
            "${PROJECT_ID}:$($ds.Key)"
        Write-Host "  OK - Dataset: $($ds.Key)" -ForegroundColor Green
    } else {
        Write-Host "  Dataset já existe: $($ds.Key)" -ForegroundColor Yellow
    }
}

# --- VALIDAÇÃO FINAL ---
Write-Host "`n[8/8] Validação final..." -ForegroundColor Yellow
Write-Host ""

# Projeto
$proj = gcloud config get-value project 2>$null
Write-Host "  Projeto:        " -NoNewline
if ($proj -eq $PROJECT_ID) { Write-Host "OK ($proj)" -ForegroundColor Green }
else { Write-Host "FALHOU (esperado: $PROJECT_ID, obtido: $proj)" -ForegroundColor Red }

# APIs
$enabledApis = gcloud services list --enabled --format="value(config.name)" --project=$PROJECT_ID 2>$null
foreach ($api in $apis) {
    Write-Host "  API $api`: " -NoNewline
    if ($enabledApis -match $api) { Write-Host "OK" -ForegroundColor Green }
    else { Write-Host "FALHOU" -ForegroundColor Red }
}

# Service Account
$saCheck = gcloud iam service-accounts describe $SA_EMAIL --project=$PROJECT_ID 2>$null
Write-Host "  Service Account: " -NoNewline
if ($saCheck) { Write-Host "OK" -ForegroundColor Green }
else { Write-Host "FALHOU" -ForegroundColor Red }

# Chave JSON
Write-Host "  Chave JSON:      " -NoNewline
if (Test-Path $KEY_FILE) { Write-Host "OK ($KEY_FILE)" -ForegroundColor Green }
else { Write-Host "FALHOU" -ForegroundColor Red }

# Bucket
$bucketCheck = gsutil ls "gs://$BUCKET_NAME/" 2>$null
Write-Host "  Bucket GCS:      " -NoNewline
if ($bucketCheck -ne $null) { Write-Host "OK (gs://$BUCKET_NAME/)" -ForegroundColor Green }
else { Write-Host "FALHOU" -ForegroundColor Red }

# Datasets
foreach ($ds in $datasets.Keys) {
    $dsCheck = bq show "${PROJECT_ID}:$ds" 2>$null
    Write-Host "  Dataset $ds`: " -NoNewline
    if ($dsCheck) { Write-Host "OK" -ForegroundColor Green }
    else { Write-Host "FALHOU" -ForegroundColor Red }
}

Write-Host "`n" + "=" * 70 -ForegroundColor Cyan
Write-Host " SETUP FASE 1 COMPLETO!" -ForegroundColor Cyan
Write-Host " Próximo passo: Upload dos Parquets (etapa 1.4)" -ForegroundColor Cyan
Write-Host " Execute: .\upload_gcs.ps1" -ForegroundColor Cyan
Write-Host "=" * 70 -ForegroundColor Cyan
