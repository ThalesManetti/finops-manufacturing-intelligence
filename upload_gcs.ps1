# =============================================================================
# FinOps Manufacturing Intelligence - Upload para GCS (Etapa 1.4)
# Script PowerShell para Windows
# =============================================================================
# Uso: .\upload_gcs.ps1
# Pré-requisito: setup_gcp.ps1 já executado com sucesso
# =============================================================================

$BUCKET_NAME = "finops-manufacturing-datalake"
$PARQUET_DIR = "data\parquet"
$GCS_RAW_PATH = "gs://$BUCKET_NAME/raw"

Write-Host "=" * 70 -ForegroundColor Cyan
Write-Host " FinOps Manufacturing - Upload para GCS (Etapa 1.4)" -ForegroundColor Cyan
Write-Host "=" * 70 -ForegroundColor Cyan

# Verificar se os Parquets existem
if (-not (Test-Path $PARQUET_DIR)) {
    Write-Host "ERRO: Pasta $PARQUET_DIR não encontrada." -ForegroundColor Red
    Write-Host "Execute primeiro o script de geração de dados (generate_data.py)" -ForegroundColor Yellow
    exit 1
}

$files = Get-ChildItem "$PARQUET_DIR\*.parquet"
if ($files.Count -eq 0) {
    Write-Host "ERRO: Nenhum arquivo .parquet encontrado em $PARQUET_DIR" -ForegroundColor Red
    exit 1
}

Write-Host "`n[1/3] Encontrados $($files.Count) arquivos Parquet:" -ForegroundColor Yellow
foreach ($f in $files) {
    $sizeKB = [math]::Round($f.Length / 1024, 1)
    Write-Host "  $($f.Name) ($sizeKB KB)"
}

# Upload paralelo
Write-Host "`n[2/3] Fazendo upload para $GCS_RAW_PATH/..." -ForegroundColor Yellow
gsutil -m cp "$PARQUET_DIR\*.parquet" "$GCS_RAW_PATH/"

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERRO: Upload falhou. Verifique credenciais e bucket." -ForegroundColor Red
    exit 1
}
Write-Host "  Upload concluído!" -ForegroundColor Green

# Validação
Write-Host "`n[3/3] Validando upload..." -ForegroundColor Yellow
$gcsFiles = gsutil ls "$GCS_RAW_PATH/*.parquet" 2>$null
$gcsCount = ($gcsFiles | Measure-Object).Count

Write-Host "  Arquivos no GCS: $gcsCount" -NoNewline
if ($gcsCount -eq $files.Count) {
    Write-Host " OK (esperado: $($files.Count))" -ForegroundColor Green
} else {
    Write-Host " FALHOU (esperado: $($files.Count))" -ForegroundColor Red
}

# Listar com tamanhos
Write-Host "`n  Detalhes:"
gsutil ls -l "$GCS_RAW_PATH/"

Write-Host "`n" + "=" * 70 -ForegroundColor Cyan
Write-Host " UPLOAD CONCLUIDO! Dados prontos no GCS." -ForegroundColor Cyan
Write-Host " Próximo passo: Fase 2 - Setup Airflow (Docker)" -ForegroundColor Cyan
Write-Host "=" * 70 -ForegroundColor Cyan
