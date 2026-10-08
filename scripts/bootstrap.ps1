# One-command setup of a fresh clone. Idempotent: safe to re-run.
# Usage: scripts\bootstrap.ps1 [-SkipBuildImages]
param([switch]$SkipBuildImages)

$root = (Resolve-Path "$PSScriptRoot\..").Path
Push-Location $root
function Read-Env { $m = @{}; Get-Content "$root\.env" | ForEach-Object { if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') { $m[$matches[1]] = $matches[2].Trim() } }; $m }
function Wait-Healthy([string]$container, [int]$timeoutSec = 300) {
  $t0 = Get-Date
  while (((Get-Date) - $t0).TotalSeconds -lt $timeoutSec) {
    if ((docker inspect -f '{{.State.Health.Status}}' $container 2>$null) -eq 'healthy') { return }
    Start-Sleep -Seconds 5
  }
  throw "$container did not become healthy within $timeoutSec seconds (docker compose logs $container)"
}
try {
  Write-Host "== 1/8 Prerequisites" -ForegroundColor Cyan
  docker version --format '{{.Server.Version}}' | Out-Null; if ($LASTEXITCODE -ne 0) { throw "Docker is not running" }
  $mem = [long](docker info --format '{{.MemTotal}}')
  if ($mem -lt 9GB) { Write-Warning ("Docker has {0:N1} GB. 10 GB or more is recommended (see README: .wslconfig)." -f ($mem / 1GB)) }

  Write-Host "== 2/8 .env" -ForegroundColor Cyan
  & "$PSScriptRoot\init-env.ps1"
  $cfg = Read-Env

  Write-Host "== 3/8 Spark jar" -ForegroundColor Cyan
  if (-not (Test-Path "$root\spark\apps\ecommerce-pipeline.jar")) {
    docker build -f spark/Dockerfile --target export --output type=local,dest=spark/apps spark
    if (-not (Test-Path "$root\spark\apps\ecommerce-pipeline.jar")) { throw "Spark jar build failed" }
  }

  Write-Host "== 4/8 Images" -ForegroundColor Cyan
  if (-not $SkipBuildImages) { docker compose --profile "*" build }

  Write-Host "== 5/8 Kafka and ClickHouse" -ForegroundColor Cyan
  docker compose up -d kafka clickhouse
  Wait-Healthy kafka; Wait-Healthy clickhouse

  Write-Host "== 6/8 Topic" -ForegroundColor Cyan
  $topic = $cfg['KAFKA_TOPIC']; if (-not $topic) { $topic = "ecommerce_events_stream" }
  $have = docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>$null | Select-String -SimpleMatch $topic
  if (-not $have) {
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic $topic `
      --partitions 3 --replication-factor 1 --config retention.ms=604800000 --config compression.type=lz4
  }

  Write-Host "== 7/8 Kafka Connect + connector" -ForegroundColor Cyan
  docker compose --profile ingest up -d kafka-connect
  $port = $cfg['CONNECT_REST_PORT']; if (-not $port) { $port = 8083 }
  $up = $false
  for ($i = 0; $i -lt 60 -and -not $up; $i++) { try { Invoke-RestMethod "http://localhost:$port/" | Out-Null; $up = $true } catch { Start-Sleep -Seconds 5 } }
  if (-not $up) { throw "Kafka Connect REST did not come up (docker compose logs kafka-connect)" }
  try { Invoke-RestMethod "http://localhost:$port/connectors/ecommerce-csv-source/status" | Out-Null; Write-Host "connector already registered" }
  catch { Invoke-RestMethod -Method Post -Uri "http://localhost:$port/connectors" -ContentType "application/json" -Body (Get-Content "$root\kafka\config\spooldir-ecommerce.json" -Raw) | Out-Null; Write-Host "connector registered" }

  Write-Host "== 8/8 Superset's read-only ClickHouse user" -ForegroundColor Cyan
  & "$PSScriptRoot\create-superset-user.ps1"

  Write-Host ""
  Write-Host "Bootstrap complete. Next: scripts\smoke-test.ps1 (golden test on the committed fixtures)" -ForegroundColor Green
} finally { Pop-Location }