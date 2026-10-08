param([switch]$Kafka, [switch]$SkipClickHouse)

$root = (Resolve-Path "$PSScriptRoot\..").Path
Push-Location $root
try {
  Write-Host "Stopping spark-job..."
  docker compose --profile process --profile job rm -sf spark-job 2>&1 | Out-Null

  Write-Host "Deleting Spark checkpoints..."
  docker compose --profile process run --rm --no-deps spark-master sh -c "rm -rf /opt/spark-checkpoints/*" 2>&1 | Out-Null

  foreach ($d in "input", "finished", "error", "unprocessed") {
    Get-ChildItem "$root\data\connect\$d" -File -ErrorAction SilentlyContinue |
      Where-Object { $_.Name -ne ".gitkeep" } | Remove-Item -Force
  }

  if (-not $SkipClickHouse) {
    Write-Host "Truncating ClickHouse data tables..."
    docker compose up -d clickhouse 2>&1 | Out-Null
    & "$PSScriptRoot\ch.ps1" -File "$root\clickhouse\queries\reset_data.sql"
  }

  if ($Kafka) {$topic = "ecommerce_events_stream"
    Write-Host "Recreating Kafka topic $topic..."
    docker compose up -d kafka 2>&1 | Out-Null
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic $topic 2>&1 | Out-Null
    for ($i = 0; $i -lt 30; $i++) {$exists = docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>$null \vert{} Select-String -SimpleMatch$topic
      if (-not $exists) { break }
      Start-Sleep -Seconds 2
    }
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic $topic `
      --partitions 3 --replication-factor 1 --config retention.ms=604800000 --config compression.type=lz4
  }
  Write-Host "Reset done."
} finally { Pop-Location }