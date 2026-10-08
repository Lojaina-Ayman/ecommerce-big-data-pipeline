# End-to-end golden test using only tests/fixtures (no Kaggle data needed). Exit code 0 = PASS.
# Usage: scripts\smoke-test.ps1 [-TimeoutMin 8]
param([int]$TimeoutMin = 8)

$root = (Resolve-Path "$PSScriptRoot\..").Path
Push-Location $root
$ch = "$PSScriptRoot\ch.ps1"
function Get-KafkaTotal {
  $t = 0
  foreach ($l in (docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic ecommerce_events_stream --time -1 2>$null)) {
    if ("$l" -match ':(\d+)$') { $t += [long]$matches[1] }
  }
  return $t
}
function Wait-Until([scriptblock]$cond, [string]$what) {
  $t0 = Get-Date
  while (((Get-Date) - $t0).TotalMinutes -lt $TimeoutMin) { if (& $cond) { return }; Start-Sleep -Seconds 5 }
  throw "Timed out waiting for: $what"
}
try {
  Write-Host "1. Clean slate" -ForegroundColor Cyan
  docker compose up -d kafka clickhouse | Out-Null
  & "$PSScriptRoot\reset-pipeline-state.ps1" -Kafka

  Write-Host "2. Ingest the three fixtures through Kafka Connect" -ForegroundColor Cyan
  docker compose --profile ingest up -d kafka-connect | Out-Null
  Wait-Until { try { (Invoke-RestMethod "http://localhost:8083/connectors/ecommerce-csv-source/status").connector.state -eq 'RUNNING' } catch { $false } } "connector RUNNING"
  foreach ($f in 'bad_rows', 'abandonment', 'airflow_load_test') {
    & "$PSScriptRoot\load-csv.ps1" -Csv "$root\tests\fixtures\$f.csv"
  }
  Wait-Until { (Get-KafkaTotal) -ge 30 } "30 messages in Kafka"
  if ((Get-KafkaTotal) -ne 30) { throw "Kafka holds $(Get-KafkaTotal) messages, expected exactly 30" }
  docker compose --profile ingest stop kafka-connect | Out-Null

  Write-Host "3. Process with Spark (one micro-batch) into ClickHouse" -ForegroundColor Cyan
  $env:SPARK_SINK = 'clickhouse'; $env:SPARK_TRIGGER_SECONDS = '0'; $env:SPARK_MAX_OFFSETS_PER_TRIGGER = '20000'
  docker compose --profile process up -d spark-master spark-worker | Out-Null
  docker compose --profile process --profile job up -d spark-job | Out-Null
  Wait-Until { try { [long](("$(& $ch -Query 'SELECT toUInt64(ifNull(sum(total_rows),0)) FROM batch_ledger FORMAT TSV')").Trim()) -ge 30 } catch { $false } } "ledger total = 30"
  docker compose --profile process --profile job stop spark-job | Out-Null

  Write-Host "4. Assertions" -ForegroundColor Cyan
  $row = ("$(& $ch -Query "SELECT (SELECT toUInt64(ifNull(sum(total_rows),0)) FROM batch_ledger), (SELECT toUInt64(ifNull(sum(valid_rows),0)) FROM batch_ledger), (SELECT toUInt64(ifNull(sum(quarantined_rows),0)) FROM batch_ledger), (SELECT toUInt64(ifNull(sum(duplicates_removed),0)) FROM batch_ledger), total_events, views, carts, purchases, toFloat64(total_revenue), unique_users, unique_sessions, view_to_cart, cart_to_purchase, view_to_purchase, cart_abandonment_rate FROM v_kpi_summary FORMAT TSV")").Trim() -split "\t"
  $names    = 'ledger_total', 'ledger_valid', 'ledger_quarantined', 'ledger_duplicates', 'total_events', 'views', 'carts', 'purchases', 'total_revenue', 'unique_users', 'unique_sessions', 'view_to_cart', 'cart_to_purchase', 'view_to_purchase', 'cart_abandonment_rate'
  $expected = 30, 22, 7, 1, 22, 10, 7, 5, 955, 15, 15, 0.7, 0.7143, 0.5, 0.7143
  $fail = 0
  $results = for ($i = 0; $i -lt $names.Count; $i++) {
    $a = [double]$row[$i]; $ok = [math]::Abs($a - $expected[$i]) -lt 0.0002
    if (-not $ok) { $fail++ }
    [pscustomobject]@{ check = $names[$i]; expected = $expected[$i]; actual = $a; result = $(if ($ok) { 'PASS' } else { 'FAIL' }) }
  }
  $reasons = ("$(& $ch -Query "SELECT arrayStringConcat(arraySort(groupArray(reject_reason)), ',') FROM events_quarantine FORMAT TSV")").Trim()
  $wantReasons = 'invalid_event_type,invalid_price,invalid_timestamp,invalid_user_id,missing_price_on_purchase,missing_product_id,negative_price'
  $okR = ($reasons -eq $wantReasons); if (-not $okR) { $fail++ }
  $results += [pscustomobject]@{ check = 'quarantine_reasons'; expected = $wantReasons; actual = $reasons; result = $(if ($okR) { 'PASS' } else { 'FAIL' }) }

  $sql = (Get-Content "$root\clickhouse\queries\reconcile.sql" -Raw).Trim().TrimEnd(';')
  $bad = [int]("$(& $ch -Query "SELECT countIf(ok = 0) FROM ($sql) FORMAT TSV")").Trim()
  if ($bad -ne 0) { $fail++ }
  $results += [pscustomobject]@{ check = 'reconcile_failures'; expected = 0; actual = $bad; result = $(if ($bad -eq 0) { 'PASS' } else { 'FAIL' }) }

  $results | Format-Table -AutoSize
  if ($fail -eq 0) { Write-Host "SMOKE TEST PASSED" -ForegroundColor Green; exit 0 }
  Write-Host "SMOKE TEST FAILED ($fail checks)" -ForegroundColor Red; exit 1
} finally {
  Remove-Item Env:SPARK_SINK, Env:SPARK_TRIGGER_SECONDS, Env:SPARK_MAX_OFFSETS_PER_TRIGGER -ErrorAction SilentlyContinue
  Pop-Location
}