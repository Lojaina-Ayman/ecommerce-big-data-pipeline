# One time-boxed Spark experiment on whatever is currently in Kafka.
# Usage: scripts\run-experiment.ps1 -Name ch_100k -Sink clickhouse -MaxOffsets 100000 -ChBatchSize 100000 -Minutes 5
param(
  [Parameter(Mandatory = $true)][string]$Name,
  [ValidateSet('noop', 'clickhouse')][string]$Sink = 'noop',
  [int]$MaxOffsets = 20000,
  [int]$TriggerSeconds = 0,
  [int]$ChBatchSize = 10000,
  [string]$RepartitionByPartition = 'true',
  [string]$ExecutorMemory = '2g',
  [string]$MinPartitions = '',
  [string]$WriteFormat = 'json',
  [int]$CoresMax = 2,
  [int]$Minutes = 4
)
$root = (Resolve-Path "$PSScriptRoot\..").Path
Push-Location $root
function Median($v) { $s = @($v | Sort-Object); if ($s.Count -eq 0) { return 0 }; return $s[[int][math]::Floor(($s.Count - 1) / 2)] }
try {
  $reset = @{}
  if ($Sink -eq 'noop') { $reset['SkipClickHouse'] = $true }
  & "$PSScriptRoot\reset-pipeline-state.ps1" @reset

  $env:SPARK_SINK = $Sink; $env:SPARK_MAX_OFFSETS_PER_TRIGGER = "$MaxOffsets"; $env:SPARK_TRIGGER_SECONDS = "$TriggerSeconds"
  $env:CH_WRITE_BATCH_SIZE = "$ChBatchSize"; $env:CH_REPARTITION_BY_PARTITION = $RepartitionByPartition
  $env:SPARK_EXECUTOR_MEMORY = $ExecutorMemory; $env:KAFKA_MIN_PARTITIONS = $MinPartitions
  $env:CH_WRITE_FORMAT = $WriteFormat; $env:SPARK_CORES_MAX = "$CoresMax"

  docker compose --profile process up -d spark-master spark-worker 2>&1 | Out-Null
  docker compose --profile process --profile job up -d spark-job 2>&1 | Out-Null
  Write-Host "Running '$Name' for $Minutes min..."
  Start-Sleep -Seconds ($Minutes * 60)

  $logs = docker compose --profile process --profile job logs --no-color spark-job 2>&1
  $rows = @()
  foreach ($l in $logs) {
    if ("$l" -match '\[progress\]\s+\S+\s+batch=(\d+)\s+rows=(\d+)\s+in/s=(\d+)\s+processed/s=(\d+)\s+trigger_ms=(\d+)') {
      $rows += [pscustomobject]@{ batch = [int]$matches[1]; rows = [long]$matches[2]; procs = [long]$matches[4]; ms = [long]$matches[5] }
    }
  }
  $rows = @($rows | Where-Object { $_.rows -gt 0 })
  $steady = if ($rows.Count -gt 4) { @($rows | Where-Object { $_.batch -ge 2 }) } else { $rows }
  $sumRows = ($steady | Measure-Object rows -Sum).Sum
  $sumMs = ($steady | Measure-Object ms -Sum).Sum
  $busyRate = if ($sumMs) { [math]::Round($sumRows / ($sumMs / 1000)) } else { 0 }
  $failed = @($logs | Select-String -Pattern 'Exception|OutOfMemory|query terminated.*error=(?!none)' ).Count

  $ledger = ""; $parts = ""; $maxParts = ""
  if ($Sink -eq 'clickhouse') {
    $r = & "$PSScriptRoot\ch.ps1" -Query "SELECT (SELECT toUInt64(ifNull(sum(total_rows),0)) FROM batch_ledger), (SELECT count() FROM system.parts WHERE database='ecommerce' AND table='ecommerce_events' AND active), (SELECT ifNull(max(c),0) FROM (SELECT count() AS c FROM system.parts WHERE database='ecommerce' AND table='ecommerce_events' AND active GROUP BY partition)) FORMAT TSV"
    $f = ("$r").Trim() -split "\s+"
    if ($f.Count -ge 3) { $ledger = $f[0]; $parts = $f[1]; $maxParts = $f[2] }
  }
  docker compose --profile process --profile job stop spark-job 2>&1 | Out-Null

  $summary = [pscustomobject]@{
    name = $Name; sink = $Sink; max_offsets = $MaxOffsets; trigger_s = $TriggerSeconds; ch_batch = $ChBatchSize
    repartition = $RepartitionByPartition; exec_mem = $ExecutorMemory; min_partitions = $MinPartitions
    write_format = $WriteFormat; cores_max = $CoresMax; batches = $rows.Count
    median_processed_rows_s = (Median ($steady | ForEach-Object { $_.procs })); busy_rows_s = $busyRate
    median_batch_ms = (Median ($steady | ForEach-Object { $_.ms })); ledger_rows = $ledger
    active_parts = $parts; max_parts_in_partition = $maxParts; error_lines = $failed
  }
  $summary | Format-List
  New-Item -ItemType Directory -Force -Path "$root\bench" | Out-Null
  $summary | Export-Csv -Path "$root\bench\experiments_p11.csv" -Append -NoTypeInformation
} finally {
  Remove-Item Env:SPARK_SINK, Env:SPARK_MAX_OFFSETS_PER_TRIGGER, Env:SPARK_TRIGGER_SECONDS, Env:CH_WRITE_BATCH_SIZE, Env:CH_REPARTITION_BY_PARTITION, Env:SPARK_EXECUTOR_MEMORY, Env:KAFKA_MIN_PARTITIONS, Env:CH_WRITE_FORMAT, Env:SPARK_CORES_MAX -ErrorAction SilentlyContinue
  Pop-Location
}