param(
  [int]$IntervalSec = 10,
  [string]$Out = "bench\monitor.csv",
  [int]$Minutes = 0
)

$root = (Resolve-Path "$PSScriptRoot\..").Path
Push-Location $root

New-Item -ItemType Directory -Force -Path (Split-Path $Out) | Out-Null
$names = @("kafka", "kafka-connect", "spark-worker", "spark-job", "clickhouse")

if (-not (Test-Path $Out)) {
  $hdr = @("time", "kafka_total", "ledger_total", "kafka_rows_s", "ledger_rows_s", "lag", "events_parts")
  foreach ($n in $names) {
    $hdr += "${n}_cpu"
    $hdr += "${n}_mem"
  }
  Set-Content -Path $Out -Value ($hdr -join ",")
}

$prev = $null
$start = Get-Date

try {
  while ($true) {
    $now = Get-Date

    $kt = 0
    $o = docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic ecommerce_events_stream --time -1 2>$null
    foreach ($l in $o) {
      if ("$l" -match ':(\d+)$') {
        $kt += [long]$matches[1]
      }
    }

    $lt = $null
    $parts = $null
    try {
      $r = & "$PSScriptRoot\ch.ps1" -Query "SELECT (SELECT toUInt64(ifNull(sum(total_rows),0)) FROM batch_ledger), (SELECT count() FROM system.parts WHERE database='ecommerce' AND table='ecommerce_events' AND active) FORMAT TSV" 2>$null
      $f = ("$r").Trim() -split "\s+"
      if ($f.Count -ge 2) {
        $lt = [long]$f[0]
        $parts = [long]$f[1]
      }
    } catch {}

    $st = @{}
    docker stats --no-stream --format "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}" 2>$null | ForEach-Object {
      $p = "$_" -split "\|"
      if ($p.Count -eq 3) {
        $st[$p[0]] = @(($p[1] -replace '%', '').Trim(), (($p[2] -split " / ")[0]).Trim())
      }
    }

    $kr = $null
    $lr = $null
    if ($prev) {
      $dt = ($now - $prev.t).TotalSeconds
      if ($dt -gt 0) {
        $kr = [math]::Round(($kt - $prev.k) / $dt)
        if ($null -ne $lt -and $null -ne $prev.l) {
          $lr = [math]::Round(($lt - $prev.l) / $dt)
        }
      }
    }
    $lag = if ($null -ne $lt) { $kt - $lt } else { $null }

    $line = "{0:HH:mm:ss} kafka={1} (+{2}/s) ledger={3} (+{4}/s) lag={5} parts={6}" -f $now, $kt, $kr, $lt, $lr, $lag, $parts
    foreach ($n in $names) {
      if ($st.ContainsKey($n)) {
        $line += " | ${n}: $($st[$n][0])% $($st[$n][1])"
      }
    }
    Write-Host $line

    $row = @(("{0:HH:mm:ss}" -f $now), $kt, $lt, $kr, $lr, $lag, $parts)
    foreach ($n in $names) {
      if ($st.ContainsKey($n)) {
        $row += $st[$n][0]
        $row += $st[$n][1]
      } else {
        $row += ""
        $row += ""
      }
    }
    Add-Content -Path $Out -Value ($row -join ",")

    $prev = @{ t = $now; k = $kt; l = $lt }

    if ($Minutes -gt 0 -and ((Get-Date) - $start).TotalMinutes -ge $Minutes) {
      break
    }
    Start-Sleep -Seconds $IntervalSec
  }
} finally {
  Pop-Location
}
