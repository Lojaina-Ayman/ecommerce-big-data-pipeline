# Peak CPU% and memory per container from monitor CSVs.
param([string[]]$Files = @("bench\full_stage1.csv", "bench\full_stage3.csv", "bench\stage3.csv"))
function ToMiB([string]$s) {
  if ($s -match '^([\d\.]+)\s*([KkMG])i?B$') {
    $v = [double]$matches[1]
    switch ($matches[2].ToUpper()) { 'K' { return $v / 1024 } 'M' { return $v } 'G' { return $v * 1024 } }
  }
  return 0
}
$mem = @{}; $cpu = @{}
foreach ($f in $Files) {
  if (-not (Test-Path $f)) { continue }
  foreach ($row in Import-Csv $f) {
    foreach ($p in $row.PSObject.Properties) {
      if (-not $p.Value) { continue }
      if ($p.Name -like '*_mem') { $n = $p.Name -replace '_mem$', ''; $m = ToMiB $p.Value; if (-not $mem[$n] -or $m -gt $mem[$n]) { $mem[$n] = $m } }
      if ($p.Name -like '*_cpu') { $n = $p.Name -replace '_cpu$', ''; $c = [double]$p.Value; if (-not $cpu[$n] -or $c -gt $cpu[$n]) { $cpu[$n] = $c } }
    }
  }
}
$mem.Keys | Sort-Object | ForEach-Object {
  [pscustomobject]@{
    container         = $_
    peak_mem_MiB      = [math]::Round($mem[$_])
    peak_cpu_pct      = $cpu[$_]
    suggested_cap_MiB = [math]::Ceiling($mem[$_] * 1.25 / 128) * 128
  }
} | Format-Table -AutoSize
