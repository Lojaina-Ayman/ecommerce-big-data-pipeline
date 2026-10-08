# Usage: scripts\compare-workload.ps1 -A baseline1 -B baseline2
param([Parameter(Mandatory = $true)][string]$A, [Parameter(Mandatory = $true)][string]$B)
$root = (Resolve-Path "$PSScriptRoot\..").Path
$d = Import-Csv "$root\bench\workload.csv"
$ra = $d | Where-Object { $_.label -eq $A }
$rb = $d | Where-Object { $_.label -eq $B }
$out = foreach ($x in $ra) {
  $y = $rb | Where-Object { $_.query_name -eq $x.query_name }
  if ($y) {
    $ma = [double]$x.median_ms; $mb = [double]$y.median_ms
    [pscustomobject]@{
      query = $x.query_name
      ms_A = $ma
      ms_B = $mb
      ms_change_pct = [math]::Round(100 * ($mb - $ma) / [math]::Max(1, $ma), 1)
      rows_A = [long]$x.read_rows
      rows_B = [long]$y.read_rows
      rows_change_pct = [math]::Round(100 * ([double]$y.read_rows - [double]$x.read_rows) / [math]::Max(1, [double]$x.read_rows), 1)
    }
  }
}
$out | Format-Table -AutoSize
