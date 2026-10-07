# Creates the read-only ClickHouse user Superset connects with (values come from .env)
$root = (Resolve-Path "$PSScriptRoot\..").Path
$cfg = @{}
Get-Content "$root\.env" | ForEach-Object {
  if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') { $cfg[$matches[1]] = $matches[2].Trim() }
}
foreach ($k in 'SUPERSET_CH_USER', 'SUPERSET_CH_PASSWORD', 'CLICKHOUSE_DB') {
  if (-not $cfg[$k]) { throw "Missing $k in .env" }
}
$sql = @"
CREATE USER IF NOT EXISTS $($cfg['SUPERSET_CH_USER']) IDENTIFIED BY '$($cfg['SUPERSET_CH_PASSWORD'])' SETTINGS max_execution_time = 60, max_memory_usage = 2000000000;
GRANT SELECT ON $($cfg['CLICKHOUSE_DB']).* TO $($cfg['SUPERSET_CH_USER']);
"@
$tmp = Join-Path $env:TEMP "create_superset_user.sql"
Set-Content -Path $tmp -Value $sql
& "$PSScriptRoot\ch.ps1" -File $tmp
Remove-Item $tmp