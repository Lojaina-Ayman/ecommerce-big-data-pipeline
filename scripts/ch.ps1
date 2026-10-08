# Usage: scripts\ch.ps1 -Query "SELECT 1"      or      scripts\ch.ps1 -File clickhouse\init\001_core_tables.sql
param([string]$Query, [string]$File)

$root = (Resolve-Path "$PSScriptRoot\..").Path
$cfg = @{}
Get-Content "$root\.env" | ForEach-Object {
  if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') { $cfg[$matches[1]] = $matches[2].Trim() }
}
$common = @('--user', $cfg['CLICKHOUSE_USER'], '--password', $cfg['CLICKHOUSE_PASSWORD'],
            '--receive_timeout', '7200', '--send_timeout', '7200')
Push-Location $root
try {
  if ($File) {
    Get-Content $File -Raw | docker compose exec -T clickhouse clickhouse-client @common --multiquery --format PrettyCompact
  } else {
    docker compose exec -T clickhouse clickhouse-client @common --database $cfg['CLICKHOUSE_DB'] --format PrettyCompact --query $Query
  }
} finally { Pop-Location }
