param([string]$Query, [string]$File)

$root = (Resolve-Path "$PSScriptRoot\..").Path
$cfg = @{}
Get-Content "$root\.env" | ForEach-Object {
    if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') {
        $cfg[$matches[1]] = $matches[2].Trim()
    }
}

$user = $cfg['CLICKHOUSE_USER']
$pass = $cfg['CLICKHOUSE_PASSWORD']
$db   = $cfg['CLICKHOUSE_DB']

Push-Location $root
try {
    if ($File) {
        Get-Content $File -Raw | docker compose exec -T clickhouse clickhouse-client `
            --user $user --password $pass --database "$db" --multiquery --format PrettyCompact
    } else {
        docker compose exec -T clickhouse clickhouse-client `
            --user $user --password $pass --database "$db" --format PrettyCompact --query $Query
    }
} finally {
    Pop-Location
}