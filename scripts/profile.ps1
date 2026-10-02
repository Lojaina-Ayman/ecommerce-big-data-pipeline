param([string]$Csv = "sample/events_sample_100k.csv")

$root = (Resolve-Path "$PSScriptRoot\..").Path
$sql  = (Get-Content "$root\clickhouse\queries\profile.sql" -Raw).Replace("__FILE__", $Csv)

docker run --rm `
  -v "${root}:/work" -w /work/data `
  --entrypoint clickhouse `
  clickhouse/clickhouse-server:latest `
  local --multiquery --format PrettyCompact --query "$sql"