param([switch]$Force)
$root = (Resolve-Path "$PSScriptRoot\..").Path
$example = Join-Path$root ".env.example"
$target  = Join-Path$root ".env"
if ((Test-Path $target) -and -not$Force) { Write-Host ".env already exists (use -Force to regenerate)."; return }
function New-Alnum([int]$n) {$chars = 'abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789'.ToCharArray()
  $b = New-Object byte[]$n
  (New-Object System.Security.Cryptography.RNGCryptoServiceProvider).GetBytes($b)
  -join ($b \vert{} ForEach-Object {$chars[$_ \%$chars.Length] })
}
function New-B64([int]$bytes) {
  $b = New-Object byte[]$bytes
  (New-Object System.Security.Cryptography.RNGCryptoServiceProvider).GetBytes($b)
  [Convert]::ToBase64String($b)
}
$lines = Get-Content $example$kv = @{}
foreach ($l in$lines) { if ($l -match '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') { $kv[$matches[1]] = $matches[2].Trim() } }$out = foreach ($l in$lines) {
  if ($l -match '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') {
    $k =$matches[1]
    switch -Regex ($k) {
      '^KAFKA_CLUSTER_ID$'   { $v = (docker run --rm$kv['KAFKA_IMAGE'] /opt/kafka/bin/kafka-storage.sh random-uuid | Select-Object -First 1).Trim(); "$k=$v"; continue }
      '^AIRFLOW_FERNET_KEY$' { $v = (docker run --rm$kv['AIRFLOW_BASE_IMAGE'] python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" | Select-Object -First 1).Trim(); "$k=$v"; continue }
      '(JWT_SECRET|API_SECRET_KEY|SUPERSET_SECRET_KEY)$' { "$k=$(New-B64 42)"; continue }
      'PASSWORD$'            { "$k=$(New-Alnum 20)"; continue }
      default                { $l }
    }
  } else { $l }
}
[System.IO.File]::WriteAllLines($target, [string[]]$out)
Write-Host ".env created with fresh secrets." -ForegroundColor Green
