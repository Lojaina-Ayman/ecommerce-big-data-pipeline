$issues = 0
if (git ls-files .env) { Write-Host "[!] .env tracked" -ForegroundColor Yellow; $issues++ }
git ls-files | ForEach-Object { $p=$_; if (Test-Path $p) { if ((Get-Item $p).Length -gt 5242880) { Write-Host "[!] >5MB: $p" -ForegroundColor Yellow; $issues++ } } }
git ls-files "*.crv" "*.jar" "*.parquet" | Where-Object { $_ -notmatch "^(tests/fixtures|bench)/" } | ForEach-Object { Write-Host "[!] Tracked binary/dataset: $_" -ForegroundColor Yellow; $issues++ }
git ls-files "*.sh" | ForEach-Object { if ((Get-Content $_ -Raw) -match "`r`n") { Write-Host "[!] CRLF in: $_" -ForegroundColor Yellow; $issues++ } }
if ($issues -eq 0) { Write-Host "All checks passed." -ForegroundColor Green } else { Write-Host "$($issues) issue(s) found." -ForegroundColor Red }