[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Csv,
    [string]$TargetName = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

if (-not (Test-Path $Csv)) {
    Write-Error "CSV file not found: $Csv"
}

$inputDir = Join-Path $root "data\connect\input"
if (-not (Test-Path $inputDir)) {
    New-Item -ItemType Directory -Force -Path $inputDir | Out-Null
}

$fileName = if ($TargetName) { $TargetName } else { Split-Path -Leaf $Csv }
$tempFile = Join-Path $inputDir "$fileName.tmp"
$destFile = Join-Path $inputDir $fileName

Write-Host "Staging $fileName into $inputDir ..."
Copy-Item -Path $Csv -Destination $tempFile -Force
Rename-Item -Path $tempFile -NewName $fileName -Force
Write-Host "File ready for Kafka Connect SpoolDir: $destFile"
