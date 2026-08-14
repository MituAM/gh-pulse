<#
.SYNOPSIS
Downloads hours of the public GH Archive event stream into data/raw/.

.EXAMPLE
./scripts/fetch-data.ps1 2024-01-15
./scripts/fetch-data.ps1 2024-01-15 -From 15
./scripts/fetch-data.ps1 2024-01-15 -From 0 -To 23   # a whole day, about 3 GB compressed

.DESCRIPTION
The files are gzipped newline-delimited JSON and both pipelines read them as they are.
#>
param(
    [Parameter(Mandatory = $true)][string]$Date,
    [int]$From = 0,
    [int]$To = -1
)

$ErrorActionPreference = 'Stop'
if ($To -lt 0) { $To = $From }

$root = Split-Path -Parent $PSScriptRoot
$dest = Join-Path $root 'data/raw'
New-Item -ItemType Directory -Force -Path $dest | Out-Null

foreach ($hour in $From..$To) {
    $name = "$Date-$hour.json.gz"
    $target = Join-Path $dest $name

    if ((Test-Path $target) -and (Get-Item $target).Length -gt 0) {
        Write-Output "have    $name"
        continue
    }

    Write-Output "fetch   $name"
    try {
        Invoke-WebRequest -Uri "https://data.gharchive.org/$name" -OutFile $target -UseBasicParsing
    } catch {
        # Leave no truncated file behind for an hour GH Archive never published.
        if (Test-Path $target) { Remove-Item $target -Force }
        Write-Warning "missing $name (skipped)"
    }
}

Write-Output ''
Write-Output 'Downloaded into data/raw/. Run the pipelines against it with:'
Write-Output "  sbt 'spark/runMain ghpulse.spark.SparkPipeline --input data/raw'"
Write-Output "  sbt 'flink/runMain ghpulse.flink.FlinkPipeline --input data/raw'"
