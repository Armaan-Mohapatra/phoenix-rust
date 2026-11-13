#Requires -Version 5.1
# Read-only, bounded BTC source audit; no trading, production or collector access.
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$RepoRoot,
  [Parameter(Mandatory=$true)][string]$InputsDirectory,
  [Parameter(Mandatory=$true)][string]$OutputDirectory,
  [string]$CargoExecutable = "cargo"
)
$ErrorActionPreference="Stop"
Set-StrictMode -Version Latest
$market="KXBTC15M-26OCT241900-00"
$start="2026-10-24T22:44:40Z"
$end="2026-10-24T22:47:39Z"
$expected="55f869f1231b67948395ac16a33b50ec774711653da2a3d22c693050eda2db08"
function Canon([string]$path){return [System.IO.Path]::GetFullPath($path).TrimEnd('\')}
$repo=Canon $RepoRoot;$inputs=Canon $InputsDirectory;$out=Canon $OutputDirectory
if(!(Test-Path -LiteralPath $repo -PathType Container) -or !(Test-Path -LiteralPath $inputs -PathType Container)){
    throw "Verified source/repo directory missing"
}
if($out.Equals($inputs,[StringComparison]::OrdinalIgnoreCase) -or
   $out.StartsWith($inputs+"\",[StringComparison]::OrdinalIgnoreCase) -or
   $out.Equals($repo,[StringComparison]::OrdinalIgnoreCase) -or
   $out.StartsWith($repo+"\",[StringComparison]::OrdinalIgnoreCase)){
    throw "Research outputs cannot reside inside a source directory or checkout"
}
$members=Join-Path $repo "docs\BTC_20261024_ASOF_MEMBERS.txt"
$manifest=Join-Path $repo "integrations\source-loader\Cargo.toml"
if(!(Test-Path -LiteralPath $members) -or !(Test-Path -LiteralPath $manifest)){
    throw "Rust loader or allowlist missing"
}
$names=@(Get-Content -LiteralPath $members | Where-Object {$_ -and !$_.StartsWith("#")})
if($names.Count -ne 24){throw "Refusing non-24-member allowlist"}
$zips=@(1..6 | ForEach-Object {
  Join-Path $inputs ("LunaDelta_20261025T083801Z_market_data_part{0:D3}.zip" -f $_)
})
foreach($path in $zips){if(!(Test-Path -LiteralPath $path -PathType Leaf)){throw "ZIP missing: $path"}}
if(!(Get-Command $CargoExecutable -ErrorAction SilentlyContinue)){throw "Cargo unavailable; no execution attempted"}
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip=[System.IO.Compression.ZipFile]::OpenRead($zips[0])
try{
  $entry=$zip.GetEntry("archive/curated/curation_catalog.jsonl")
  if($null -eq $entry -or $entry.Length -gt 4MB){throw "Missing/oversized curation catalog"}
  $hash=[System.Security.Cryptography.SHA256]::Create()
  try{
    $stream=$entry.Open()
    try{$actual=([BitConverter]::ToString($hash.ComputeHash($stream))).Replace("-","").ToLowerInvariant()}
    finally{$stream.Dispose()}
  }finally{$hash.Dispose()}
  if($actual -ne $expected){throw "Catalog SHA-256 mismatch: $actual"}
}finally{$zip.Dispose()}
New-Item -ItemType Directory -Force -Path $out | Out-Null
$argumentsList=@("run","--release","--manifest-path",$manifest,"--bin","phoenix-source-audit","--",$market,$start,$end,$members)+$zips
$report=Join-Path $out "btc_source_replay.json"
& $CargoExecutable @argumentsList | Out-File -LiteralPath $report -Encoding UTF8
if($LASTEXITCODE -ne 0){throw "Rust replay failed. Do not promote source evidence."}
$result=Get-Content -Raw -LiteralPath $report | ConvertFrom-Json
if($result.historical.matching_events -le 0 -or $result.historical.scanned_rows -gt 250000){
    throw "Unexpected count or scan budget"
}
if($result.verified_executable_book -eq $true){throw "Unexpected executable-book promotion"}
$allowlistSha=(Get-FileHash -LiteralPath $members -Algorithm SHA256).Hash.ToLowerInvariant()
@{market=$market;catalog_sha256=$actual;allowlist_sha256=$allowlistSha;rows_scanned=$result.historical.scanned_rows;matched_events=$result.historical.matching_events;book_state=$result.book_state;source_report=$report} |
  ConvertTo-Json | Out-File -LiteralPath (Join-Path $out "btc_source_replay_summary.json") -Encoding UTF8
Write-Host "Read-only original-event audit completed: $report"
