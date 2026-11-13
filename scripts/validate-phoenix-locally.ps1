#Requires -Version 5.1
<#
.SYNOPSIS
Final, explicitly invoked LOCAL Phoenix Rust validation. Not an installer or collector.
.DESCRIPTION
Runs the complete Rust test inventory inside an isolated research checkout.
No GitHub-hosted runners and no Codex agent required.
The operator must invoke this only after the build-first development freeze.
No production source, live exchange, or collector APIs are accessed.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$RepoRoot,
    [Parameter(Mandatory=$true)][string]$OutputDirectory,
    [string]$HistoricalZipDirectory,
    [switch]$AllowDependencyDownloads,
    [switch]$IncludeHistoricalSourceAudit,
    [switch]$ContinueAfterFailure
)
$ErrorActionPreference='Stop'
Set-StrictMode -Version Latest
function Canon([string]$Value){
    return [System.IO.Path]::GetFullPath($Value).TrimEnd([System.IO.Path]::DirectorySeparatorChar)
}
$repo=Canon $RepoRoot
$out=Canon $OutputDirectory
if(!(Test-Path -LiteralPath $repo -PathType Container) -or
   !(Test-Path -LiteralPath (Join-Path $repo 'Cargo.toml') -PathType Leaf)){
    throw 'RepoRoot is not a verified Rust research checkout'
}
if($out.Equals($repo,[System.StringComparison]::OrdinalIgnoreCase) -or
   $out.StartsWith($repo+[System.IO.Path]::DirectorySeparatorChar,[System.StringComparison]::OrdinalIgnoreCase)){
    throw 'OutputDirectory must be outside the repository checkout'
}
$source=$null
if($HistoricalZipDirectory){
    $source=Canon $HistoricalZipDirectory
    if($out.Equals($source,[System.StringComparison]::OrdinalIgnoreCase) -or
       $out.StartsWith($source+[System.IO.Path]::DirectorySeparatorChar,[System.StringComparison]::OrdinalIgnoreCase)){
        throw 'OutputDirectory must be outside source inputs'
    }
}
if($IncludeHistoricalSourceAudit -and !$source){throw 'HistoricalZipDirectory required for historical audit'}
foreach($name in @('cargo','rustc','git')){
    if(!(Get-Command $name -ErrorAction SilentlyContinue)){
        throw "$name unavailable; install/check prerequisites separately before final validation"
    }
}
New-Item -ItemType Directory -Force -Path $out | Out-Null
$runId=[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$logs=Join-Path $out "validation_$runId"
New-Item -ItemType Directory -Path $logs -ErrorAction Stop | Out-Null
$env:CARGO_BUILD_JOBS='2'
$env:CARGO_INCREMENTAL='0'
$env:CARGO_TARGET_DIR=Join-Path $logs 'cargo-target'
$env:CARGO_NET_OFFLINE=if($AllowDependencyDownloads){'false'}else{'true'}
$script:results=New-Object 'System.Collections.Generic.List[object]'
$script:counter=0
function Invoke-Logged([string]$Label,[string]$Command,[string[]]$Arguments,[string]$WorkingDirectory){
    $script:counter++
    $log=Join-Path $logs ("{0:D2}_{1}.log" -f $script:counter,($Label -replace '[^A-Za-z0-9_-]','_'))
    $started=[DateTime]::UtcNow
    $exit=999
    Push-Location -LiteralPath $WorkingDirectory
    try{
        & $Command @Arguments *> $log
        $exit=$LASTEXITCODE
        if($null -eq $exit){$exit=0}
    }catch{
        $_ | Out-String | Add-Content -LiteralPath $log
        $exit=999
    }finally{Pop-Location}
    $script:results.Add([pscustomobject]@{
        task=$Label;exit_code=$exit;started_utc=$started.ToString('o');
        finished_utc=[DateTime]::UtcNow.ToString('o');log=$log
    })
    if($exit -ne 0 -and !$ContinueAfterFailure){
        throw "Validation stopped on $Label (exit $exit). See $log"
    }
}
function Cargo([string]$Label,[string[]]$Arguments){
    Invoke-Logged $Label 'cargo' $Arguments $repo
}
$head=(& git -C $repo rev-parse HEAD).Trim()
if($LASTEXITCODE -ne 0){throw 'Could not read checkout SHA'}
$repoUrl=(& git -C $repo remote get-url origin 2>$null)
$preflight=[pscustomobject]@{
    head_sha=$head;repo_root=$repo;origin=$repoUrl;
    cargo=(& cargo --version);rustc=(& rustc --version);
    source_audit_requested=[bool]$IncludeHistoricalSourceAudit;
    network_dependency_fetch_allowed=[bool]$AllowDependencyDownloads;
    target_directory=$env:CARGO_TARGET_DIR;
    test_policy='LOCAL_FINAL_VALIDATION_ONLY'
}
$preflight | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $logs 'preflight.json') -Encoding UTF8
$offline=@()
if(!$AllowDependencyDownloads){$offline=@('--offline')}
try{
    Cargo 'metadata-root' (@('metadata','--no-deps','--format-version','1')+$offline)
    Cargo 'root-fmt' @('fmt','--all','--','--check')
    Cargo 'root-clippy' (@('clippy','--workspace','--all-targets')+$offline)
    Cargo 'root-all-target-tests' (@('test','--workspace','--all-targets')+$offline)
    $integrations=Get-ChildItem -LiteralPath (Join-Path $repo 'integrations') -Directory |
        Where-Object {Test-Path -LiteralPath (Join-Path $_.FullName 'Cargo.toml')} |
        Sort-Object Name
    foreach($item in $integrations){
        $manifest=Join-Path $item.FullName 'Cargo.toml'
        Cargo "metadata-$($item.Name)" (@('metadata','--no-deps','--format-version','1','--manifest-path',$manifest)+$offline)
        $testArgs=@('test','--manifest-path',$manifest,'--all-targets')+$offline
        if($item.Name -eq 'hftbacktest-adapter'){$testArgs+=@('--features','upstream-hft')}
        Cargo "test-$($item.Name)" $testArgs
    }
    if($IncludeHistoricalSourceAudit){
        $launcher=Join-Path $repo 'integrations/source-loader/scripts/run-original-btc.ps1'
        if(!(Test-Path -LiteralPath $launcher -PathType Leaf)){throw 'Original BTC launcher not found'}
        $historical=Join-Path $logs 'historical-source'
        New-Item -ItemType Directory -Force -Path $historical | Out-Null
        $shell=(Get-Process -Id $PID).Path
        Invoke-Logged 'historical-source-btc' $shell @(
            '-NoProfile','-NonInteractive','-File',$launcher,
            '-RepoRoot',$repo,'-InputsDirectory',$source,
            '-OutputDirectory',$historical
        ) $repo
    }
}finally{
    $script:results | ConvertTo-Json -Depth 4 |
        Set-Content -LiteralPath (Join-Path $logs 'results.json') -Encoding UTF8
}
Write-Host "Final local validation logs: $logs"
if(@($script:results | Where-Object {$_.exit_code -ne 0}).Count -gt 0){
    throw 'At least one validation task failed. See results.json. Do not declare release ready.'
}
Write-Host 'All EXECUTED local validation tasks passed. Independent settlement and production-release evidence remain separate gates.'
