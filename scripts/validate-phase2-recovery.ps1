#Requires -Version 5.1
<#
  Final LOCAL Phase 2 validation only, not executed by ChatGPT or Actions.
  Requires explicit operator approval and an isolated research checkout.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$RepoRoot,
    [Parameter(Mandatory=$true)][string]$OutputDirectory,
    [switch]$ConfirmIsolatedResearchOnly,
    [switch]$RunIgnoredWindowsSymlink,
    [switch]$RunMillionIdentityStress
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (-not $ConfirmIsolatedResearchOnly) {
    throw 'Explicit -ConfirmIsolatedResearchOnly required. No work attempted.'
}
function Canon([string]$path) {
    return [IO.Path]::GetFullPath($path).TrimEnd([IO.Path]::DirectorySeparatorChar)
}
$repo = Canon $RepoRoot
$out = Canon $OutputDirectory
$blocked = @('TradingNextData','KalshiData','KalshiRuntime','paper15m_runtime','agent_arena')
foreach ($path in @($repo,$out)) {
    foreach ($segment in ($path -split '[\\/]')) {
        if ($blocked -contains $segment) { throw "Protected Phoenix root: $path" }
    }
}
if ($out.Equals($repo,[StringComparison]::OrdinalIgnoreCase) -or
    $out.StartsWith($repo+[IO.Path]::DirectorySeparatorChar,
        [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Output directory must be outside the research source checkout'
}
if (!(Test-Path -LiteralPath $repo -PathType Container) -or
    !(Test-Path -LiteralPath (Join-Path $repo 'integrations\durable-archive\Cargo.toml'))) {
    throw 'RepoRoot must be a valid isolated Phoenix Rust research checkout'
}
foreach ($name in @('git','cargo','rustc')) {
    if (!(Get-Command $name -ErrorAction SilentlyContinue)) {
        throw "Missing prerequisite: $name. No tests started."
    }
}
New-Item -ItemType Directory -Force -Path $out | Out-Null
$run = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$logs = Join-Path $out "phase2_$run"
New-Item -ItemType Directory -Path $logs -ErrorAction Stop | Out-Null
$env:CARGO_BUILD_JOBS = '2'
$env:CARGO_INCREMENTAL = '0'
$env:CARGO_NET_OFFLINE = 'true'
$env:CARGO_TARGET_DIR = Join-Path $logs 'cargo-target'
$sha = (& git -C $repo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0) { throw 'Cannot identify research source commit' }
$results = New-Object 'System.Collections.Generic.List[object]'
function Run-Case([string]$label,[string[]]$argumentsList) {
    $log = Join-Path $logs ($label+'.log')
    Push-Location $repo
    try {
        & cargo @argumentsList 2>&1 | Out-File -LiteralPath $log -Encoding UTF8
        $code = $LASTEXITCODE
        $results.Add([pscustomobject]@{
            label=$label;exit_code=$code;log=$log;source_sha=$sha
        })
        if ($code -ne 0) { throw "$label failed ($code)" }
    } finally { Pop-Location }
}
try {
    foreach ($module in @('durable-archive','disk-identity','indexed-archive')) {
        $manifest = "integrations/$module/Cargo.toml"
        Run-Case "test_$module" @('test','--manifest-path',$manifest,'--all-targets')
        Run-Case "clippy_$module" @('clippy','--manifest-path',$manifest,'--all-targets')
    }
    if ($RunIgnoredWindowsSymlink) {
        Run-Case 'windows_reparse_symlink' @('test','--manifest-path',
            'integrations/durable-archive/Cargo.toml',
            'windows_reparse_directory_is_rejected','--','--ignored','--nocapture')
    }
    if ($RunMillionIdentityStress) {
        Run-Case 'million_identity_stress' @('test','--manifest-path',
            'integrations/disk-identity/Cargo.toml',
            'million_logical_ids_in_bounded_256_id_transactions',
            '--','--ignored','--nocapture')
    }
} finally {
    [pscustomobject]@{
        schema_version=1;source_commit=$sha
        completed_utc=[DateTime]::UtcNow.ToString('o')
        simulated_faults_only=$true;physical_power_loss_tested=$false
        github_actions_used=$false;results=@($results.ToArray())
    } | ConvertTo-Json -Depth 8 |
        Out-File -LiteralPath (Join-Path $logs 'phase2_results.json') -Encoding UTF8
}
Write-Host "Completed isolated research validation: $logs"
