# Thin Phoenix entry point; delegates to phoenix_storage.py and phoenix-campaign.
#   .\scripts\phoenix.ps1 doctor
#   .\scripts\phoenix.ps1 storage-audit [-Roots a,b]      # inventory + SHA-256 duplicates
#   .\scripts\phoenix.ps1 cleanup [-Apply]                # dry-run unless -Apply; workspace caches only
#   .\scripts\phoenix.ps1 research -Cases cases.jsonl [-Registry] [-- extra campaign args]
#   .\scripts\phoenix.ps1 manifest [-Root E:\TradingNextData\kalshi\raw]   # decode + verify collector segments -> catalog\tnevent-<stamp>
#   .\scripts\phoenix.ps1 cases -Manifest <catalog dir> [-Partial]          # verified books -> campaign cases.jsonl
# Research root: $env:PHOENIX_LAB, default E:\PhoenixLab. Never starts services or schedules jobs.
param(
    [Parameter(Mandatory, Position = 0)]
    [ValidateSet('doctor', 'storage-audit', 'cleanup', 'research', 'manifest', 'cases')] [string] $Command,
    [string[]] $Roots,
    [string] $Cases,
    [string] $Root = 'E:\TradingNextData\kalshi\raw',
    [string] $Manifest,
    [switch] $Partial,
    [switch] $Apply,
    [switch] $Registry,
    [Parameter(ValueFromRemainingArguments)] [string[]] $Rest
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$lab = if ($env:PHOENIX_LAB) { $env:PHOENIX_LAB } else { 'E:\PhoenixLab' }
$stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$storage = Join-Path $PSScriptRoot 'phoenix_storage.py'
$target = Join-Path $lab 'cache\build\target'
$exe = Join-Path $target 'release\phoenix-campaign.exe'
$tnexe = Join-Path $target 'release\phoenix-tnevent.exe'
# Original evidence and live runtimes: campaign outputs are refused under these.
if (-not $env:PHOENIX_PROTECTED_ROOTS) {
    $env:PHOENIX_PROTECTED_ROOTS = 'E:\KalshiData;E:\TradingNextData;C:\PhoenixResearchLab;E:\PhoenixResearchLab;E:\PhoenixTooling;E:\SwiftAutoTrader'
}

switch ($Command) {
    'doctor' {
        foreach ($d in 'C:\', $lab) {
            $drive = (Get-Item $d).PSDrive
            '{0} free {1:N1} GB' -f $drive.Name, ($drive.Free / 1GB)
        }
        foreach ($t in 'cargo', 'python') {
            $c = Get-Command $t -ErrorAction SilentlyContinue
            '{0}: {1}' -f $t, $(if ($c) { $c.Source } else { 'MISSING' })
        }
        'campaign binary: {0}' -f $(if (Test-Path $exe) { $exe } else { "not built (cargo build --release -p phoenix-campaign, CARGO_TARGET_DIR=$target)" })
        foreach ($l in Get-ChildItem -Recurse -Filter registry.lock (Join-Path $lab 'registry') -ErrorAction SilentlyContinue) { "STALE? registry lock: $($l.FullName)" }
        python $storage selftest
    }
    'storage-audit' {
        $out = Join-Path $lab "maintenance\storage-audits\$stamp"
        if (-not $Roots) { $Roots = (Get-Content (Join-Path $lab 'config\audit-roots.txt') | Where-Object { $_ -and -not $_.StartsWith('#') }) }
        python $storage inventory @Roots --out $out
        python $storage dupes --out $out
    }
    'cleanup' {
        if ($Apply) { python $storage cleanup --workspace $lab --apply } else { python $storage cleanup --workspace $lab }
    }
    'manifest' {
        if (-not (Test-Path $tnexe)) { $env:CARGO_TARGET_DIR = $target; cargo build --release -p phoenix-tnevent --manifest-path (Join-Path $repo 'Cargo.toml') }
        & $tnexe manifest --root $Root --out (Join-Path $lab "catalog\tnevent-$stamp") @Rest
    }
    'cases' {
        if (-not $Manifest) { throw '-Manifest <catalog dir containing markets.jsonl> is required' }
        $a = @('cases', '--root', $Root, '--manifest', (Join-Path $Manifest 'markets.jsonl'), '--out', (Join-Path $lab "derived\cases-$stamp"))
        if ($Partial) { $a += '--partial' }
        & $tnexe @a @Rest
    }
    'research' {
        if (-not $Cases) { throw "-Cases cases.jsonl is required" }
        if (-not (Test-Path $exe)) {
            $env:CARGO_TARGET_DIR = $target
            cargo build --release -p phoenix-campaign --manifest-path (Join-Path $repo 'Cargo.toml')
        }
        $a = @("--input", $Cases, '--cache-dir', (Join-Path $lab 'cache\replay-cache'), '--out', (Join-Path $lab "research\runs\$stamp"))
        if ($Registry) { $a += @('--registry', (Join-Path $lab 'registry\holdout')) }
        & $exe @a @Rest
    }
}
