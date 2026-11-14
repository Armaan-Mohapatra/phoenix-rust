#Requires -Version 5.1
<#
.SYNOPSIS
Phase 15 OPERATOR-ONLY LOCAL validation plan. SOURCE PREPARED, NOT EXECUTED.
.DESCRIPTION
Clones an exact Git commit into an isolated nonproduction research directory.
Runs only after -ExecuteValidation is explicitly set. Default excludes all
large/real-data replay, stress, optimizer, collector, network and live trading.
No Codex or GitHub Actions required. Never execute this during Phase 14.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$RepoRoot,
    [Parameter(Mandatory=$true)][string]$OutputDirectory,
    [Parameter(Mandatory=$true)][ValidatePattern('^[0-9a-f]{40}$')][string]$ExpectedSha,
    [string]$ExpectedBranch='codex/phase14-integration-freeze-v1',
    [switch]$ExecuteValidation,
    [switch]$AllowDependencyDownloads,
    [switch]$IncludeOptionalHft,
    [switch]$IncludeStress,
    [switch]$IncludeOriginalSourceAudit,
    [switch]$IncludeMultiMarketReplay,
    [string]$MultiMarketPlan,
    [string]$MultiMarketPriorReport='-',
    [string[]]$MultiMarketSourceZips=@(),
    [switch]$IncludeHypothesisResearch,
    [switch]$IncludePerformance,
    [switch]$ContinueAfterFailure,
    [string]$HistoricalZipDirectory,
    [string]$CoreToolchain='1.78.0',
    [string]$HftToolchain='1.91.1',
    [ValidateRange(1,4)][int]$CargoJobs=2,
    [ValidateRange(1,16)][int]$MaxTargetGiB=8
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
if(-not $ExecuteValidation){
    throw 'OPERATOR_AUTHORIZATION_REQUIRED: Script intentionally does nothing without -ExecuteValidation.'
}
function FullPath([string]$p){
    return [System.IO.Path]::GetFullPath($p).TrimEnd([char[]]@('\','/'))
}
function IsWithin([string]$path,[string]$ancestor){
    return $path.Equals($ancestor,[StringComparison]::OrdinalIgnoreCase) -or
        $path.StartsWith($ancestor+[IO.Path]::DirectorySeparatorChar,
            [StringComparison]::OrdinalIgnoreCase)
}
$protected=@('TradingNextData','KalshiData','KalshiRuntime',
    'paper15m_runtime','collector','active','production')
function Assert-SafeDestination([string]$p){
    $segments=@($p -split '[\\/]' | Where-Object {$_})
    foreach($part in $segments){
        if($protected -contains $part){
            throw "REFUSING_PRODUCTION_OR_COLLECTOR_DESTINATION: $p"
        }
    }
    # Reject Windows reparse, junction and symlink ancestors before writing.
    $walk=FullPath $p
    while($walk -and (Test-Path -LiteralPath $walk)){
        $info=Get-Item -LiteralPath $walk -Force
        if(($info.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0){
            throw "REFUSING_REPARSE_ANCESTOR: $walk"
        }
        $parent=[IO.Path]::GetDirectoryName($walk)
        if(-not $parent -or $parent -eq $walk){break}
        $walk=$parent
    }
}
$repo=FullPath $RepoRoot
$out=FullPath $OutputDirectory
Assert-SafeDestination $repo
Assert-SafeDestination $out
if((IsWithin $out $repo) -or (IsWithin $repo $out)){
    throw 'RESEARCH_CHECKOUT_AND_OUTPUT_MUST_BE_DISJOINT'
}
if(-not (Test-Path -LiteralPath (Join-Path $repo 'Cargo.toml') -PathType Leaf)){
    throw 'GIT_SOURCE_CHECKOUT_NOT_VERIFIED'
}
$source=$null
if($HistoricalZipDirectory){
    $source=FullPath $HistoricalZipDirectory
    if((IsWithin $out $source) -or (IsWithin $repo $source)){
        throw 'RESEARCH_WRITES_MUST_NOT_OVERLAP_HISTORICAL_INPUT'
    }
}
if($IncludeOriginalSourceAudit -and -not $source){
    throw 'HISTORICAL_INPUT_DIRECTORY_REQUIRED'
}
if($IncludeMultiMarketReplay){
    if(-not $MultiMarketPlan -or
        $MultiMarketSourceZips.Count -lt 1 -or
        $MultiMarketSourceZips.Count -gt 12){
        throw 'BOUNDED_MULTIMARKET_PLAN_AND_ZIPS_REQUIRED'
    }
    foreach($inputPath in @($MultiMarketPlan)+$MultiMarketSourceZips){
        if(-not (Test-Path -LiteralPath $inputPath -PathType Leaf)){
            throw "MULTIMARKET_INPUT_MISSING: $inputPath"
        }
        if((IsWithin (FullPath $inputPath) $out)){
            throw 'MULTIMARKET_SOURCE_INPUTS_MUST_BE_READ_ONLY'
        }
    }
    if($MultiMarketPriorReport -ne '-' -and
        -not (Test-Path -LiteralPath $MultiMarketPriorReport -PathType Leaf)){
        throw 'MULTIMARKET_PRIOR_REPORT_MISSING'
    }
}
foreach($exe in @('git','rustup','cargo','rustc')){
    if(-not (Get-Command $exe -ErrorAction SilentlyContinue)){
        throw "MISSING_TOOL_$exe"
    }
}
# Check original source first; clone only the exact expected commit.
$observed=(& git -C $repo rev-parse HEAD).Trim()
if($LASTEXITCODE -ne 0 -or $observed -ne $ExpectedSha){
    throw "GIT_HEAD_MISMATCH expected $ExpectedSha got $observed"
}
$observedBranch=(& git -C $repo branch --show-current).Trim()
if($LASTEXITCODE -ne 0 -or
    ($observedBranch -and $observedBranch -ne $ExpectedBranch)){
    throw "GIT_BRANCH_MISMATCH expected $ExpectedBranch got $observedBranch"
}
$origin=(& git -C $repo remote get-url origin 2>$null | Out-String).Trim()
# No remote network use is necessary: input is already a local git checkout.
New-Item -ItemType Directory -Force -Path $out | Out-Null
$runId=[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffffffZ')
$logs=Join-Path $out ("phoenix_local_validation_"+$runId)
New-Item -ItemType Directory -Path $logs -ErrorAction Stop | Out-Null
$checkout=Join-Path $logs 'isolated-checkout'
$target=Join-Path $logs 'target'
$script:records=New-Object 'System.Collections.Generic.List[object]'
$script:resourceSnapshots=New-Object 'System.Collections.Generic.List[object]'
$script:sequence=0
$script:failed=$false
$script:continueFailures=[bool]$ContinueAfterFailure
$script:offlineArgs=if($AllowDependencyDownloads){@()}else{@('--offline')}
$env:RUSTUP_AUTO_INSTALL='0'
$env:CARGO_BUILD_JOBS=[string]$CargoJobs
$env:CARGO_INCREMENTAL='0'
$env:CARGO_NET_OFFLINE=if($AllowDependencyDownloads){'false'}else{'true'}
$env:CARGO_TARGET_DIR=$target
$env:RUST_BACKTRACE='1'
function Add-Record([string]$Task,[string]$Status,[string]$Reason,
    [int]$ExitCode,[string]$Log,[double]$Seconds){
    $hash=$null
    if($Log -and (Test-Path -LiteralPath $Log -PathType Leaf)){
        $hash=(Get-FileHash -LiteralPath $Log -Algorithm SHA256).Hash.ToLowerInvariant()
    }
    $script:records.Add([pscustomobject]@{
        task=$Task;status=$Status;reason=$Reason;exit_code=$ExitCode;
        log=$Log;log_sha256=$hash;elapsed_seconds=$Seconds
    })
}
function Block([string]$Task,[string]$Reason){
    Add-Record $Task 'BLOCKED' $Reason 0 '' 0
}
function Check-TargetBudget(){
    if(-not (Test-Path -LiteralPath $target)){return}
    $items=Get-ChildItem -LiteralPath $target -File -Recurse -ErrorAction Stop
    $sum=($items | Measure-Object -Property Length -Sum).Sum
    if($null -eq $sum){$sum=0}
    if($sum -ge ([int64]$MaxTargetGiB*1GB)){
        throw 'LOCAL_BUILD_TARGET_BUDGET_EXCEEDED'
    }
}
function Sample-Resources([string]$Task,[string]$Moment){
    # Low-rate system snapshots, NOT per-process peak usage. Optional only.
    if(-not $IncludePerformance){return}
    try{
        $os=Get-CimInstance -ClassName Win32_OperatingSystem -ErrorAction Stop
        $cpu=@(Get-CimInstance -ClassName Win32_Processor -ErrorAction Stop)
        $driveName=[IO.Path]::GetPathRoot($logs).Substring(0,1)
        $disk=Get-PSDrive -Name $driveName -ErrorAction Stop
        $load=($cpu | Measure-Object -Property LoadPercentage -Average).Average
        $script:resourceSnapshots.Add([pscustomobject]@{
            task=$Task;sample=$Moment;observed_utc=[DateTime]::UtcNow.ToString('o');
            system_cpu_load_percent=$load;
            available_ram_mib=([double]$os.FreePhysicalMemory/1024.0);
            validation_volume_free_bytes=$disk.Free;
            note='SYSTEM_SNAPSHOT_NOT_PEAK_OR_PER_PROCESS'
        })
    }catch{
        $script:resourceSnapshots.Add([pscustomobject]@{
            task=$Task;sample=$Moment;error='RESOURCE_SAMPLE_UNAVAILABLE';
            note='NO_RESOURCE_CLAIM'
        })
    }
}
function Logged([string]$Task,[string]$Exe,[string[]]$CommandArgs,[string]$Cwd){
    Check-TargetBudget
    $script:sequence++
    $log=Join-Path $logs ("{0:D3}_{1}.log" -f $script:sequence,
        ($Task -replace '[^a-zA-Z0-9_-]','_'))
    $timer=[Diagnostics.Stopwatch]::StartNew()
    $exitCode=999
    Sample-Resources $Task 'before'
    Push-Location -LiteralPath $Cwd
    try{
        & $Exe @CommandArgs *> $log
        $exitCode=if($null -ne $LASTEXITCODE){[int]$LASTEXITCODE}else{0}
    }catch{
        $_ | Out-String | Add-Content -LiteralPath $log
    }finally{
        Pop-Location
        $timer.Stop()
    }
    Sample-Resources $Task 'after'
    Add-Record $Task $(if($exitCode -eq 0){'PASS'}else{'FAIL'}) '' $exitCode $log $timer.Elapsed.TotalSeconds
    if($exitCode -ne 0){
        $script:failed=$true
        if(-not $script:continueFailures){
            throw "LOCAL_VALIDATION_FAILURE: $Task. See $log"
        }
    }
}
function Cargo([string]$Task,[string]$Toolchain,[string[]]$CommandArgs){
    Logged $Task 'cargo' (@("+$Toolchain")+$CommandArgs) $checkout
}
$reportFile=Join-Path $logs 'results.json'
$preflightFile=Join-Path $logs 'preflight.json'
try{
    [pscustomobject]@{
        original_repo=$repo;original_sha=$observed;
        branch=$observedBranch;origin=$origin;
        original_historical_input=$source;
        requested_at_utc=[DateTime]::UtcNow.ToString('o');
        core_toolchain=$CoreToolchain;hft_toolchain=$HftToolchain;
        dependency_downloads=[bool]$AllowDependencyDownloads;
        historical_replay=[bool]$IncludeOriginalSourceAudit;
        multimarket_replay=[bool]$IncludeMultiMarketReplay;
        stress=[bool]$IncludeStress;
        hypothesis_research=[bool]$IncludeHypothesisResearch;
        target_budget_gib=$MaxTargetGiB;build_jobs=$CargoJobs;
        safety='LOCAL_ISOLATED_PAPER_RESEARCH_ONLY'
    } | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $preflightFile -Encoding UTF8
    Logged 'clone-isolated-commit' 'git' @('clone','--no-hardlinks',
        '--no-checkout','--local',$repo,$checkout) $logs
    Logged 'checkout-exact-commit' 'git' @('-C',$checkout,
        'checkout','--detach',$ExpectedSha) $logs
    $isolatedSha=(& git -C $checkout rev-parse HEAD).Trim()
    if($isolatedSha -ne $ExpectedSha){throw 'ISOLATED_COMMIT_MISMATCH'}
    # Toolchains are never downloaded automatically. Their availability
    # and hashes must be recorded before any compiling.
    Logged 'toolchain-inventory' 'rustup' @('toolchain','list') $checkout
    Logged 'core-rustc-version' 'rustup' @('run',$CoreToolchain,
        'rustc','--version','--verbose') $checkout
    $root=(Join-Path $checkout 'Cargo.toml')
    Cargo 'root-metadata' $CoreToolchain (@('metadata',
        '--no-deps','--format-version','1')+$script:offlineArgs)
    Cargo 'root-format' $CoreToolchain @('fmt','--all','--','--check')
    Cargo 'root-clippy' $CoreToolchain (@('clippy','--workspace',
        '--all-targets')+$script:offlineArgs+@('--','-D','warnings'))
    Cargo 'root-test' $CoreToolchain (@('test','--workspace',
        '--all-targets')+$script:offlineArgs)
    # Every direct-child standalone manifest is independently checked.
    # No recursive drive/research archive scan is performed.
    $integrations=Get-ChildItem -LiteralPath (Join-Path $checkout 'integrations') -Directory |
        Where-Object {Test-Path -LiteralPath (Join-Path $_.FullName 'Cargo.toml')} |
        Sort-Object Name
    foreach($integration in $integrations){
        $manifest=Join-Path $integration.FullName 'Cargo.toml'
        if($integration.Name -eq 'hftbacktest-adapter'){
            if(-not $IncludeOptionalHft){
                Block 'hftbacktest-opt-in' 'EXPLICIT_OPT_IN_AND_RUST_1_91_1_REQUIRED'
                continue
            }
            $tc=$HftToolchain
            Logged 'hft-rustc-version' 'rustup' @('run',$tc,'rustc','--version') $checkout
        }else{$tc=$CoreToolchain}
        Cargo ("metadata-"+$integration.Name) $tc (@('metadata','--no-deps',
            '--format-version','1','--manifest-path',$manifest)+$script:offlineArgs)
        Cargo ("fmt-"+$integration.Name) $tc @('fmt','--manifest-path',$manifest,
            '--','--check')
        $args=@('clippy','--manifest-path',$manifest,'--all-targets')+$script:offlineArgs
        if($integration.Name -eq 'hftbacktest-adapter'){$args+=@('--features','upstream-hft')}
        Cargo ("clippy-"+$integration.Name) $tc ($args+@('--','-D','warnings'))
        $tests=@('test','--manifest-path',$manifest,'--all-targets')+$script:offlineArgs
        if($integration.Name -eq 'hftbacktest-adapter'){$tests+=@('--features','upstream-hft')}
        Cargo ("test-"+$integration.Name) $tc $tests
    }
    if($IncludeStress){
        Cargo 'disk-index-million-ids' $CoreToolchain (@('test',
            '--manifest-path',(Join-Path $checkout 'integrations/disk-identity/Cargo.toml'),
            'million_logical_ids_in_bounded_256_id_transactions',
            '--','--nocapture')+$script:offlineArgs)
        Block 'windows-powercut' 'REQUIRES_SEPARATE_ISOLATED_HARDWARE_FAULT_CAMPAIGN'
    }else{
        Block 'million-id-index-stress' 'REQUIRES_IncludeStress'
        Block 'power-loss-faults' 'REQUIRES_SEPARATE_OPERATOR_FAULT_ENVIRONMENT'
    }
    if($IncludeOriginalSourceAudit){
        $env:RUSTUP_TOOLCHAIN=$CoreToolchain
        $launcher=Join-Path $checkout 'integrations/source-loader/scripts/run-original-btc.ps1'
        if(Test-Path -LiteralPath $launcher){
            $historical=Join-Path $logs 'original-btc'
            New-Item -ItemType Directory -Path $historical | Out-Null
            Logged 'original-btc-readonly-hash-replay' 'powershell.exe' @(
                '-NoProfile','-NonInteractive','-File',$launcher,
                '-RepoRoot',$checkout,
                '-InputsDirectory',$source,
                '-OutputDirectory',$historical) $checkout
        }else{Block 'original-btc' 'VERIFIED_SOURCE_LAUNCHER_MISSING'}
    }else{Block 'original-btc' 'REQUIRES_IncludeOriginalSourceAudit'}
    # Multi-market replay requires an externally verified market list and
    # explicit bounded ZIP files. It is never automatically triggered.
    if($IncludeMultiMarketReplay){
        $multi=Join-Path $checkout 'integrations/source-loader/Cargo.toml'
        Cargo 'multimarket-historical' $CoreToolchain (
            @('run','--manifest-path',$multi,'--bin',
              'phoenix-multimarket-replay')+
            $script:offlineArgs+
            @('--',$MultiMarketPlan,$MultiMarketPriorReport)+
            $MultiMarketSourceZips)
    }else{Block 'multimarket-historical' 'REQUIRES_IncludeMultiMarketReplay_AND_VERIFIED_PLAN'}
    # Additional tasks require authentic proof beyond a runnable CLI.

    Block 'original-perps-schema' 'ACTUAL_ORIGINAL_VENUE_SCHEMA_UNAVAILABLE'
    Block 'source-book-continuity' 'COMPLETE_SNAPSHOT_SEQUENCE_MANIFEST_REQUIRED'
    Block 'effective-dated-fees' 'AUTHENTIC_HISTORICAL_FEE_SCHEDULE_REQUIRED'
    Block 'independent-settlement' 'OFFICIAL_AND_INDEPENDENT_PRIMARY_PROOF_REQUIRED'
    if($IncludeHypothesisResearch){
        Block 'untouched-oos-confirmation' 'PRE-REGISTERED_FROZEN_OOS_PLAN_REQUIRED'
        Block 'strategy-optimization' 'CANDIDATE_PLAN_AND_APPROVED_RESOURCE_BUDGET_REQUIRED'
    }else{
        Block 'strategy-optimization' 'REQUIRES_IncludeHypothesisResearch'
        Block 'untouched-oos-confirmation' 'REQUIRES_IncludeHypothesisResearch'
    }
    if($IncludePerformance){
        # Timings for executed validations are in results.json. CPU/RAM and
        # throughput must be separately measured, not inferred from elapsed.
        Block 'cpu-ram-throughput' 'SYSTEM_CPU_RAM_DISK_SNAPSHOTS_RECORDED_NOT_PEAK_OR_MARKET_THROUGHPUT'
    }else{Block 'cpu-ram-throughput' 'REQUIRES_IncludePerformance'}
}finally{
    $script:records | ConvertTo-Json -Depth 6 |
        Set-Content -LiteralPath $reportFile -Encoding UTF8
    # Lockfile digests are copied to validation evidence. Isolated Cargo may
    # generate them; the source/production checkout is never modified.
    $lockRecords=@()
    if(Test-Path -LiteralPath $checkout -PathType Container){
        $lockRecords=@(Get-ChildItem -LiteralPath $checkout -Filter 'Cargo.lock' -File -Recurse |
          ForEach-Object {
            [pscustomobject]@{
                path=$_.FullName.Substring($checkout.Length+1);
                sha256=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
            }
          })
    }
    $lockRecords | ConvertTo-Json -Depth 4 |
        Set-Content -LiteralPath (Join-Path $logs 'resolved_lockfile_sha256.json') -Encoding UTF8
    $script:resourceSnapshots | ConvertTo-Json -Depth 5 |
        Set-Content -LiteralPath (Join-Path $logs 'sampled_resources.json') -Encoding UTF8
    Remove-Item Env:RUSTUP_TOOLCHAIN -ErrorAction SilentlyContinue
    [pscustomobject]@{
        exact_sha=$ExpectedSha;
        completed_at_utc=[DateTime]::UtcNow.ToString('o');
        total=$script:records.Count;
        passed=@($script:records|Where-Object {$_.status -eq 'PASS'}).Count;
        failed=@($script:records|Where-Object {$_.status -eq 'FAIL'}).Count;
        blocked=@($script:records|Where-Object {$_.status -eq 'BLOCKED'}).Count;
        logs_root=$logs;
        release_ready=$false
    } | ConvertTo-Json -Depth 5 |
        Set-Content -LiteralPath (Join-Path $logs 'summary.json') -Encoding UTF8
}
if($script:failed){throw "VALIDATION_FAILURE: Review $reportFile"}
Write-Host "LOCAL VALIDATION COMPLETED: $reportFile"
Write-Host 'Passing executed tasks do NOT independently authorize collector installation, verified settlements, or live orders.'
