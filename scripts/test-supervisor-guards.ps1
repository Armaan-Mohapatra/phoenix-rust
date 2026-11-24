# Exercises phoenix-phase21.ps1 guard functions with mocked status objects and processes.
# Extracts the real functions (no copy to drift); starts no worker, touches no runtime home,
# registers no task. Every file it writes lives in a throw-away directory under $env:TEMP.
$ErrorActionPreference = 'Stop'
$source = Join-Path $PSScriptRoot 'phoenix-phase21.ps1'
$ast = [System.Management.Automation.Language.Parser]::ParseFile($source, [ref]$null, [ref]$null)
$wanted = 'Write-Log', 'Rotate', 'Get-AgeSeconds', 'Resolve-LedgerPath', 'Get-LedgerPath', 'Get-Reserve', 'Get-ReserveReasons', 'Request-DiskPause',
    'Get-OwnedWorker', 'Test-OwnedPid', 'Test-StatusOwnership', 'Get-HealthReasons', 'Assert-PaperOnly',
    'Get-CpuSeconds', 'Test-Idle', 'Invoke-FrozenWorkerRecovery'
$found = @($ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -in $wanted }, $true))
foreach ($fn in $found) { Invoke-Expression $fn.Extent.Text }
$failures = 0
function Check($name, $ok) { if ($ok) { "PASS  $name" } else { "FAIL  $name"; $script:failures++ } }
Check "all $($wanted.Count) guard functions exist in the supervisor" ($found.Count -eq $wanted.Count)

$RuntimeHome = [IO.Path]::GetFullPath((Join-Path $env:TEMP ("supervisor-guard-test-" + [guid]::NewGuid().ToString('N').Substring(0, 8))))
$scripts = Join-Path $RuntimeHome 'fake-scripts'
New-Item -ItemType Directory -Force -Path (Join-Path $RuntimeHome 'runtime'), (Join-Path $RuntimeHome 'logs'), $scripts | Out-Null
$supLog = Join-Path $RuntimeHome 'logs\phoenix-supervisor.log'
$pidFile = Join-Path $RuntimeHome 'runtime\worker.pid.json'
$ProjectRoot = Join-Path $RuntimeHome 'collector-checkout'; $Port = 1
New-Item -ItemType Directory -Force -Path $ProjectRoot | Out-Null
$FreshnessLimitSeconds = 120; $MaxQueueEvents = 12000; $LedgerPauseFreeBytes = 5GB
$stopMarker = Join-Path $RuntimeHome 'stop-called.txt'
Set-Content -LiteralPath (Join-Path $scripts 'stop.ps1') -Value "param([switch]`$KeepSupervisorPausedOnTimeout) Set-Content -LiteralPath '$stopMarker' -Value ('paused=' + `$KeepSupervisorPausedOnTimeout)"
$savedLedgerEnv = $env:KALSHI15M_LEDGER_PATH
try {
    # Ledger path precedence mirrors Settings.ledger_path: env > pointer file > default.
    Remove-Item Env:KALSHI15M_LEDGER_PATH -ErrorAction SilentlyContinue
    Check 'ledger default path' ((Get-LedgerPath) -eq (Join-Path $RuntimeHome 'data\paper_ledger.sqlite3'))
    Set-Content -LiteralPath (Join-Path $RuntimeHome 'runtime\ledger_path.txt') -Value "  X:\ledger\paper_ledger.sqlite3  "
    Check 'ledger pointer file wins over default' ((Get-LedgerPath) -eq 'X:\ledger\paper_ledger.sqlite3')
    Set-Content -LiteralPath (Join-Path $RuntimeHome 'runtime\ledger_path.txt') -Value 'relative\paper_ledger.sqlite3'
    Check 'relative pointer resolves against collector working directory' ((Get-LedgerPath) -eq [IO.Path]::GetFullPath((Join-Path $ProjectRoot 'relative\paper_ledger.sqlite3')))
    $env:KALSHI15M_LEDGER_PATH = 'D:\override\ledger.sqlite3'
    Check 'ledger env override wins over pointer' ((Get-LedgerPath) -eq 'D:\override\ledger.sqlite3')
    $env:KALSHI15M_LEDGER_PATH = 'relative-env\paper_ledger.sqlite3'
    Check 'relative env override resolves against collector working directory' ((Get-LedgerPath) -eq [IO.Path]::GetFullPath((Join-Path $ProjectRoot 'relative-env\paper_ledger.sqlite3'))) 
    $env:KALSHI15M_LEDGER_PATH = 'D:\override\ledger.sqlite3'
    Remove-Item Env:KALSHI15M_LEDGER_PATH
    Set-Content -LiteralPath (Join-Path $RuntimeHome 'runtime\ledger_path.txt') -Value 'X:\ledger\paper_ledger.sqlite3'

    $rsv = Get-Reserve
    Check 'a missing ledger volume counts as no free space and pauses' ($rsv.ledger_free -eq 0 -and (Get-ReserveReasons $rsv) -match '^LEDGER_VOLUME_RESERVE')
    Set-Content -LiteralPath (Join-Path $RuntimeHome 'runtime\ledger_path.txt') -Value (Join-Path $RuntimeHome 'data\paper_ledger.sqlite3')
    $rsv = Get-Reserve
    Check 'reserve reports a readable ledger volume separately' ($rsv.ledger_free -gt 0 -and $rsv.ledger_reserve -eq 5GB)
    Check 'no reserve reason when both volumes have room' ((Get-ReserveReasons ([pscustomobject]@{ free = 50GB; reserve = 20GB; ledger_free = 6GB; ledger_reserve = 5GB })).Count -eq 0)
    $short = Get-ReserveReasons ([pscustomobject]@{ free = 50GB; reserve = 20GB; ledger_free = 4GB; ledger_reserve = 5GB })
    Check 'a short ledger volume pauses before the collector critical stop' ($short.Count -eq 1 -and $short[0] -like 'LEDGER_VOLUME_RESERVE*')
    $both = Get-ReserveReasons ([pscustomobject]@{ free = 1GB; reserve = 20GB; ledger_free = 4GB; ledger_reserve = 5GB })
    Check 'both short volumes are reported' ($both.Count -eq 2)

    Check 'unparseable timestamp is unknown, not an exception' ($null -eq (Get-AgeSeconds 'not-a-time'))
    Check 'future timestamp has negative age' ((Get-AgeSeconds ([DateTime]::UtcNow.AddMinutes(10).ToString('o'))) -lt -500)

    $now = [DateTime]::UtcNow.ToString('o'); $mem = [pscustomobject]@{ avail_mb = 4000; commit_free_mb = 8000 }
    $healthy = [pscustomobject]@{
        storage_latest_market_event_received_utc = $now; heartbeat_utc = $now; rest_state = 'READY'; websocket_state = 'READY'
        orderbook_sequence_state = 'VERIFIED_CONTIGUOUS'; queue_depth = 0; archive_manifest_failures = 0
        archive_checkpoint_write_failures = 0; parquet_dropped_rows = 0; journal_process_pid = $PID
        storage_write_state = 'HEALTHY'; parquet_archive_recovery_state = 'COMPLETE'; archive_integrity_state = 'VERIFIED_COMPLETE'
        disk_state = 'OK'; ledger_volume_state = 'OK'
    }
    $r = Get-HealthReasons $healthy $mem
    Check "fully evidenced healthy status has no reasons ($($r -join ';'))" ($r.Count -eq 0)
    $missing = $healthy | Select-Object * -ExcludeProperty queue_depth, parquet_dropped_rows, journal_process_pid
    $r = Get-HealthReasons $missing $mem
    Check 'missing counters are reasons, not zero' (($r -contains 'MISSING_OR_INVALID_queue_depth') -and ($r -contains 'MISSING_OR_INVALID_parquet_dropped_rows') -and ($r -contains 'JOURNAL_PROCESS_ABSENT'))
    $bad = $healthy.PSObject.Copy(); $bad.archive_manifest_failures = 'abc'; $bad.parquet_dropped_rows = 3
    $bad.heartbeat_utc = [DateTime]::UtcNow.AddMinutes(10).ToString('o')
    $r = Get-HealthReasons $bad $mem
    Check 'invalid counter, dropped rows and future heartbeat all degrade' (($r -contains 'MISSING_OR_INVALID_archive_manifest_failures') -and ($r -contains 'PARQUET_ROWS_DROPPED') -and (@($r | Where-Object { $_ -like 'STALE_WORKER_HEARTBEAT*' }).Count -eq 1))
    $dead = $healthy.PSObject.Copy(); $dead.journal_process_pid = 2147483000
    Check 'nonexistent journal process is absent' ((Get-HealthReasons $dead $mem) -contains 'JOURNAL_PROCESS_ABSENT')

    $status = [pscustomobject]@{ service = 'kalshi-15m-native'; runtime_home = $RuntimeHome; pid = $PID }
    Check 'ownership fails closed without a worker pid file' (-not (Test-StatusOwnership $status))
    Set-Content -LiteralPath $pidFile -Value (@{ pid = $PID } | ConvertTo-Json)
    Check 'ownership fails when the pid is not this collector root' (-not (Test-StatusOwnership $status))
    Check 'ownership fails for another runtime home' (-not (Test-StatusOwnership ([pscustomobject]@{ service = 'kalshi-15m-native'; runtime_home = 'E:\elsewhere'; pid = $PID })))
    Check 'ownership fails for another service' (-not (Test-StatusOwnership ([pscustomobject]@{ service = 'other'; runtime_home = $RuntimeHome; pid = $PID })))

    $threw = $false
    try { Assert-PaperOnly ([pscustomobject]@{ mode = 'PAPER_ONLY'; exchange_write_calls = 0 }) } catch { $threw = $_.Exception.Message -like 'SAFETY:*' }
    Check 'missing orders counter trips the paper-only gate' $threw
    Check 'paper-only gate stops with the supervisor kept paused' ((Get-Content -LiteralPath $stopMarker -Raw).Trim() -eq 'paused=True')
    Remove-Item -LiteralPath $stopMarker
    Assert-PaperOnly ([pscustomobject]@{ mode = 'PAPER_ONLY'; exchange_write_calls = 0; orders_submitted_to_exchange = 0 })
    Check 'verified paper-only status passes without a stop' (-not (Test-Path -LiteralPath $stopMarker))

    # Disk pause is one-shot: the stop script gets a 1-second wait, not its 10-minute default.
    $pauseCalls = Join-Path $RuntimeHome 'pause-calls.txt'
    $stopScript = Join-Path $scripts 'stop.ps1'
    $manualStop = Join-Path $RuntimeHome 'runtime\worker.manual-stop'
    $stopRequest = Join-Path $RuntimeHome 'runtime\stop.request'
    Set-Content -LiteralPath $stopScript -Value "param([int]`$GracefulWaitSeconds,[switch]`$KeepSupervisorPausedOnTimeout); Add-Content -LiteralPath '$pauseCalls' -Value `$GracefulWaitSeconds; Set-Content -LiteralPath '$manualStop' -Value 'manual_stop'; Set-Content -LiteralPath '$stopRequest' -Value 'stop'"
    function Test-StatusOwnership { param($status) return $true }
    Remove-Item -LiteralPath $manualStop, $stopRequest -Force -ErrorAction SilentlyContinue
    $firstPause = Request-DiskPause ([pscustomobject]@{ pid = 42 }) @('LEDGER_VOLUME_RESERVE')
    $secondPause = Request-DiskPause ([pscustomobject]@{ pid = 42 }) @('LEDGER_VOLUME_RESERVE')
    $calls = @(Get-Content -LiteralPath $pauseCalls)
    Check 'disk pause requested once, then recognized as already requested' ($firstPause -eq 'REQUESTED' -and $secondPause -eq 'ALREADY_REQUESTED' -and $calls.Count -eq 1)
    Check 'disk pause asks stop.ps1 to return promptly' ($calls.Count -eq 1 -and [int]$calls[0] -eq 1)
    Remove-Item Function:\Test-StatusOwnership

    # PID reuse: the process found after the idle check is not the one recorded before the stop request.
    $FrozenStopWaitSeconds = 0; $FrozenIdleCheckSeconds = 1  # Start-Sleep is mocked; 0 would divide by zero
    $script:calls = 0; $script:killed = $false
    function Get-Process { param($Id, $ErrorAction) $script:calls++; [pscustomobject]@{ Id = $Id; StartTime = [DateTime]::new(2026, 1, 1).AddSeconds([Math]::Min($script:calls, 2)); TotalProcessorTime = [TimeSpan]::Zero } }
    function Stop-Process { $script:killed = $true }
    function Start-Sleep { }
    $outcome = Invoke-FrozenWorkerRecovery ([pscustomobject]@{ pid = 999999; journal_process_pid = 0; heartbeat_utc = [DateTime]::UtcNow.AddMinutes(-20).ToString('o') })
    Check "a reused worker PID is never killed ($outcome)" ($outcome -eq 'IDENTITY_CHANGED' -and -not $script:killed)
    Check 'stop request is removed after an identity change' (-not (Test-Path -LiteralPath (Join-Path $RuntimeHome 'runtime\stop.request')))
    Remove-Item Function:\Get-Process, Function:\Stop-Process, Function:\Start-Sleep
} finally {
    if ($null -ne $savedLedgerEnv) { $env:KALSHI15M_LEDGER_PATH = $savedLedgerEnv } else { Remove-Item Env:KALSHI15M_LEDGER_PATH -ErrorAction SilentlyContinue }
    Remove-Item -Recurse -Force $RuntimeHome -ErrorAction SilentlyContinue
}
if ($failures -gt 0) { throw "$failures supervisor guard check(s) failed" }
'all supervisor guard checks passed'
