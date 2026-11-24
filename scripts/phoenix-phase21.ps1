#Requires -Version 5.1
<#
Phase 21 isolated PAPER-ONLY realtime collector + paper evaluator supervisor.
Reuses the existing GET/WSS-read-only kalshi-15m-native worker under its own
home/port. Never touches the original runtime (%LOCALAPPDATA%\Kalshi15mNative,
port 8768) or any production collector. No exchange order route exists.

Supervisor states (runtime\phoenix-heartbeat.json, supervisor_state):
  STARTING    worker launched / status server not yet RUNNING
  RECOVERING  worker alive and replaying storage (never relaunched meanwhile)
  HEALTHY     RUNNING, fresh market data, verified books, writer keeping up
  DEGRADED    running but an alarm is raised (reasons[] lists each), or restart backoff
  BLOCKED     fail-closed: disk reserve, manual stop, unverified port owner, or safety
#>
param(
    [ValidateSet('start','stop','status','supervise','register','enable','disable','unregister')]
    [string]$Mode = 'status',
    [Parameter(Mandatory=$true)][string]$ProjectRoot,
    [string]$RuntimeHome = 'E:\PhoenixLab\runtime\k15m-native',
    [ValidateRange(1024,65535)][int]$Port = 8778,
    [ValidateRange(5,300)][int]$PollSeconds = 30
)
$ErrorActionPreference = 'Stop'
$TaskName = 'Phoenix Realtime Paper (isolated k15m)'
$ProjectRoot = (Resolve-Path -LiteralPath $ProjectRoot).Path
$RuntimeHome = [IO.Path]::GetFullPath($RuntimeHome)
if (-not (Test-Path -LiteralPath (Join-Path $ProjectRoot 'scripts\windows\start.ps1') -PathType Leaf)) { throw 'Collector project root is invalid.' }
if ($Port -eq 8768 -or $RuntimeHome -match 'Kalshi15mNative$|KalshiData|TradingNextData') {
    throw 'Refusing the original/production runtime; this launcher is isolated only.'
}
$env:KALSHI15M_HOME = $RuntimeHome
$env:KALSHI15M_STATUS_PORT = [string]$Port
# Persist book deltas only for the six Phoenix research families (tnevent::research_asset).
# All other books still update in memory; paper fills embed their own arrival book.
$env:KALSHI15M_JOURNAL_DELTA_SERIES = 'KXBTC15M,KXETH15M,KXSOL15M,KXXRP15M,KXDOGE15M,KXBNB15M'
# Same six families for WebSocket ticker rows. Archival only (no decision reads them);
# the USB research volume cannot sustain journaling every series. Remove this line to
# journal all tickers again.
$env:KALSHI15M_JOURNAL_TICKER_SERIES = $env:KALSHI15M_JOURNAL_DELTA_SERIES
# Optional delta journaling stops at 60 GiB SQLite+WAL; Parquet raw archive capped at 20 GiB.
$env:KALSHI15M_JOURNAL_BUDGET_BYTES = [string](60GB)
$env:KALSHI15M_RAW_DATA_BUDGET_BYTES = [string](20GB)
# The event loop and the journal writer share one GIL and the collector is CPU-bound:
# run the journal/Parquet side in its own process (owner-approved 2026-11-19, P0).
$env:KALSHI15M_JOURNAL_PROCESS = '1'
# Keep 4 h of events in the SQLite journal once they are verified in the manifest-backed
# Parquet archive (owner-approved 2026-11-19, P8). The journal file stops growing (~10 GB).
$env:KALSHI15M_JOURNAL_RETENTION_HOURS = '4'
$env:TMPDIR = 'E:\PhoenixLab\cache\tmp'; $env:TMP = $env:TMPDIR; $env:TEMP = $env:TMPDIR
New-Item -ItemType Directory -Force -Path $env:TMPDIR, (Join-Path $RuntimeHome 'runtime'), (Join-Path $RuntimeHome 'logs') | Out-Null
$scripts = Join-Path $ProjectRoot 'scripts\windows'
$heartbeat = Join-Path $RuntimeHome 'runtime\phoenix-heartbeat.json'
$manualStop = Join-Path $RuntimeHome 'runtime\worker.manual-stop'
$pidFile = Join-Path $RuntimeHome 'runtime\worker.pid.json'
$supLog = Join-Path $RuntimeHome 'logs\phoenix-supervisor.log'
$growthLog = Join-Path $RuntimeHome 'logs\phoenix-growth.csv'
$FreshnessLimitSeconds = 120
$MaxQueueEvents = 12000
# A worker whose heartbeat has been silent this long has a frozen event loop (normal period 5 s,
# worst stall seen ~20 s). Seen 2026-11-19: alive, 0% CPU, counters frozen, status page still
# answering from its own thread, so "alive" was reported as healthy for 18 minutes.
$FrozenHeartbeatSeconds = 600
$FrozenStopWaitSeconds = 120
$FrozenIdleCheckSeconds = 10
# Pause at the collector's LOW threshold, before it reaches the 1 GiB CRITICAL
# stop. The worker disables optional capture below 5 GiB but keeps writing the ledger;
# the supervisor stops it at this earlier reserve to protect the remaining headroom.
$LedgerPauseFreeBytes = 5GB

function Rotate([string]$path, [long]$limit) {
    if ((Test-Path -LiteralPath $path) -and (Get-Item -LiteralPath $path).Length -gt $limit) {
        Move-Item -LiteralPath $path "$path.1" -Force
    }
}
function Write-Log([string]$m) {
    Rotate $supLog 2MB
    Add-Content -LiteralPath $supLog -Value ("{0} {1}" -f [DateTime]::UtcNow.ToString('o'), $m)
}
function Resolve-LedgerPath([string]$candidate) {
    if ([string]::IsNullOrWhiteSpace($candidate)) { throw 'Ledger path is empty.' }
    # The collector launches Python with WorkingDirectory=$ProjectRoot. Resolve relative
    # overrides/pointers against that same directory, not the scheduled task's CWD.
    if (-not [IO.Path]::IsPathRooted($candidate)) { $candidate = Join-Path $ProjectRoot $candidate }
    [IO.Path]::GetFullPath($candidate)
}
function Get-LedgerPath {
    # Same precedence and relative-path base as the collector's Settings.ledger_path.
    if ($env:KALSHI15M_LEDGER_PATH) { return (Resolve-LedgerPath $env:KALSHI15M_LEDGER_PATH) }
    $pointer = Join-Path $RuntimeHome 'runtime\ledger_path.txt'
    if (Test-Path -LiteralPath $pointer -PathType Leaf) {
        $pointed = (Get-Content -LiteralPath $pointer -Raw).Trim()
        if ($pointed) { return (Resolve-LedgerPath $pointed) }
    }
    Resolve-LedgerPath (Join-Path $RuntimeHome 'data\paper_ledger.sqlite3')
}
function Get-Reserve {
    # Unknown runtime or ledger volume capacity is zero: publish BLOCKED rather than
    # letting a failed DriveInfo lookup terminate the loop before its heartbeat.
    $runtimeFree = $null; $runtimeReserve = $null
    try {
        $runtimeDrive = [IO.DriveInfo]::new([IO.Path]::GetPathRoot([IO.Path]::GetFullPath($RuntimeHome)))
        $runtimeFree = $runtimeDrive.AvailableFreeSpace
        $runtimeReserve = [Math]::Max(20GB, [long](0.15 * $runtimeDrive.TotalSize))
    } catch { }
    if ($null -eq $runtimeFree) { $runtimeFree = 0L }
    if ($null -eq $runtimeReserve) { $runtimeReserve = 20GB }

    # An unreadable or missing ledger volume counts as zero free space: pause, never start.
    $ledgerFree = $null
    try {
        $ledgerRoot = [IO.Path]::GetPathRoot((Get-LedgerPath))
        if (-not $ledgerRoot) { throw 'Ledger path has no volume root.' }
        $ledgerFree = [IO.DriveInfo]::new($ledgerRoot).AvailableFreeSpace
    } catch { }
    if ($null -eq $ledgerFree) { $ledgerFree = 0L }
    [pscustomobject]@{
        free = $runtimeFree; reserve = $runtimeReserve
        ledger_free = $ledgerFree; ledger_reserve = $LedgerPauseFreeBytes
    }
}
function Get-ReserveReasons($rsv) {
    $r = @()
    if ($rsv.free -lt $rsv.reserve) { $r += "DISK_RESERVE free=$($rsv.free) reserve=$($rsv.reserve)" }
    if ($rsv.ledger_free -lt $rsv.ledger_reserve) { $r += "LEDGER_VOLUME_RESERVE free=$($rsv.ledger_free) pause_reserve=$($rsv.ledger_reserve)" }
    ,$r
}
function Request-DiskPause($s, $Reasons) {
    if (-not (Test-StatusOwnership $s)) { return 'UNOWNED' }
    $stopRequest = Join-Path $RuntimeHome 'runtime\stop.request'
    # The worker polls stop.request itself. Write durable markers and return immediately:
    # stop.ps1's bounded wait can clear stop.request after timeout, before a stalled event
    # loop sees it, and a long wait would starve the supervisor heartbeat.
    $changed = $false
    foreach ($entry in @(
        @{ path = $manualStop; value = 'manual_stop' },
        @{ path = $stopRequest; value = 'stop' }
    )) {
        if (-not (Test-Path -LiteralPath $entry.path -PathType Leaf)) {
            $temporary = "$($entry.path).tmp"
            try {
                Set-Content -LiteralPath $temporary -Value $entry.value -Encoding ascii
                Move-Item -LiteralPath $temporary -Destination $entry.path -Force
                $changed = $true
            } catch {
                Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
                throw
            }
        }
    }
    if ($changed) {
        Write-Log ("DISK_PAUSE_REQUESTED " + ($Reasons -join ';'))
        return 'REQUESTED'
    }
    'ALREADY_REQUESTED'
}
function Get-Memory {
    $os = Get-CimInstance Win32_OperatingSystem
    [pscustomobject]@{ avail_mb = [int]($os.FreePhysicalMemory / 1KB); commit_free_mb = [int]($os.FreeVirtualMemory / 1KB) }
}
function Get-Status {
    try { Invoke-RestMethod -UseBasicParsing "http://127.0.0.1:$Port/api/v1/status" -TimeoutSec 5 } catch { $null }
}
function Test-OwnedPid([int]$procId) {
    if ($procId -le 0) { return $null }
    Get-CimInstance Win32_Process -Filter "ProcessId = $procId" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -match [regex]::Escape($ProjectRoot) -and $_.CommandLine -match '-m kalshi15m\.worker' }
}
function Get-OwnedWorker {
    if (-not (Test-Path -LiteralPath $pidFile)) { return $null }
    try { $wp = [int](Get-Content -LiteralPath $pidFile -Raw | ConvertFrom-Json).pid } catch { return $null }
    Test-OwnedPid $wp
}
function Get-AgeSeconds($utc) {
    if (-not $utc) { return $null }
    try { [Math]::Round(([DateTimeOffset]::UtcNow - [DateTimeOffset]::Parse([string]$utc).ToUniversalTime()).TotalSeconds, 1) }
    catch { $null }
}
function Test-StatusOwnership($s) {
    if (-not $s -or -not $s.pid -or -not $s.runtime_home -or $s.service -ne 'kalshi-15m-native') { return $false }
    try {
        if ([IO.Path]::GetFullPath([string]$s.runtime_home) -ne $RuntimeHome) { return $false }
        $owned = Get-OwnedWorker
        if (-not $owned -or [int]$owned.ProcessId -ne [int]$s.pid) { return $false }
        $listeners = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique)
        return ($listeners.Count -eq 1 -and $listeners[0] -eq [int]$s.pid)
    } catch { return $false }
}
function Get-CpuSeconds([int]$procId) {
    try { (Get-Process -Id $procId -ErrorAction Stop).TotalProcessorTime.TotalSeconds } catch { $null }
}
function Test-Idle([int]$procId, [double]$maxFraction) {
    # True when the process is gone or used less than maxFraction of one core over the check window.
    $before = Get-CpuSeconds $procId
    if ($null -eq $before) { return $true }
    Start-Sleep -Seconds $FrozenIdleCheckSeconds
    $after = Get-CpuSeconds $procId
    if ($null -eq $after) { return $true }
    (($after - $before) / $FrozenIdleCheckSeconds) -lt $maxFraction
}
function Invoke-FrozenWorkerRecovery($s) {
    # Returns EXITED (stopped on request), KILLED (frozen and idle, terminated) or BUSY (left alone).
    # The journal child flushes and exits when its parent's pipe closes, so killing only the parent
    # is the same recovery path as every checkpoint restart; nothing is killed unless BOTH are idle.
    $workerPid = [int]$s.pid; $childPid = [int]$s.journal_process_pid
    $workerStart = (Get-Process -Id $workerPid -ErrorAction Stop).StartTime
    $stopRequest = Join-Path $RuntimeHome 'runtime\stop.request'
    'stop' | Set-Content -LiteralPath $stopRequest -Encoding ascii
    Write-Log "FROZEN_WORKER_STOP_REQUESTED pid=$workerPid journal_pid=$childPid heartbeat_age=$(Get-AgeSeconds $s.heartbeat_utc)s"
    $deadline = [DateTime]::UtcNow.AddSeconds($FrozenStopWaitSeconds)
    while ((Get-Process -Id $workerPid -ErrorAction SilentlyContinue) -and [DateTime]::UtcNow -lt $deadline) { Start-Sleep -Seconds 2 }
    if (-not (Get-Process -Id $workerPid -ErrorAction SilentlyContinue)) {
        Remove-Item -LiteralPath $stopRequest -Force -ErrorAction SilentlyContinue
        return 'EXITED'
    }
    $workerIdle = Test-Idle $workerPid 0.02
    $childIdle = if ($childPid -gt 0) { Test-Idle $childPid 0.05 } else { $true }
    if (-not ($workerIdle -and $childIdle)) {
        Remove-Item -LiteralPath $stopRequest -Force -ErrorAction SilentlyContinue
        Write-Log "FROZEN_WORKER_BUSY_NOT_TERMINATED pid=$workerPid worker_idle=$workerIdle child_idle=$childIdle"
        return 'BUSY'
    }
    $current = Get-Process -Id $workerPid -ErrorAction SilentlyContinue
    if (-not $current -or $current.StartTime -ne $workerStart) {
        Remove-Item -LiteralPath $stopRequest -Force -ErrorAction SilentlyContinue
        return 'IDENTITY_CHANGED'
    }
    Stop-Process -InputObject $current -Force
    Write-Log "FROZEN_WORKER_TERMINATED pid=$workerPid (heartbeat silent, worker and journal process idle)"
    if ($childPid -gt 0) {
        $childDeadline = [DateTime]::UtcNow.AddSeconds(60)
        while ((Get-Process -Id $childPid -ErrorAction SilentlyContinue) -and [DateTime]::UtcNow -lt $childDeadline) { Start-Sleep -Seconds 2 }
        if (Get-Process -Id $childPid -ErrorAction SilentlyContinue) { Write-Log "FROZEN_WORKER_JOURNAL_PROCESS_STILL_RUNNING pid=$childPid (left to flush)" }
    }
    Remove-Item -LiteralPath $stopRequest -Force -ErrorAction SilentlyContinue
    'KILLED'
}
function Get-HealthReasons($s, $mem) {
    $r = New-Object System.Collections.Generic.List[string]
    $fresh = Get-AgeSeconds $s.storage_latest_market_event_received_utc
    if ($null -eq $fresh -or $fresh -lt -2 -or $fresh -gt $FreshnessLimitSeconds) { $r.Add("STALE_MARKET_EVENTS_${fresh}s") }
    $hb = Get-AgeSeconds $s.heartbeat_utc
    if ($null -eq $hb -or $hb -lt -2 -or $hb -gt 60) { $r.Add("STALE_WORKER_HEARTBEAT_${hb}s") }
    if ($s.rest_state -ne 'READY') { $r.Add("REST_$($s.rest_state)") }
    if ($s.websocket_state -ne 'READY') { $r.Add("WEBSOCKET_$($s.websocket_state)") }
    if ($s.orderbook_sequence_state -ne 'VERIFIED_CONTIGUOUS') { $r.Add("BOOK_SEQUENCE_$($s.orderbook_sequence_state)") }
    $counters = @{}
    foreach ($field in @('queue_depth','archive_manifest_failures','archive_checkpoint_write_failures','parquet_dropped_rows','journal_process_pid')) {
        $p = $s.PSObject.Properties[$field]
        $number = 0L
        if (-not $p -or $null -eq $p.Value -or -not [long]::TryParse([string]$p.Value, [ref]$number) -or $number -lt 0) { $r.Add("MISSING_OR_INVALID_$field") }
        else { $counters[$field] = $number }
    }
    if ($counters.parquet_dropped_rows -gt 0) { $r.Add('PARQUET_ROWS_DROPPED') }
    if (-not $counters.journal_process_pid -or $counters.journal_process_pid -gt [int]::MaxValue -or -not (Get-Process -Id ([int]$counters.journal_process_pid) -ErrorAction SilentlyContinue)) { $r.Add('JOURNAL_PROCESS_ABSENT') }
    if ($counters.queue_depth -ge [int](0.9 * $MaxQueueEvents)) { $r.Add("WRITER_BACKLOG_$($s.queue_depth)") }
    if ($s.storage_write_state -ne 'HEALTHY') { $r.Add("STORAGE_$($s.storage_write_state)") }
    if ($s.parquet_archive_recovery_state -match '^(FAILED_|PARTIAL_RECOVERY_|SKIPPED_LOW_DISK$|SKIPPED_RAW_BUDGET$)') {
        $r.Add("ARCHIVE_RECOVERY_$($s.parquet_archive_recovery_state)")
    }
    # Unknown or missing evidence is not success: only positive archive verification passes.
    if ($s.archive_integrity_state -notmatch '^VERIFIED_COMPLETE') { $r.Add("ARCHIVE_INTEGRITY_$($s.archive_integrity_state)") }
    if ($counters.archive_manifest_failures -gt 0) { $r.Add("ARCHIVE_MANIFEST_FAILURES_$($s.archive_manifest_failures)") }
    if ($counters.archive_checkpoint_write_failures -gt 0) { $r.Add("ARCHIVE_CHECKPOINT_WRITE_FAILURES_$($s.archive_checkpoint_write_failures)") }
    if ($s.disk_state -and $s.disk_state -ne 'OK') { $r.Add("DISK_$($s.disk_state)") }
    if ($s.ledger_volume_state -and $s.ledger_volume_state -ne 'OK') { $r.Add("LEDGER_VOLUME_$($s.ledger_volume_state)") }
    if ($s.journal_optional_capture_state -eq 'STOPPED_JOURNAL_BUDGET') { $r.Add('JOURNAL_BUDGET_REACHED_DELTAS_STOPPED') }
    if ($mem.avail_mb -lt 400) { $r.Add("LOW_RAM_$($mem.avail_mb)MB") }
    if ($mem.commit_free_mb -lt 1024) { $r.Add("LOW_COMMIT_$($mem.commit_free_mb)MB") }
    ,$r
}
function Write-Heartbeat([string]$State, $Reasons, $s, $Extra) {
    $rsv = Get-Reserve; $mem = Get-Memory
    $o = [ordered]@{
        utc = [DateTime]::UtcNow.ToString('o'); supervisor_pid = $PID; supervisor_state = $State
        reasons = @($Reasons)
        service_state = $s.service_state; startup_phase = $s.startup_phase; worker_pid = $s.pid; mode = $s.mode
        market_event_freshness_s = (Get-AgeSeconds $s.storage_latest_market_event_received_utc)
        rest_state = $s.rest_state; websocket_state = $s.websocket_state; reconnect_count = $s.reconnect_count
        sequence_state = $s.orderbook_sequence_state; sequence_gap_count = $s.sequence_gap_count
        active_market_rows = $s.active_market_rows; queue_depth = $s.queue_depth
        events_inserted = $s.events_inserted; events_duplicate = $s.events_duplicate
        sqlite_bytes = $s.sqlite_bytes; parquet_bytes = $s.parquet_bytes
        journal_optional_capture_state = $s.journal_optional_capture_state
        journal_delta_skipped = $s.journal_delta_skipped
        recovery_mode = $s.parquet_archive_recovery_mode; recovery_seconds = $s.storage_recovery_seconds
        recovery_state = $s.parquet_archive_recovery_state
        recovery_elapsed_seconds = $s.storage_recovery_elapsed_seconds
        archive_integrity_state = $s.archive_integrity_state
        archive_recovery_reason = $s.archive_recovery_reason
        archive_recovery_files_scanned = $s.archive_recovery_files_scanned
        archive_recovery_rows_scanned = $s.archive_recovery_rows_scanned
        strategy_decisions_persisted = $s.strategy_decisions_persisted
        crossmarket_decisions_persisted = $s.crossmarket_decisions_persisted
        paper_fill_records = $s.paper_fill_records; paper_settlement_records = $s.paper_settlement_records
        crossmarket_paper_fill_records = $s.crossmarket_paper_fill_records
        crossmarket_paper_settlement_records = $s.crossmarket_paper_settlement_records
        crossmarket_paper_realized_net_pnl_dollars = $s.crossmarket_paper_realized_net_pnl_dollars
        exchange_write_calls = $s.exchange_write_calls; orders_submitted_to_exchange = $s.orders_submitted_to_exchange
        disk_free_bytes = $rsv.free; disk_reserve_bytes = $rsv.reserve
        ledger_volume_free_bytes = $rsv.ledger_free; ledger_volume_reserve_bytes = $rsv.ledger_reserve
        ram_available_mb = $mem.avail_mb; commit_free_mb = $mem.commit_free_mb
    }
    if ($Extra) { foreach ($k in $Extra.Keys) { $o[$k] = $Extra[$k] } }
    $tmp = "$heartbeat.tmp"
    ($o | ConvertTo-Json -Compress -Depth 4) | Set-Content -LiteralPath $tmp -Encoding utf8
    Move-Item -LiteralPath $tmp -Destination $heartbeat -Force
    $o
}
function Get-TaskArgument {
    "--headless C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$PSCommandPath`" -Mode supervise -ProjectRoot `"$ProjectRoot`" -RuntimeHome `"$RuntimeHome`" -Port $Port"
}
function Assert-PaperOnly($s) {
    if ($s -and ($s.mode -ne 'PAPER_ONLY' -or $null -eq $s.exchange_write_calls -or $null -eq $s.orders_submitted_to_exchange -or [string]$s.exchange_write_calls -ne '0' -or [string]$s.orders_submitted_to_exchange -ne '0')) {
        & (Join-Path $scripts 'stop.ps1') -KeepSupervisorPausedOnTimeout | Out-Null
        throw 'SAFETY: worker not PAPER_ONLY or exchange writes observed; stopped.'
    }
}

switch ($Mode) {
    'start' { & (Join-Path $scripts 'start.ps1') }
    'stop' { & (Join-Path $scripts 'stop.ps1') }
    'status' { $s = Get-Status; [pscustomobject]@{ status = $s; owned = (Test-StatusOwnership $s) } | ConvertTo-Json -Depth 8 }
    'supervise' {
        # Single-instance: hold an exclusive lock for the life of the loop.
        $lockPath = Join-Path $RuntimeHome 'runtime\phoenix-supervisor.lock'
        try { $lock = [IO.File]::Open($lockPath, 'OpenOrCreate', 'ReadWrite', 'None') }
        catch { Write-Log 'SECOND_INSTANCE_REFUSED'; exit 0 }
        $failures = 0; $nextStartUtc = [DateTime]::MinValue; $restartIssuedUtc = $null
        $lastRestartSeconds = $null; $restartsIssued = 0; $lastState = ''; $nextGrowthUtc = [DateTime]::MinValue
        try {
            Write-Log "SUPERVISOR_START pid=$PID home=$RuntimeHome port=$Port"
            while ($true) {
              try {
                $mem = Get-Memory; $rsv = Get-Reserve; $s = Get-Status; $reasons = @(); $state = 'BLOCKED'
                $reserveReasons = Get-ReserveReasons $rsv
                if ($reserveReasons.Count -gt 0) {
                    # Pause, never loop: no start and no frozen-worker kill/restart while a volume is short.
                    $pauseOutcome = Request-DiskPause $s $reserveReasons
                    if ($pauseOutcome -eq 'REQUESTED') { Write-Log ("STOPPED_" + ($reserveReasons -join ';')) }
                    elseif ($pauseOutcome -eq 'ALREADY_REQUESTED') { $reasons += 'DISK_PAUSE_ALREADY_REQUESTED' }
                    $reasons += $reserveReasons
                } elseif ($s) {
                    if (-not (Test-StatusOwnership $s)) {
                        $reasons = @("PORT_${Port}_SERVED_BY_UNVERIFIED_PID_$($s.pid)")
                        # Never stop an unowned listener, but never let an unsafe one pass silently either.
                        if ($s.mode -ne 'PAPER_ONLY' -or [string]$s.exchange_write_calls -ne '0' -or [string]$s.orders_submitted_to_exchange -ne '0') {
                            $reasons += 'SAFETY_UNOWNED_LISTENER_NOT_VERIFIED_PAPER_ONLY'
                        }
                    } else {
                    Assert-PaperOnly $s
                    if ($s.service_state -eq 'RECOVERING') {
                        $state = 'RECOVERING'; $reasons = @("PHASE_$($s.startup_phase)")
                        if ($s.storage_recovery_elapsed_seconds -and [double]$s.storage_recovery_elapsed_seconds -gt 1800) {
                            $state = 'DEGRADED'
                            $reasons += "RECOVERY_OVER_30_MINUTES_$([int]$s.storage_recovery_elapsed_seconds)s"
                        }
                    } elseif ($s.service_state -eq 'STARTING') {
                        $state = 'STARTING'
                    } elseif ($s.service_state -eq 'RUNNING') {
                        $failures = 0
                        if ($restartIssuedUtc) {
                            $lastRestartSeconds = [Math]::Round(([DateTime]::UtcNow - $restartIssuedUtc).TotalSeconds, 1)
                            Write-Log "RESTART_COMPLETE seconds=$lastRestartSeconds worker_pid=$($s.pid) recovery_mode=$($s.parquet_archive_recovery_mode) recovery_seconds=$($s.storage_recovery_seconds)"
                            $restartIssuedUtc = $null
                        }
                        $reasons = Get-HealthReasons $s $mem
                        $state = 'HEALTHY'; if ($reasons.Count -gt 0) { $state = 'DEGRADED' }
                        $hbAge = Get-AgeSeconds $s.heartbeat_utc
                        if ($null -ne $hbAge -and $hbAge -gt $FrozenHeartbeatSeconds -and $s.journal_process_pid -and $reasons -notcontains 'JOURNAL_PROCESS_ABSENT' -and $reasons -notcontains 'MISSING_OR_INVALID_journal_process_pid') {
                            $outcome = Invoke-FrozenWorkerRecovery $s
                            $reasons += "FROZEN_WORKER_$outcome"
                            if ($outcome -in @('EXITED','KILLED')) { $nextStartUtc = [DateTime]::UtcNow; $restartIssuedUtc = [DateTime]::UtcNow; $restartsIssued++ }
                        }
                    } else {
                        $state = 'DEGRADED'; $reasons = @("SERVICE_$($s.service_state)")
                    }
                    }
                } else {
                    $owned = Get-OwnedWorker
                    if ($owned) {
                        # Alive but not serving: never launch a second worker over it.
                        $age = ([DateTime]::Now - $owned.CreationDate).TotalSeconds
                        if ($age -lt 120) { $state = 'STARTING' }
                        elseif ($age -lt 1800) { $state = 'RECOVERING'; $reasons = @('NO_STATUS_LISTENER_YET') }
                        else { $state = 'DEGRADED'; $reasons = @("WORKER_$($owned.ProcessId)_NO_STATUS_FOR_$([int]$age)s") }
                    } elseif (Test-Path -LiteralPath $manualStop) {
                        $reasons = @('MANUAL_STOP_HONORED')
                    } elseif ([DateTime]::UtcNow -lt $nextStartUtc) {
                        $state = 'DEGRADED'; $reasons = @("RESTART_BACKOFF_UNTIL_$($nextStartUtc.ToString('o'))", "CONSECUTIVE_START_FAILURES_$failures")
                    } else {
                        Write-Log "WORKER_ABSENT_STARTING attempt=$($failures + 1)"
                        $restartIssuedUtc = [DateTime]::UtcNow; $restartsIssued++
                        $failures++
                        $nextStartUtc = [DateTime]::UtcNow.AddSeconds([Math]::Min(900, 30 * [Math]::Pow(2, $failures)))
                        try { & (Join-Path $scripts 'start.ps1') | Out-Null; $state = 'STARTING' }
                        catch { Write-Log ("START_FAILED " + $_.Exception.Message); $state = 'DEGRADED'; $reasons = @('START_FAILED: ' + $_.Exception.Message) }
                    }
                }
                $extra = [ordered]@{ restarts_issued = $restartsIssued; consecutive_start_failures = $failures; last_restart_seconds = $lastRestartSeconds }
                Write-Heartbeat $state $reasons $s $extra | Out-Null
                if ($state -ne $lastState) { Write-Log ("STATE {0} -> {1} {2}" -f $lastState, $state, ($reasons -join ';')); $lastState = $state }
                if ($s -and [DateTime]::UtcNow -ge $nextGrowthUtc) {
                    Rotate $growthLog 1MB
                    if (-not (Test-Path -LiteralPath $growthLog)) { Add-Content -LiteralPath $growthLog -Value 'utc,sqlite_bytes,parquet_bytes,events_inserted,journal_delta_skipped,queue_depth,reconnect_count' }
                    Add-Content -LiteralPath $growthLog -Value ("{0},{1},{2},{3},{4},{5},{6}" -f [DateTime]::UtcNow.ToString('o'), $s.sqlite_bytes, $s.parquet_bytes, $s.events_inserted, $s.journal_delta_skipped, $s.queue_depth, $s.reconnect_count)
                    $nextGrowthUtc = [DateTime]::UtcNow.AddMinutes(10)
                }
              } catch {
                Write-Log ("ERROR " + $_.Exception.Message)
                if ($_.Exception.Message -like 'SAFETY:*') { throw }
              }
                Start-Sleep -Seconds $PollSeconds
            }
        } finally { $lock.Dispose() }
    }
    'register' {
        if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) { throw "Task exists: $TaskName" }
        # conhost --headless: a normal console is handed to the default terminal
        # (Windows Terminal); if that app hangs, every new console blocks forever.
        $action = New-ScheduledTaskAction -Execute 'C:\Windows\System32\conhost.exe' -Argument (Get-TaskArgument) -WorkingDirectory 'E:\PhoenixLab'
        $triggers = @((New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"),
            (New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 5)))
        $settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew `
            -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
        Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $triggers -Settings $settings `
            -Description 'Phoenix Phase 21 isolated PAPER-ONLY collector supervisor. No exchange orders.' | Out-Null
        Disable-ScheduledTask -TaskName $TaskName | Out-Null
        "Registered DISABLED: $TaskName"
    }
    'enable' { Enable-ScheduledTask -TaskName $TaskName | Out-Null; Start-ScheduledTask -TaskName $TaskName; "Enabled and started: $TaskName" }
    'disable' { Disable-ScheduledTask -TaskName $TaskName | Out-Null; Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue; "Disabled: $TaskName (worker left as-is; use -Mode stop)" }
    'unregister' { Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false; "Unregistered: $TaskName" }
}
