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
    [string]$ProjectRoot = 'C:\Users\dakin\Projects\kalshi-15m-native',
    [string]$RuntimeHome = 'E:\PhoenixLab\runtime\k15m-native',
    [int]$Port = 8778,
    [int]$PollSeconds = 30
)
$ErrorActionPreference = 'Stop'
$TaskName = 'Phoenix Realtime Paper (isolated k15m)'
if ($Port -eq 8768 -or $RuntimeHome -match 'Kalshi15mNative$|KalshiData|TradingNextData') {
    throw 'Refusing the original/production runtime; this launcher is isolated only.'
}
$env:KALSHI15M_HOME = $RuntimeHome
$env:KALSHI15M_STATUS_PORT = [string]$Port
# Persist book deltas only for the six Phoenix research families (tnevent::research_asset).
# All other books still update in memory; paper fills embed their own arrival book.
$env:KALSHI15M_JOURNAL_DELTA_SERIES = 'KXBTC15M,KXETH15M,KXSOL15M,KXXRP15M,KXDOGE15M,KXBNB15M'
# Optional delta journaling stops at 60 GiB SQLite+WAL; Parquet raw archive capped at 20 GiB.
$env:KALSHI15M_JOURNAL_BUDGET_BYTES = [string](60GB)
$env:KALSHI15M_RAW_DATA_BUDGET_BYTES = [string](20GB)
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

function Rotate([string]$path, [long]$limit) {
    if ((Test-Path -LiteralPath $path) -and (Get-Item -LiteralPath $path).Length -gt $limit) {
        Move-Item -LiteralPath $path "$path.1" -Force
    }
}
function Write-Log([string]$m) {
    Rotate $supLog 2MB
    Add-Content -LiteralPath $supLog -Value ("{0} {1}" -f [DateTime]::UtcNow.ToString('o'), $m)
}
function Get-Reserve {
    $d = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($RuntimeHome))
    [pscustomobject]@{ free = $d.AvailableFreeSpace; reserve = [Math]::Max(20GB, [long](0.15 * $d.TotalSize)) }
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
    [Math]::Round(([DateTime]::UtcNow - [DateTime]::Parse($utc).ToUniversalTime()).TotalSeconds, 1)
}
function Get-HealthReasons($s, $mem) {
    $r = New-Object System.Collections.Generic.List[string]
    $fresh = Get-AgeSeconds $s.storage_latest_market_event_received_utc
    if ($null -eq $fresh -or $fresh -gt $FreshnessLimitSeconds) { $r.Add("STALE_MARKET_EVENTS_${fresh}s") }
    $hb = Get-AgeSeconds $s.heartbeat_utc
    if ($null -eq $hb -or $hb -gt 60) { $r.Add("STALE_WORKER_HEARTBEAT_${hb}s") }
    if ($s.rest_state -ne 'READY') { $r.Add("REST_$($s.rest_state)") }
    if ($s.websocket_state -ne 'READY') { $r.Add("WEBSOCKET_$($s.websocket_state)") }
    if ($s.orderbook_sequence_state -ne 'VERIFIED_CONTIGUOUS') { $r.Add("BOOK_SEQUENCE_$($s.orderbook_sequence_state)") }
    if ([int]$s.queue_depth -ge [int](0.9 * $MaxQueueEvents)) { $r.Add("WRITER_BACKLOG_$($s.queue_depth)") }
    if ($s.storage_write_state -ne 'HEALTHY') { $r.Add("STORAGE_$($s.storage_write_state)") }
    if ($s.disk_state -and $s.disk_state -ne 'OK') { $r.Add("DISK_$($s.disk_state)") }
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
        strategy_decisions_persisted = $s.strategy_decisions_persisted
        crossmarket_decisions_persisted = $s.crossmarket_decisions_persisted
        paper_fill_records = $s.paper_fill_records; paper_settlement_records = $s.paper_settlement_records
        crossmarket_paper_fill_records = $s.crossmarket_paper_fill_records
        crossmarket_paper_settlement_records = $s.crossmarket_paper_settlement_records
        crossmarket_paper_realized_net_pnl_dollars = $s.crossmarket_paper_realized_net_pnl_dollars
        exchange_write_calls = $s.exchange_write_calls; orders_submitted_to_exchange = $s.orders_submitted_to_exchange
        disk_free_bytes = $rsv.free; disk_reserve_bytes = $rsv.reserve
        ram_available_mb = $mem.avail_mb; commit_free_mb = $mem.commit_free_mb
    }
    if ($Extra) { foreach ($k in $Extra.Keys) { $o[$k] = $Extra[$k] } }
    $tmp = "$heartbeat.tmp"
    ($o | ConvertTo-Json -Compress -Depth 4) | Set-Content -LiteralPath $tmp -Encoding utf8
    Move-Item -LiteralPath $tmp -Destination $heartbeat -Force
    $o
}
function Get-TaskArgument {
    "--headless C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$PSCommandPath`" -Mode supervise"
}
function Assert-PaperOnly($s) {
    if ($s -and ($s.mode -ne 'PAPER_ONLY' -or [int]$s.exchange_write_calls -ne 0 -or [int]$s.orders_submitted_to_exchange -ne 0)) {
        & (Join-Path $scripts 'stop.ps1') | Out-Null
        throw 'SAFETY: worker not PAPER_ONLY or exchange writes observed; stopped.'
    }
}

switch ($Mode) {
    'start' { & (Join-Path $scripts 'start.ps1') }
    'stop' { & (Join-Path $scripts 'stop.ps1') }
    'status' { $s = Get-Status; Write-Heartbeat 'STATUS_QUERY' @() $s $null | ConvertTo-Json -Depth 4 }
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
                if ($rsv.free -lt $rsv.reserve) {
                    if ($s) { & (Join-Path $scripts 'stop.ps1') | Out-Null; Write-Log "STOPPED_DISK_RESERVE free=$($rsv.free) reserve=$($rsv.reserve)" }
                    $reasons = @("DISK_RESERVE free=$($rsv.free) reserve=$($rsv.reserve)")
                } elseif ($s) {
                    Assert-PaperOnly $s
                    if (-not (Test-OwnedPid ([int]$s.pid))) {
                        $reasons = @("PORT_${Port}_SERVED_BY_UNVERIFIED_PID_$($s.pid)")
                    } elseif ($s.service_state -eq 'RECOVERING') {
                        $state = 'RECOVERING'; $reasons = @("PHASE_$($s.startup_phase)")
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
                    } else {
                        $state = 'DEGRADED'; $reasons = @("SERVICE_$($s.service_state)")
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
