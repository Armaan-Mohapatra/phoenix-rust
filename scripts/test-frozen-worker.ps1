# Exercises the supervisor's frozen-worker recovery against throw-away python processes.
# It extracts the real functions from phoenix-phase21.ps1 (no copy to drift) and never touches
# a real worker: every "worker" here is a dummy process this script starts and cleans up.
param([string]$Python = 'C:\Users\dakin\Projects\kalshi15m-phase21\.venv\Scripts\python.exe')
$ErrorActionPreference = 'Stop'
# Use the real interpreter: the venv python.exe is a launcher stub whose own CPU stays near zero,
# so measuring it would call a spinning child idle. (The worker reports its real interpreter's PID.)
$Python = (& $Python -c 'import sys; print(sys._base_executable)').Trim()
$source = Join-Path $PSScriptRoot 'phoenix-phase21.ps1'
$ast = [System.Management.Automation.Language.Parser]::ParseFile($source, [ref]$null, [ref]$null)
$wanted = 'Write-Log', 'Get-AgeSeconds', 'Get-CpuSeconds', 'Test-Idle', 'Invoke-FrozenWorkerRecovery', 'Rotate'
foreach ($fn in $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -in $wanted }, $true)) {
    Invoke-Expression $fn.Extent.Text
}
$RuntimeHome = Join-Path $env:TEMP ("frozen-worker-test-" + [guid]::NewGuid().ToString('N').Substring(0, 8))
New-Item -ItemType Directory -Force -Path (Join-Path $RuntimeHome 'runtime'), (Join-Path $RuntimeHome 'logs') | Out-Null
$supLog = Join-Path $RuntimeHome 'logs\phoenix-supervisor.log'
$FrozenStopWaitSeconds = 3; $FrozenIdleCheckSeconds = 2
$stopRequest = Join-Path $RuntimeHome 'runtime\stop.request'
$failures = 0
function Check($name, $ok) { if ($ok) { "PASS  $name" } else { "FAIL  $name"; $script:failures++ } }
function New-Dummy([string]$code) { Start-Process -FilePath $Python -ArgumentList '-c', ('"' + $code + '"') -PassThru -WindowStyle Hidden }
function Fake($p) { [pscustomobject]@{ pid = $p.Id; journal_process_pid = 0; heartbeat_utc = [DateTime]::UtcNow.AddMinutes(-20).ToString('o') } }

$idle = New-Dummy 'import time; time.sleep(600)'
Start-Sleep -Seconds 1
$r = Invoke-FrozenWorkerRecovery (Fake $idle)
Start-Sleep -Seconds 1
Check "idle frozen worker is terminated ($r)" (($r -eq 'KILLED') -and -not (Get-Process -Id $idle.Id -ErrorAction SilentlyContinue))
Check 'stop request is cleaned up after a kill' (-not (Test-Path $stopRequest))

$busy = New-Dummy 'while True: pass'
Start-Sleep -Seconds 1
$r = Invoke-FrozenWorkerRecovery (Fake $busy)
Check "busy worker is NOT terminated ($r)" (($r -eq 'BUSY') -and [bool](Get-Process -Id $busy.Id -ErrorAction SilentlyContinue))
Stop-Process -Id $busy.Id -Force -ErrorAction SilentlyContinue

$stopFile = $stopRequest -replace '\\', '/'
$polite = New-Dummy "import os, time; exec('while not os.path.exists(r'$stopRequest'): time.sleep(0.2)')"
Start-Sleep -Seconds 1
$r = Invoke-FrozenWorkerRecovery (Fake $polite)
Check "worker that honours the stop request is left to exit ($r)" ($r -eq 'EXITED')
Stop-Process -Id $polite.Id -Force -ErrorAction SilentlyContinue

$log = Get-Content $supLog -Raw
Check 'decisions are logged' (($log -match 'FROZEN_WORKER_TERMINATED') -and ($log -match 'FROZEN_WORKER_BUSY_NOT_TERMINATED'))
Remove-Item -Recurse -Force $RuntimeHome -ErrorAction SilentlyContinue
if ($failures) { "$failures check(s) FAILED"; exit 1 } else { 'all frozen-worker checks passed'; exit 0 }
