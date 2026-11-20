# Exercises the supervisor's frozen-worker recovery against throw-away python processes.
# It extracts the real functions from phoenix-phase21.ps1 (no copy to drift) and never touches
# a real worker: every "worker" here is a dummy process this script starts and cleans up.
param([string]$Python = '')
$ErrorActionPreference = 'Stop'
if (-not $Python) {
    $found = Get-Command python.exe -ErrorAction SilentlyContinue
    if (-not $found) { throw 'Pass -Python <path-to-python.exe> for an installed Python interpreter.' }
    $Python = $found.Source
}
# Use the real interpreter: a venv python.exe can be a launcher whose own CPU stays near zero.
$Python = (& $Python -c 'import sys; print(sys._base_executable)').Trim()
if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) { throw "Interpreter not found: $Python" }
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
function New-Dummy([string]$code) {
    # Passing inline Python through Start-Process re-quotes it, and a failed child
    # could falsely satisfy an 'EXITED' recovery assertion. Use real scratch scripts.
    $file = Join-Path $RuntimeHome ("dummy-" + [guid]::NewGuid().ToString('N') + '.py')
    Set-Content -LiteralPath $file -Value $code -Encoding UTF8
    $p = Start-Process -FilePath $Python -ArgumentList ('"' + $file + '"'), ('"' + $stopRequest + '"') -PassThru -WindowStyle Hidden
    Start-Sleep -Milliseconds 500
    if (-not (Get-Process -Id $p.Id -ErrorAction SilentlyContinue)) {
        throw "Dummy interpreter exited before test began: $file"
    }
    return $p
}
function Fake($p) { [pscustomobject]@{ pid = $p.Id; journal_process_pid = 0; heartbeat_utc = [DateTime]::UtcNow.AddMinutes(-20).ToString('o') } }

$idle = New-Dummy 'import time; time.sleep(600)'
Check 'idle dummy was alive before recovery' ([bool](Get-Process -Id $idle.Id -ErrorAction SilentlyContinue))
Start-Sleep -Seconds 1
$r = Invoke-FrozenWorkerRecovery (Fake $idle)
Start-Sleep -Seconds 1
Check "idle frozen worker is terminated ($r)" (($r -eq 'KILLED') -and -not (Get-Process -Id $idle.Id -ErrorAction SilentlyContinue))
Check 'stop request is cleaned up after a kill' (-not (Test-Path $stopRequest))

$busy = New-Dummy 'while True: pass'
Check 'busy dummy was alive before recovery' ([bool](Get-Process -Id $busy.Id -ErrorAction SilentlyContinue))
Start-Sleep -Seconds 1
$r = Invoke-FrozenWorkerRecovery (Fake $busy)
Check "busy worker is NOT terminated ($r)" (($r -eq 'BUSY') -and [bool](Get-Process -Id $busy.Id -ErrorAction SilentlyContinue))
Stop-Process -Id $busy.Id -Force -ErrorAction SilentlyContinue

$polite = New-Dummy @'
import os
import sys
import time
marker = sys.argv[1]
while not os.path.exists(marker):
    time.sleep(0.2)
'@
Start-Sleep -Seconds 1
$r = Invoke-FrozenWorkerRecovery (Fake $polite)
Check "worker that honours the stop request is left to exit ($r)" ($r -eq 'EXITED')
Stop-Process -Id $polite.Id -Force -ErrorAction SilentlyContinue

$log = Get-Content $supLog -Raw
Check 'decisions are logged' (($log -match 'FROZEN_WORKER_TERMINATED') -and ($log -match 'FROZEN_WORKER_BUSY_NOT_TERMINATED'))
Remove-Item -Recurse -Force $RuntimeHome -ErrorAction SilentlyContinue
if ($failures) { "$failures check(s) FAILED"; exit 1 } else { 'all frozen-worker checks passed'; exit 0 }
