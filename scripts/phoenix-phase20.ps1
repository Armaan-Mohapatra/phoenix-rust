# Phoenix Phase 20 — offline finalized-data pilot (NO network or exchange orders).
# Do not use this to start, stop, configure, or replace existing collectors.
# Modes: doctor / test / build / plan / once / watch / register-disabled
[CmdletBinding()]
param(
    [ValidateSet('doctor','test','build','plan','once','watch','register-disabled')]
    [string]$Mode = 'doctor',
    [string]$Source = 'E:\TradingNextData\kalshi\raw',
    [string]$Lab = 'E:\PhoenixLab',
    [switch]$Cases,
    [switch]$Execute,
    [switch]$ConfirmOffline
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$script = Join-Path $PSScriptRoot 'phoenix_watch_finalized.py'
$exe = Join-Path $Lab 'cache\build\target\release\phoenix-tnevent.exe'
if (!(Test-Path -LiteralPath $script -PathType Leaf)) { throw "Missing watcher: $script" }

function Invoke-NativeChecked([string]$Cmd, [string[]]$Arguments) {
    & $Cmd @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Cmd returned exit code $LASTEXITCODE" }
}
function Assert-Offline {
    if (!$ConfirmOffline) {
        throw 'Requires -ConfirmOffline. This service reads only finalized historical files and does not run real-time paper orders.'
    }
}
function Assert-Storage {
    $vol = Get-PSDrive -Name E -ErrorAction Stop
    if ($vol.Free -lt 20GB) {
        throw 'Less than 20 GiB free on E:. No build/research job launched.'
    }
}
switch ($Mode) {
    'doctor' {
        foreach ($name in @('C','E')) {
            $vol = Get-PSDrive -Name $name -ErrorAction SilentlyContinue
            if ($vol) { "{0}: {1:N2} GiB free" -f $name, ($vol.Free / 1GB) }
        }
        "source present: $(Test-Path -LiteralPath $Source -PathType Container)"
        "lab present: $(Test-Path -LiteralPath $Lab -PathType Container)"
        "compiled tnevent present: $(Test-Path -LiteralPath $exe -PathType Leaf)"
        "python: $(Get-Command python -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source)"
        "cargo: $(Get-Command cargo -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source)"
        "mode: OFFLINE historical pilot, no paper orders, no exchange orders"
    }
    'test' {
        Invoke-NativeChecked 'python' @('-m','unittest','discover','-s', (Join-Path $repo 'tests'),
            '-p','test_phoenix_watch_finalized.py','-v')
    }
    'build' {
        Assert-Offline
        Assert-Storage
        $env:CARGO_TARGET_DIR = Join-Path $Lab 'cache\build\target'
        Invoke-NativeChecked 'cargo' @('test','--release','--workspace',
            '--manifest-path', (Join-Path $repo 'Cargo.toml'))
        Invoke-NativeChecked 'cargo' @('build','--release','-p','phoenix-tnevent',
            '--manifest-path', (Join-Path $repo 'Cargo.toml'))
        Invoke-NativeChecked 'python' @('-m','unittest','discover','-s', (Join-Path $repo 'tests'),
            '-p','test_phoenix_watch_finalized.py','-v')
    }
    'register-disabled' {
        Assert-Offline
        $taskName = 'Phoenix Finalized Research Paper Only'
        if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
            throw "Task already exists. Refusing to overwrite it: $taskName"
        }
        $arguments = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$PSCommandPath`" -Mode watch -Execute -ConfirmOffline"
        if ($Cases) { $arguments += ' -Cases' }
        $action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $arguments
        $trigger = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"
        $settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Days 0) -MultipleInstances IgnoreNew
        Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Description 'Historical finalized read-only research; no exchange or simulated order submission' | Out-Null
        Disable-ScheduledTask -TaskName $taskName | Out-Null
        "Registered DISABLED: $taskName. Requires local certification before enabling."
    }
    default {
        if ($Mode -in @('once','watch')) { Assert-Offline }
        if ($Mode -in @('once','watch') -and !$Execute) {
            throw "Mode $Mode requires -Execute; dry-run is Mode plan"
        }
        $a = @($script,'--source',$Source,'--workspace',$Lab,'--tnevent',$exe)
        if ($Cases) { $a += '--cases' }
        if ($Mode -in @('once','watch')) {
            Assert-Storage
            $a += '--execute'
        }
        if ($Mode -eq 'watch') { $a += '--watch' }
        Invoke-NativeChecked 'python' $a
    }
}
