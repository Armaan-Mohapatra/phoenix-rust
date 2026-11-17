# Runs phoenix-campaign once and prints one JSON line with wall/CPU/peak-RSS/IO.
# Usage: powershell -File scripts\bench-campaign.ps1 -Exe phoenix-campaign.exe -Label x -ArgLine "--input f.jsonl --out DIR"
# (one string: under -File, PowerShell would otherwise bind --out to its own -OutVariable)
param(
    [Parameter(Mandatory)] [string] $Exe,
    [Parameter(Mandatory)] [string] $Label,
    [Parameter(Mandatory)] [string] $ArgLine
)
$ErrorActionPreference = 'Stop'
Add-Type -Namespace Phx -Name Io -MemberDefinition @'
[StructLayout(LayoutKind.Sequential)] public struct IO_COUNTERS {
  public ulong ReadOperationCount, WriteOperationCount, OtherOperationCount,
               ReadTransferCount, WriteTransferCount, OtherTransferCount; }
[DllImport("kernel32.dll")] public static extern bool GetProcessIoCounters(IntPtr h, out IO_COUNTERS c);
[StructLayout(LayoutKind.Sequential)] public struct MEM {
  public uint cb, PageFaultCount; public UIntPtr PeakWorkingSetSize, WorkingSetSize,
  QuotaPeakPagedPoolUsage, QuotaPagedPoolUsage, QuotaPeakNonPagedPoolUsage, QuotaNonPagedPoolUsage,
  PagefileUsage, PeakPagefileUsage; }
[DllImport("psapi.dll")] public static extern bool GetProcessMemoryInfo(IntPtr h, out MEM m, uint cb);
'@
$psi = New-Object System.Diagnostics.ProcessStartInfo $Exe
$psi.Arguments = $ArgLine
$psi.UseShellExecute = $false
$psi.RedirectStandardOutput = $true
$sw = [Diagnostics.Stopwatch]::StartNew()
$p = [Diagnostics.Process]::Start($psi)
$stdout = $p.StandardOutput.ReadToEnd()
$p.WaitForExit()
$sw.Stop()
$io = New-Object Phx.Io+IO_COUNTERS
[void][Phx.Io]::GetProcessIoCounters($p.Handle, [ref]$io)
# PeakWorkingSet64 reads 0 after exit; psapi still answers while the handle is open.
$mem = New-Object Phx.Io+MEM
[void][Phx.Io]::GetProcessMemoryInfo($p.Handle, [ref]$mem, [uint32][Runtime.InteropServices.Marshal]::SizeOf($mem))
[ordered]@{
    label = $Label; exit = $p.ExitCode; wall_s = [math]::Round($sw.Elapsed.TotalSeconds, 2)
    cpu_s = [math]::Round($p.TotalProcessorTime.TotalSeconds, 2)
    peak_rss_mb = [math]::Round($mem.PeakWorkingSetSize.ToUInt64() / 1MB, 1)
    read_mb = [math]::Round($io.ReadTransferCount / 1MB, 1)
    write_mb = [math]::Round($io.WriteTransferCount / 1MB, 1)
    resources = ($stdout -split "`n" | Where-Object { $_ -like 'resources:*' } | Select-Object -First 1)
} | ConvertTo-Json -Compress
