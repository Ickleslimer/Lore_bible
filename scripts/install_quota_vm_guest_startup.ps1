# Run inside the VM once (as the user that logs on).
# Adds a Startup folder shortcut to run quota_worker.py --loop on logon.

param(
    [string]$RepoRoot = $env:THERIAC_QUOTA_VM_REPO_ROOT,
    [string]$PythonExe = $env:THERIAC_QUOTA_PYTHON
)

$ErrorActionPreference = "Stop"

if (-not $RepoRoot) {
    $RepoRoot = "Z:\Lore_bible"
}
if (-not $PythonExe) {
    $PythonExe = "python"
}

$worker = Join-Path $RepoRoot "scripts\quota_worker.py"
if (-not (Test-Path $worker)) {
    Write-Error "Worker script not found: $worker. Set -RepoRoot to the guest mount path."
}

$startup = [Environment]::GetFolderPath("Startup")
$vbs = Join-Path $startup "TheriacQuotaWorker.vbs"
$cmd = "`"$PythonExe`" `"$worker`" --loop --repo-root `"$RepoRoot`""

$vbsContent = @"
Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "$($cmd -replace '"', '""')", 0, False
"@

Set-Content -Path $vbs -Value $vbsContent -Encoding ASCII
Write-Host "Installed: $vbs"
Write-Host "Command: $cmd"
