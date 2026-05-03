<#
.SYNOPSIS
    Refresh the report end to end: open the SSH tunnel to the Azure VM,
    extract the warehouse / marts views to CSV, then rebuild every figure.

.DESCRIPTION
    Designed to be invoked manually or from Windows Task Scheduler.
    All steps are skippable so the same script works on a developer
    workstation (figures-only) and on a release machine (full refresh).

.PARAMETER SkipExtract
    Do not open the SSH tunnel and do not run extract_to_csv.py.
    Use this when no Azure access is available; figure scripts will
    fall back to their built-in synthetic data.

.PARAMETER SkipFigures
    Run only the SQL extraction.

.PARAMETER VmHost
    Public IP / DNS name of the Azure VM (default: $env:AFA_VM_HOST).

.PARAMETER SshKey
    Path to the SSH private key (default: $HOME\.ssh\azure_id_ed25519).

.EXAMPLE
    .\scripts\run_all.ps1                # full refresh
    .\scripts\run_all.ps1 -SkipExtract   # rebuild figures only
#>

[CmdletBinding()]
param(
    [switch]$SkipExtract,
    [switch]$SkipFigures,
    [string]$VmHost = $env:AFA_VM_HOST,
    [string]$SshKey = (Join-Path $HOME ".ssh\azure_id_ed25519")
)

$ErrorActionPreference = "Stop"
$ReportRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ReportRoot

$python = "python"           # adjust if you need a specific interpreter
$logDir = Join-Path $ReportRoot "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$stamp = (Get-Date -Format "yyyyMMdd-HHmmss")
$logFile = Join-Path $logDir "run_all_$stamp.log"

function Write-Step($msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format "HH:mm:ss"), $msg
    Write-Host $line -ForegroundColor Cyan
    Add-Content -Path $logFile -Value $line
}

# ---- 1. SQL extraction (optional) ----------------------------------------
$tunnelProc = $null
if (-not $SkipExtract) {
    if (-not $VmHost) {
        Write-Warning "AFA_VM_HOST is not set; skipping the extraction step."
    } else {
        Write-Step "Opening SSH tunnel to $VmHost"
        $tunnelProc = Start-Process -FilePath "ssh" -PassThru -WindowStyle Hidden `
            -ArgumentList @(
                "-i", $SshKey,
                "-L", "5432:localhost:5432",
                "-N", "azureuser@$VmHost"
            )
        Start-Sleep -Seconds 3      # give the tunnel time to bind

        Write-Step "Extracting query manifest -> exports/"
        & $python "scripts\python\extract_to_csv.py" `
            --manifest "config\queries.yaml" `
            --out-dir "exports" 2>&1 | Tee-Object -Append -FilePath $logFile

        if ($LASTEXITCODE -ne 0) {
            Write-Warning "extract_to_csv.py exited with code $LASTEXITCODE"
        }
    }
}

# ---- 2. Figure rebuild ---------------------------------------------------
if (-not $SkipFigures) {
    Write-Step "Rebuilding all figures -> figures/"
    & $python "scripts\python\run_all_figures.py" 2>&1 |
        Tee-Object -Append -FilePath $logFile

    if ($LASTEXITCODE -ne 0) {
        Write-Warning "run_all_figures.py exited with code $LASTEXITCODE"
    }
}

# ---- 3. Cleanup ----------------------------------------------------------
if ($tunnelProc -and -not $tunnelProc.HasExited) {
    Write-Step "Closing SSH tunnel"
    Stop-Process -Id $tunnelProc.Id -Force
}

Write-Step "Done. Log: $logFile"
