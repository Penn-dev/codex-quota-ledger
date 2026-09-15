[CmdletBinding()]
param([switch]$RemoveData)

$ErrorActionPreference = 'Stop'
$installRoot = Join-Path $env:LOCALAPPDATA 'CodexQuotaLedger'
$binary = Join-Path $installRoot 'codex-quota-ledger.exe'
$skillRoot = if ($env:CODEX_HOME) { $env:CODEX_HOME } else { Join-Path $HOME '.codex' }
$skillPath = Join-Path $skillRoot 'skills\codex-quota-ledger'

if (Test-Path -LiteralPath $binary) {
    & $binary uninstall | Out-Host
}

$pidFile = Join-Path $installRoot 'dashboard.pid'
if (Test-Path -LiteralPath $pidFile) {
    $savedPid = ((Get-Content -LiteralPath $pidFile -Raw).Trim() -split '\s+')[0]
    if ($savedPid -match '^\d+$') {
        & taskkill.exe /PID $savedPid /T /F 2>$null | Out-Null
    }
    Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
}

Remove-Item -LiteralPath $skillPath -Recurse -Force -ErrorAction SilentlyContinue
$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
$keptEntries = @($userPath -split ';' | Where-Object {
    $_ -and $_.TrimEnd('\') -ine $installRoot.TrimEnd('\')
})
[Environment]::SetEnvironmentVariable('Path', ($keptEntries -join ';'), 'User')

if ($RemoveData) {
    Remove-Item -LiteralPath $installRoot -Recurse -Force -ErrorAction SilentlyContinue
    Write-Output 'Codex Quota Ledger, its skill, and local ledger data were removed.'
}
else {
    Remove-Item -LiteralPath $binary -Force -ErrorAction SilentlyContinue
    Write-Output "Codex Quota Ledger was removed. Local ledger data was preserved at $installRoot"
}
