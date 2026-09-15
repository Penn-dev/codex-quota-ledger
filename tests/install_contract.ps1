[CmdletBinding()]
param(
    [string]$BinaryPath = (Join-Path (Split-Path $PSScriptRoot -Parent) 'target\release\codex-quota-ledger.exe')
)

$ErrorActionPreference = 'Stop'
$repository = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$binary = (Resolve-Path -LiteralPath $BinaryPath).Path
$testRoot = Join-Path ([IO.Path]::GetTempPath()) ("codex-quota-ledger-install-contract-" + [guid]::NewGuid().ToString('N'))
$previousLocalAppData = $env:LOCALAPPDATA
$previousCodexHome = $env:CODEX_HOME

try {
    $env:LOCALAPPDATA = Join-Path $testRoot 'local-app-data'
    $env:CODEX_HOME = Join-Path $testRoot 'codex-home'
    & (Join-Path $repository 'install.ps1') -BinaryPath $binary -SkipPathUpdate -SkipRecorderTask | Out-Host
    if ($LASTEXITCODE -ne 0) { throw 'Isolated installer invocation failed.' }

    $installRoot = Join-Path $env:LOCALAPPDATA 'CodexQuotaLedger'
    $installedBinary = Join-Path $installRoot 'codex-quota-ledger.exe'
    $installedSkill = Join-Path $env:CODEX_HOME 'skills\codex-quota-ledger\SKILL.md'
    foreach ($path in @($installedBinary, $installedSkill)) {
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
            throw "Installer did not create expected file: $path"
        }
    }
    if (Test-Path -LiteralPath (Join-Path $installRoot 'recorder.pid')) {
        throw 'Isolated installer unexpectedly started the recorder.'
    }
    if ((& $installedBinary --version) -ne 'codex-quota-ledger 0.1.0') {
        throw 'Installed binary version is incorrect.'
    }
    Write-Output 'Isolated installer contract passed.'
}
finally {
    $env:LOCALAPPDATA = $previousLocalAppData
    $env:CODEX_HOME = $previousCodexHome
    Remove-Item -LiteralPath $testRoot -Recurse -Force -ErrorAction SilentlyContinue
}
