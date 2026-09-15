[CmdletBinding()]
param(
    [ValidatePattern('^\d+\.\d+\.\d+$')]
    [string]$Version = '0.1.0',
    [string]$Repository = 'Penn-dev/codex-quota-ledger',
    [string]$BinaryPath,
    [Parameter(DontShow)]
    [switch]$SkipPathUpdate,
    [Parameter(DontShow)]
    [switch]$SkipRecorderTask
)

$ErrorActionPreference = 'Stop'
if ($env:OS -ne 'Windows_NT') {
    throw 'This release currently supports Windows only.'
}

$installRoot = Join-Path $env:LOCALAPPDATA 'CodexQuotaLedger'
$destination = Join-Path $installRoot 'codex-quota-ledger.exe'
$skillRoot = if ($env:CODEX_HOME) { $env:CODEX_HOME } else { Join-Path $HOME '.codex' }
$skillDestination = Join-Path $skillRoot 'skills\codex-quota-ledger'
$temporaryRoot = Join-Path ([IO.Path]::GetTempPath()) ("codex-quota-ledger-install-" + [guid]::NewGuid().ToString('N'))
$assetName = 'codex-quota-ledger-windows-x64.exe'

function Stop-InstalledDashboard {
    $pidFile = Join-Path $installRoot 'dashboard.pid'
    if (-not (Test-Path -LiteralPath $pidFile)) { return }
    $savedPid = ((Get-Content -LiteralPath $pidFile -Raw).Trim() -split '\s+')[0]
    if ($savedPid -match '^\d+$') {
        & taskkill.exe /PID $savedPid /T /F 2>$null | Out-Null
    }
    Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
}

New-Item -ItemType Directory -Path $temporaryRoot -Force | Out-Null
try {
    if ($BinaryPath) {
        $sourceBinary = (Resolve-Path -LiteralPath $BinaryPath).Path
    }
    else {
        $tag = "v$Version"
        $baseUrl = "https://github.com/$Repository/releases/download/$tag"
        $sourceBinary = Join-Path $temporaryRoot $assetName
        $checksumPath = "$sourceBinary.sha256"
        Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl/$assetName" -OutFile $sourceBinary
        Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl/$assetName.sha256" -OutFile $checksumPath
        $expected = ((Get-Content -LiteralPath $checksumPath -Raw).Trim() -split '\s+')[0]
        $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $sourceBinary).Hash
        if ($expected -notmatch '^[A-Fa-f0-9]{64}$' -or $actual -ne $expected) {
            throw 'The downloaded binary failed SHA-256 verification.'
        }
    }

    if (Test-Path -LiteralPath $destination) {
        & $destination uninstall | Out-Host
        Stop-InstalledDashboard
    }

    New-Item -ItemType Directory -Path $installRoot -Force | Out-Null
    Copy-Item -LiteralPath $sourceBinary -Destination $destination -Force

    $bundledSkill = Join-Path $PSScriptRoot 'skills\codex-quota-ledger\SKILL.md'
    New-Item -ItemType Directory -Path $skillDestination -Force | Out-Null
    if (Test-Path -LiteralPath $bundledSkill) {
        Copy-Item -LiteralPath $bundledSkill -Destination (Join-Path $skillDestination 'SKILL.md') -Force
    }
    else {
        $skillUrl = "https://raw.githubusercontent.com/$Repository/v$Version/skills/codex-quota-ledger/SKILL.md"
        Invoke-WebRequest -UseBasicParsing -Uri $skillUrl -OutFile (Join-Path $skillDestination 'SKILL.md')
    }

    if (-not $SkipPathUpdate) {
        $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
        $pathEntries = @($userPath -split ';' | Where-Object { $_ })
        if (-not ($pathEntries | Where-Object { $_.TrimEnd('\') -ieq $installRoot.TrimEnd('\') })) {
            [Environment]::SetEnvironmentVariable('Path', (($pathEntries + $installRoot) -join ';'), 'User')
        }
    }

    if (-not $SkipRecorderTask) {
        & $destination install | Out-Host
        if ($LASTEXITCODE -ne 0) {
            throw 'The binary was installed, but the recorder task could not be installed.'
        }
        $deadline = [DateTime]::UtcNow.AddSeconds(15)
        while (-not (Test-Path -LiteralPath (Join-Path $installRoot 'recorder.pid')) -and [DateTime]::UtcNow -lt $deadline) {
            Start-Sleep -Milliseconds 250
        }
        if (-not (Test-Path -LiteralPath (Join-Path $installRoot 'recorder.pid'))) {
            throw 'The recorder task was installed, but startup was not confirmed within 15 seconds.'
        }

        $scanDeadline = [DateTime]::UtcNow.AddSeconds(60)
        do {
            $syncResult = & $destination sync 2>$null
            $syncExitCode = $LASTEXITCODE
            if ($syncExitCode -eq 0) { break }
            Start-Sleep -Seconds 1
        } while ([DateTime]::UtcNow -lt $scanDeadline)
        if ($syncExitCode -ne 0) {
            throw 'The recorder started, but a verified account was not available for the first local scan within 60 seconds.'
        }
    }

    $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $destination).Hash.ToLowerInvariant()
    Write-Output "Installed $(& $destination --version)"
    Write-Output "Binary: $destination"
    Write-Output "SHA-256: $hash"
    Write-Output "Codex skill: $skillDestination"
    if ($SkipRecorderTask) {
        Write-Output 'Isolated package validation complete; recorder task was intentionally skipped.'
    }
    else {
        Write-Output "First local scan complete: $syncResult"
        Write-Output 'The recorder is running. Start a new Codex task to use the installed skill.'
    }
}
finally {
    Remove-Item -LiteralPath $temporaryRoot -Recurse -Force -ErrorAction SilentlyContinue
}
