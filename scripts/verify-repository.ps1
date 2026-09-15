[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repository = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Push-Location $repository

try {
    $tracked = @(git ls-files)
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to list tracked files.'
    }

    $forbiddenFiles = @(
        $tracked | Where-Object {
            $_ -match '(?i)\.(exe|pdb|db|sqlite|sqlite3|jsonl|log|pid)$' -or
            $_ -match '(?i)(^|/)(target|build|data|logs|tmp)/' -or
            ($_ -match '(^|/)\.env($|\.)' -and $_ -notmatch '(^|/)\.env\.example$')
        }
    )
    if ($forbiddenFiles.Count -gt 0) {
        throw "Forbidden generated or private files are tracked:`n$($forbiddenFiles -join "`n")"
    }

    $checks = @(
        @{
            Name = 'machine-specific Windows absolute path'
            Pattern = '[A-Za-z]:' + '\\' + '(Users|dev|CodexQuota)' + '\\'
        },
        @{
            Name = 'machine-specific Unix home path'
            Pattern = '/' + '(Users|home)' + '/[^/[:space:]]+/'
        },
        @{
            Name = 'private key marker'
            Pattern = 'BEGIN ' + '(RSA |OPENSSH |EC )?' + 'PRIVATE KEY'
        },
        @{
            Name = 'credential-like assignment'
            Pattern = '(sk' + '-[A-Za-z0-9_-]{16,}|(api[_-]?key|access[_-]?token|refresh[_-]?token|bearer)[[:space:]]*[:=][[:space:]]*[A-Za-z0-9_./+-]{12,})'
        }
    )

    foreach ($check in $checks) {
        $matches = @(& git grep -n -I -E -- $check.Pattern -- .)
        $grepExit = $LASTEXITCODE
        if ($grepExit -eq 0) {
            throw "$($check.Name) found in tracked source:`n$($matches -join "`n")"
        }
        if ($grepExit -ne 1) {
            throw "Repository scan failed for $($check.Name)."
        }
    }

    $cargoText = Get-Content -LiteralPath 'Cargo.toml' -Raw
    $installText = Get-Content -LiteralPath 'install.ps1' -Raw
    $cargoVersion = [regex]::Match($cargoText, '(?m)^version\s*=\s*"(?<version>\d+\.\d+\.\d+)"').Groups['version'].Value
    $installVersion = [regex]::Match($installText, '(?m)^\s*\[string\]\$Version\s*=\s*''(?<version>\d+\.\d+\.\d+)''').Groups['version'].Value
    if (-not $cargoVersion -or $installVersion -ne $cargoVersion) {
        throw "Installer version '$installVersion' does not match Cargo version '$cargoVersion'."
    }

    $skillText = Get-Content -LiteralPath 'skills\codex-quota-ledger\SKILL.md' -Raw
    if ($skillText -notmatch '(?s)^---\r?\nname: codex-quota-ledger\r?\ndescription: .+?\r?\n---') {
        throw 'The installed Codex skill is missing valid minimal frontmatter.'
    }

    if (Test-Path -LiteralPath 'public') {
        @('public\README.md', 'public\README.zh-CN.md', 'public\AGENTS.md') | ForEach-Object {
            if (-not (Test-Path -LiteralPath $_)) {
                throw "Missing public release template: $_"
            }
        }
    }

    Write-Output "Repository audit passed for $($tracked.Count) tracked files."
}
finally {
    Pop-Location
}
