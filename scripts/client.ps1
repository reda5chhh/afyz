$ErrorActionPreference = 'Stop'
try { if ($PSVersionTable.PSVersion.Major -ge 7) { $PSStyle.OutputRendering = 'PlainText' } } catch {}
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Split-Path -Parent $here
$code = Join-Path $root 'code'
$venv = Join-Path $root 'venvs\client\Scripts\python.exe'
Set-Location -LiteralPath $code
$env:ROLE = 'client'
$env:CONTROL_PORT = '9102'
Write-Host '[client] Open the web dashboard to start Copycord…'
& $venv -m control.control
if ($LASTEXITCODE) { Write-Host ('[client] crashed with ' + $LASTEXITCODE); Read-Host 'Press Enter to close' }
