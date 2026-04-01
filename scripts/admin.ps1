$ErrorActionPreference = 'Stop'
try { if ($PSVersionTable.PSVersion.Major -ge 7) { $PSStyle.OutputRendering = 'PlainText' } } catch {}
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Split-Path -Parent $here
$code = Join-Path $root 'code'
$venv = Join-Path $root 'venvs\admin\Scripts\python.exe'
$envPath = Join-Path $code '.env'
$port = 8080
$hostVal = 'localhost'
if (Test-Path $envPath) {
  $line = (Get-Content -LiteralPath $envPath -Encoding UTF8 | Where-Object { $_ -match '^ADMIN_PORT=' } | Select-Object -First 1)
  if ($line) { $port = ($line -split '=',2)[1].Trim() }
  $hline = (Get-Content -LiteralPath $envPath -Encoding UTF8 | Where-Object { $_ -match '^ADMIN_HOST=' } | Select-Object -First 1)
  if ($hline) { $hostVal = ($hline -split '=',2)[1].Trim() }
}
$displayHost = if ($hostVal -eq '0.0.0.0') { 'localhost' } else { $hostVal }
Set-Location -LiteralPath $code
Write-Host ('[admin] Web UI Started: http://' + $displayHost + ':' + $port)

# Auto-open dashboard unless user opts out via COPYCORD_NO_AUTO_OPEN
if (-not $env:COPYCORD_NO_AUTO_OPEN) {
  try {
    Start-Process ("http://{0}:{1}" -f $displayHost, $port)
  } catch {
    Write-Host ("[admin] Failed to auto-open browser: $_")
  }
}

try {
  & $venv -m uvicorn admin.app:app --host 0.0.0.0 --port $port
  if ($LASTEXITCODE) { throw "Exit code: $LASTEXITCODE" }
} catch {
  Write-Host ("[admin] crashed: $_")
  Read-Host 'Press Enter to close'
}
