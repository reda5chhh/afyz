$ErrorActionPreference = 'Stop'
try { if ($PSVersionTable.PSVersion.Major -ge 7) { $PSStyle.OutputRendering = 'PlainText' } } catch {}
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Split-Path -Parent $here
$code = Join-Path $root 'code'

$envFile   = Join-Path $code '.env'
$venvAdmin = Join-Path $root 'venvs\admin\Scripts\python.exe'
$venvServer= Join-Path $root 'venvs\server\Scripts\python.exe'
$venvClient= Join-Path $root 'venvs\client\Scripts\python.exe'

function Assert-Py311 {
  param([string]$Interpreter, [string]$Name)
  if (-not (Test-Path -LiteralPath $Interpreter)) {
    throw "Missing $Name interpreter at $Interpreter"
  }
  $ver = & $Interpreter -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')"
  if (-not $ver) { throw "Unable to get Python version for $Name ($Interpreter)" }
  $parts = $ver.Split('.') | ForEach-Object { [int]$_ }
  $major = $parts[0]
  $minor = $parts[1]
  if ($major -ne 3 -or $minor -ne 11) {
    throw "$Name requires Python 3.11.x (found $ver at $Interpreter)"
  }
}

function Get-EnvPorts {
  param([string]$Path)
  $ports = New-Object 'System.Collections.Generic.HashSet[int]'
  $defaults = @(8080,8765,8766,9101,9102)

  if (-not (Test-Path -LiteralPath $Path)) {
    foreach ($p in $defaults) { [void]$ports.Add([int]$p) }
    return @($ports)
  }

  $lines = Get-Content -LiteralPath $Path -Encoding UTF8
  foreach ($line in $lines) {
    
    if ($line -match '^[A-Z0-9_]+_PORT\s*=\s*([0-9]{1,5})\s*$') {
      $v = [int]$Matches[1]
      if ($v -ge 1 -and $v -le 65535) { [void]$ports.Add($v) }
    }
    
    $matches = [System.Text.RegularExpressions.Regex]::Matches(
      $line, '(?i)\b(?:ws|wss|http|https)://[^:\s]+:(\d{2,5})\b'
    )
    foreach ($m in $matches) {
      $v = [int]$m.Groups[1].Value
      if ($v -ge 1 -and $v -le 65535) { [void]$ports.Add($v) }
    }
  }
  return @($ports)
}

function Test-PortBusy {
  param([int]$Port)
  try {
    $conn = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction Stop
    if ($conn) { return $true }
  } catch {
    $net = netstat -ano | Select-String "LISTENING.*:$Port\b"
    if ($net) { return $true }
  }
  return $false
}

Write-Host '[preflight] Checking Python versions in venvs (need 3.11.x)…'
try {
  Assert-Py311 -Interpreter $venvAdmin -Name 'admin venv'
  Assert-Py311 -Interpreter $venvServer -Name 'server venv'
  Assert-Py311 -Interpreter $venvClient -Name 'client venv'
} catch {
  Write-Host ('[preflight] ERROR: ' + $_)
  exit 1
}
Write-Host '[preflight] Python looks good.'

$ports = @(Get-EnvPorts -Path $envFile)
if (-not $ports -or $ports.Count -eq 0) { $ports = @(8080,8765,8766,9101,9102) }

$busy = @()
foreach ($p in ($ports | Sort-Object -Unique)) {
  if (Test-PortBusy -Port $p) {
    $procId = $null; $pname = $null
    $line = netstat -ano | Select-String "LISTENING.*:$p\b" | Select-Object -First 1
    if ($line) {
      $parts = ($line -split '\s+') | Where-Object { $_ -ne '' }
      if ($parts.Count -ge 5) { $procId = $parts[-1] }
    }
    if ($procId) {
      try { $pname = (Get-Process -Id $procId -ErrorAction Stop).ProcessName } catch {}
      if ($pname) { $busy += ("Port {0} is in use by PID {1} ({2})" -f $p, $procId, $pname) }
      else { $busy += ("Port {0} is in use by PID {1}" -f $p, $procId) }
    } else {
      $busy += ("Port {0} is in use" -f $p)
    }
  }
}

if ($busy.Count -gt 0) {
  Write-Host '[preflight] One or more ports referenced in code\.env are busy:'
  $busy | ForEach-Object { Write-Host ("  • " + $_) }
  Write-Host 'Fix: close the process(es) using these ports or change values in code\.env, then relaunch.'
  exit 1
} else {
  Write-Host '[preflight] All referenced ports appear free.'
}
