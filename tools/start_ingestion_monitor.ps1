param([string]$Repository = (Split-Path -Parent $PSScriptRoot), [switch]$Restart)
$ErrorActionPreference='Stop'
$repositoryPath=(Resolve-Path -LiteralPath $Repository).Path
$scriptPath=Join-Path $repositoryPath 'tools\monitor_ingestion.ps1'
$resources=Join-Path $repositoryPath 'artifacts\monthly-ingestion-01\resources'
New-Item -ItemType Directory -Force -Path $resources | Out-Null
$pidFile=Join-Path $resources 'monitor.pid'
if (Test-Path -LiteralPath $pidFile) {
    $previousId=[int](Get-Content -Raw -LiteralPath $pidFile)
    $previous=Get-CimInstance Win32_Process -Filter "ProcessId = $previousId" -ErrorAction SilentlyContinue
    if ($previous -and $previous.CommandLine -and $previous.CommandLine.Contains($scriptPath)) {
        if (-not $Restart) {
            try {
                $existingPulse=Get-Content -Raw -LiteralPath (Join-Path $resources 'host-disk.json') | ConvertFrom-Json
                $age=[DateTimeOffset]::UtcNow.ToUnixTimeSeconds()-$existingPulse.timestamp
                if ($existingPulse.producer_pid -eq $previousId -and $age -ge -30 -and $age -lt 120) {
                    Write-Output "Healthy existing monitor PID $previousId; heartbeat age $age seconds"; exit 0
                }
            } catch { }
            throw "Monitor PID $previousId exists but has no fresh matching heartbeat; inspect its logs and use -Restart for recovery"
        }
        Stop-Process -Id $previousId -Force
        Start-Sleep -Milliseconds 200
    }
}
$attempt=[Guid]::NewGuid().ToString('N')
$logs=Join-Path $resources ('monitor-runs\'+$attempt)
New-Item -ItemType Directory -Path $logs -Force | Out-Null
$arguments=@('-NoProfile','-ExecutionPolicy','Bypass','-File',('"'+$scriptPath+'"'),'-Repository',('"'+$repositoryPath+'"'))
$process=Start-Process powershell.exe -ArgumentList $arguments -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $logs 'stdout.log') -RedirectStandardError (Join-Path $logs 'stderr.log')
for ($waitAttempt=0; $waitAttempt -lt 60; $waitAttempt++) {
    Start-Sleep -Milliseconds 500
    try {
        $pulse=Get-Content -Raw -LiteralPath (Join-Path $resources 'host-disk.json') | ConvertFrom-Json
        if ($pulse.producer_pid -eq $process.Id -and ([DateTimeOffset]::UtcNow.ToUnixTimeSeconds()-$pulse.timestamp) -lt 30) {
            Write-Output "Monitor PID $($process.Id), producer $($pulse.producer_id), sequence $($pulse.sequence), free bytes $($pulse.free_bytes)"
            exit 0
        }
    } catch { }
    if ($process.HasExited) { throw "Monitor exited before its first confirmed heartbeat; inspect $logs" }
}
throw "Monitor did not publish a confirmed fresh heartbeat; inspect $logs. Resource safeguards remain active."
