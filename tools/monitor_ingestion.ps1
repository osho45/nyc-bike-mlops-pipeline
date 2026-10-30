param(
    [string]$Repository = (Split-Path -Parent $PSScriptRoot),
    [ValidateRange(0.01,60)][double]$IntervalSeconds = 10,
    [ValidateRange(0,100000)][int]$Iterations = 0
)
$ErrorActionPreference = 'Stop'
$resolvedRepository = (Resolve-Path -LiteralPath $Repository).Path
$runRoot = Join-Path $resolvedRepository 'artifacts\monthly-ingestion-01'
$resourceRoot = Join-Path $runRoot 'resources'
New-Item -ItemType Directory -Force -Path $resourceRoot | Out-Null
$hashProvider = [Security.Cryptography.SHA256]::Create()
$identity = ([BitConverter]::ToString($hashProvider.ComputeHash([Text.Encoding]::UTF8.GetBytes($resolvedRepository.ToLowerInvariant())))).Replace('-','')
$hashProvider.Dispose()
$mutex = New-Object Threading.Mutex($false, ('Local\CitiBikeDiskMonitor_' + $identity))
$owned = $false
try {
    try { $owned = $mutex.WaitOne(0) } catch [Threading.AbandonedMutexException] { $owned = $true }
    if (-not $owned) { Write-Output 'A monitor already owns this repository'; exit 0 }
    $producerId = [Guid]::NewGuid().ToString('N')
    $sequence = 0
    $started = [DateTimeOffset]::UtcNow.ToString('o')
    $encoding = New-Object Text.UTF8Encoding($false)
    function Publish-Json([string]$Target, $Value) {
        $temporary = $Target + '.' + $producerId + '.tmp'
        [IO.File]::WriteAllText($temporary, ($Value | ConvertTo-Json -Depth 8), $encoding)
        for ($publishAttempt=0; $publishAttempt -lt 4; $publishAttempt++) {
            try {
                if ([IO.File]::Exists($Target)) {
                    # ReplaceFile avoids the explicit remove-destination gap.
                    # Windows PowerShell converts bare $null string arguments to
                    # empty paths; NullString preserves the required null backup.
                    [IO.File]::Replace($temporary,$Target,[System.Management.Automation.Language.NullString]::Value)
                } else { [IO.File]::Move($temporary,$Target) }
                return
            } catch {
                if ($publishAttempt -eq 3) { throw }
                Start-Sleep -Milliseconds (20 * ($publishAttempt + 1))
            }
        }
    }
    [IO.File]::WriteAllText((Join-Path $resourceRoot 'monitor.pid'),[string]$PID,$encoding)
    Publish-Json (Join-Path $resourceRoot 'monitor-status.json') @{status='running';producer_id=$producerId;pid=$PID;started_at=$started;repository=$resolvedRepository}
    while (-not (Test-Path -LiteralPath (Join-Path $resourceRoot 'stop-monitor'))) {
        try {
            $driveName = [IO.Path]::GetPathRoot($resolvedRepository).Substring(0,1)
            $disk = Get-PSDrive -Name $driveName
            if ($null -eq $disk.Free -or $disk.Free -lt 0) { throw 'Host drive returned no valid free-space reading' }
            $sequence++
            $reading = @{timestamp=([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()/1000.0);free_bytes=[long]$disk.Free;used_bytes=[long]$disk.Used;producer_id=$producerId;producer_pid=$PID;sequence=$sequence;schema_version=2}
            if ($disk.Free -lt 10GB) {
                Publish-Json (Join-Path $runRoot 'resource-stop.json') @{reason='Host disk below 10 GiB hard reserve';timestamp=$reading.timestamp;free_bytes=$reading.free_bytes}
            }
            $workerFile = Join-Path $resourceRoot 'worker-name.txt'
            if (Test-Path -LiteralPath $workerFile) {
                $workerName = (Get-Content -Raw -LiteralPath $workerFile).Trim()
                if ($workerName -notmatch '^citibike_controlled_worker_[0-9]+$') { throw 'Unexpected controlled worker name' }
                $workerState = $null
                for ($probe=0; $probe -lt 3; $probe++) {
                    try {
                        $rawState = docker inspect --format '{{json .State}}' $workerName 2>$null
                        if ($LASTEXITCODE -eq 0) { $workerState = $rawState | ConvertFrom-Json; break }
                    } catch { }
                    Start-Sleep -Milliseconds 200
                }
                if ($null -eq $workerState -or -not $workerState.Running) {
                    $message=@{reason='Controlled Spark worker is unavailable; ingestion stopped';timestamp=$reading.timestamp;worker=$workerName}
                    Publish-Json (Join-Path $runRoot 'resource-stop.json') $message
                    Publish-Json (Join-Path $runRoot 'worker-fatal.json') $message
                } else {
                    $reading.worker=$workerName;$reading.worker_running=$true;$reading.worker_oom_killed=[bool]$workerState.OOMKilled
                }
            }
            # Append-only fallback is durable before primary-path replacement.
            [IO.File]::AppendAllText((Join-Path $resourceRoot 'host-disk.jsonl'),(($reading | ConvertTo-Json -Compress) + [Environment]::NewLine),$encoding)
            Publish-Json (Join-Path $resourceRoot 'host-disk.json') $reading
            Publish-Json (Join-Path $resourceRoot 'monitor-status.json') @{status='running';producer_id=$producerId;pid=$PID;started_at=$started;last_success=$reading.timestamp;sequence=$sequence}
        } catch {
            # Retain the real timestamp: sustained failure must still go stale.
            $failure=@{timestamp=[DateTimeOffset]::UtcNow.ToString('o');producer_id=$producerId;sequence=$sequence;error=$_.Exception.Message}
            [IO.File]::AppendAllText((Join-Path $resourceRoot 'monitor-errors.jsonl'),(($failure | ConvertTo-Json -Compress)+[Environment]::NewLine),$encoding)
            Write-Warning $failure.error
        }
        if ($Iterations -gt 0 -and $sequence -ge $Iterations) { break }
        Start-Sleep -Milliseconds ([int]($IntervalSeconds*1000))
    }
    Publish-Json (Join-Path $resourceRoot 'monitor-status.json') @{status='stopped';producer_id=$producerId;pid=$PID;started_at=$started;stopped_at=[DateTimeOffset]::UtcNow.ToString('o');sequence=$sequence}
} finally {
    if ($owned) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
