[CmdletBinding()]
param(
    [ValidateRange(5, 3600)]
    [int]$IntervalSeconds = 15,
    [ValidateRange(1, 90)]
    [int]$RetentionDays = 8,
    [ValidateRange(0.01, 0.90)]
    [double]$ReserveFraction = 0.20,
    [string]$OutputDirectory,
    [switch]$Once,
    [switch]$InstallScheduledTask,
    [switch]$RemoveScheduledTask,
    [string]$TaskName = "QuantTrad Host Capacity Sampler"
)

$ErrorActionPreference = "Stop"
if (-not $OutputDirectory) {
    $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
    $OutputDirectory = Join-Path $repoRoot "logs\host-capacity"
}

if ($RemoveScheduledTask) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Output "Removed scheduled task: $TaskName"
    return
}

if ($InstallScheduledTask) {
    $scriptPath = $MyInvocation.MyCommand.Path
    $arguments = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", ('"{0}"' -f $scriptPath),
        "-IntervalSeconds", $IntervalSeconds,
        "-RetentionDays", $RetentionDays,
        "-ReserveFraction", $ReserveFraction,
        "-OutputDirectory", ('"{0}"' -f $OutputDirectory)
    ) -join " "
    $action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $arguments
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -RestartCount 999 `
        -RestartInterval (New-TimeSpan -Minutes 1) `
        -ExecutionTimeLimit ([TimeSpan]::Zero) `
        -MultipleInstances IgnoreNew
    $principal = New-ScheduledTaskPrincipal `
        -UserId ("{0}\{1}" -f $env:USERDOMAIN, $env:USERNAME) `
        -LogonType Interactive `
        -RunLevel Limited
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Description "QuantTrad physical Docker backing-volume capacity telemetry" `
        -Force | Out-Null
    Start-ScheduledTask -TaskName $TaskName
    Write-Output "Installed and started scheduled task: $TaskName"
    return
}
New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null

function Find-DockerDesktopDisk {
    $candidates = [System.Collections.Generic.List[object]]::new()
    $settingsPath = Join-Path $env:APPDATA "Docker\settings-store.json"
    if (Test-Path -LiteralPath $settingsPath) {
        $settings = Get-Content -LiteralPath $settingsPath -Raw | ConvertFrom-Json
        $configured = [Environment]::ExpandEnvironmentVariables(
            [string]$settings.CustomWslDistroDir
        )
        if ($configured -and (Test-Path -LiteralPath $configured)) {
            Get-ChildItem -LiteralPath $configured -Recurse -Filter *.vhdx |
                ForEach-Object {
                    $candidates.Add([pscustomobject]@{
                        File = $_
                        Method = "docker_settings.CustomWslDistroDir"
                    })
                }
        }
    }
    $defaultRoot = Join-Path $env:LOCALAPPDATA "Docker\wsl"
    if (Test-Path -LiteralPath $defaultRoot) {
        Get-ChildItem -LiteralPath $defaultRoot -Recurse -Filter *.vhdx |
            ForEach-Object {
                $candidates.Add([pscustomobject]@{
                    File = $_
                    Method = "docker_localappdata"
                })
            }
    }
    if ($candidates.Count -eq 0) {
        throw "host_capacity_discovery_failed: Docker Desktop VHDX was not found"
    }
    $selected = $candidates |
        Sort-Object `
            @{Expression = { if ($_.File.Name -eq "docker_data.vhdx") { 0 } else { 1 } }}, `
            @{Expression = { -$_.File.Length }} |
        Select-Object -First 1
    return $selected
}

$history = [System.Collections.Generic.Queue[object]]::new()
while ($true) {
    $observedAt = [DateTimeOffset]::UtcNow
    try {
        $discovery = Find-DockerDesktopDisk
        $disk = Get-Item -LiteralPath $discovery.File.FullName
        $driveRoot = [System.IO.Path]::GetPathRoot($disk.FullName)
        $drive = [System.IO.DriveInfo]::new($driveRoot)
        if (-not $drive.IsReady) {
            throw "host_capacity_discovery_failed: backing volume is not ready"
        }
        $history.Enqueue([pscustomobject]@{
            ObservedAt = $observedAt
            VhdxBytes = [long]$disk.Length
        })
        while (
            $history.Count -gt 1 -and
            ($observedAt - $history.Peek().ObservedAt).TotalHours -gt 24
        ) {
            [void]$history.Dequeue()
        }
        $growthBytesPerDay = $null
        $projectedDaysToReserve = $null
        $oldest = $history.Peek()
        $elapsedDays = ($observedAt - $oldest.ObservedAt).TotalDays
        if ($elapsedDays -ge (1.0 / 24.0)) {
            $growthBytesPerDay = [math]::Max(
                0.0,
                ([long]$disk.Length - [long]$oldest.VhdxBytes) / $elapsedDays
            )
        }
        $totalBytes = [long]$drive.TotalSize
        $availableBytes = [long]$drive.AvailableFreeSpace
        $usedBytes = $totalBytes - $availableBytes
        $reserveBytes = [long][math]::Ceiling($totalBytes * $ReserveFraction)
        if ($growthBytesPerDay -and $growthBytesPerDay -gt 0) {
            $projectedDaysToReserve = [math]::Max(
                0.0,
                ($availableBytes - $reserveBytes) / $growthBytesPerDay
            )
        }
        $payload = [ordered]@{
            observed_at = $observedAt.ToString("o")
            sample_kind = "host_filesystem"
            resource_id = "docker-backing-volume"
            capacity_scope = "physical_host_volume"
            capacity_authority = "physical_host_filesystem"
            physical_host_visible = $true
            runtime_kind = "windows_docker_desktop"
            discovery_method = $discovery.Method
            volume_id = $drive.Name
            backing_artifact = $disk.FullName
            backing_artifact_bytes = [long]$disk.Length
            total_bytes = $totalBytes
            used_bytes = $usedBytes
            available_bytes = $availableBytes
            used_percent = if ($totalBytes) { 100.0 * $usedBytes / $totalBytes } else { 0.0 }
            reserve_bytes = $reserveBytes
            growth_basis = "docker_vhdx_allocated_bytes"
            growth_bytes_per_day = $growthBytesPerDay
            projected_days_to_reserve = $projectedDaysToReserve
        }
    }
    catch {
        $payload = [ordered]@{
            observed_at = $observedAt.ToString("o")
            sample_kind = "host_capacity_unavailable"
            resource_id = "docker-backing-volume"
            capacity_scope = "physical_host_volume"
            capacity_authority = "unavailable"
            physical_host_visible = $false
            runtime_kind = "windows_docker_desktop"
            reason = $_.Exception.Message
        }
    }
    $line = $payload | ConvertTo-Json -Compress
    $dailyPath = Join-Path $OutputDirectory (
        "host-capacity-{0}.ndjson" -f $observedAt.UtcDateTime.ToString("yyyy-MM-dd")
    )
    Add-Content -LiteralPath $dailyPath -Value $line -Encoding utf8
    Write-Output $line
    Get-ChildItem -LiteralPath $OutputDirectory -Filter "host-capacity-*.ndjson" |
        Where-Object { $_.LastWriteTimeUtc -lt [DateTime]::UtcNow.AddDays(-$RetentionDays) } |
        Remove-Item -Force
    if ($Once) {
        break
    }
    Start-Sleep -Seconds $IntervalSeconds
}
