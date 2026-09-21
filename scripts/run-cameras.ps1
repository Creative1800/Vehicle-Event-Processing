<#
.SYNOPSIS
    Replays a detection feed as the cameras would send it: one detection per file,
    in time order, a few seconds apart.

.DESCRIPTION
    Each detection is dropped into ingest/ as its own file, named after the camera that
    saw the vehicle. The feed is sorted by timestamp first, so files arrive in the order
    the cameras saw the vehicles - the gap compresses the timeline, it does not reorder it.

    The gap must stay longer than GetFile's Polling Interval plus its Minimum File Age.
    Two files picked up in one poll can be published in either order, and a swap of
    minutes of event time is more than the Flink job's 30 s out-of-orderness allows.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\run-cameras.ps1
    powershell -ExecutionPolicy Bypass -File scripts\run-cameras.ps1 -Feed sample-data\detections-b.csv
#>
param(
    [string]$Feed = "sample-data\detections.csv",
    [double]$GapSeconds = 3
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$feedPath = if ([System.IO.Path]::IsPathRooted($Feed)) { $Feed } else { Join-Path $root $Feed }
$ingest = Join-Path $root "ingest"

$lines = [System.IO.File]::ReadAllLines($feedPath) | Where-Object { $_.Trim() -ne "" }
$header = $lines[0]
$rows = $lines | Select-Object -Skip 1 | ForEach-Object {
    $fields = $_.Split(",")
    [pscustomobject]@{
        Line      = $_
        Id        = $fields[0]
        Plate     = $fields[1]
        CameraId  = $fields[2]
        Timestamp = [DateTimeOffset]::Parse($fields[3])
    }
} | Sort-Object Timestamp, Id

# No BOM: NiFi's CSV reader would take it as part of the first column name.
$utf8 = New-Object System.Text.UTF8Encoding($false)

$sent = 0
foreach ($row in $rows) {
    if ($sent -gt 0) {
        Start-Sleep -Milliseconds ([int]($GapSeconds * 1000))
    }

    $name = "$($row.CameraId)_$($row.Id).csv"
    $hidden = Join-Path $ingest ".$name"

    # Written under a dot-name, then renamed: GetFile's File Filter skips dot-files,
    # so NiFi never sees a half-written detection.
    [System.IO.File]::WriteAllText($hidden, "$header`n$($row.Line)`n", $utf8)
    Move-Item -LiteralPath $hidden -Destination (Join-Path $ingest $name)

    $sent++
    Write-Host ("{0}  {1,-7} {2,-9} {3}  -> {4}" -f `
        (Get-Date -Format "HH:mm:ss"), $row.CameraId, $row.Plate,
        $row.Timestamp.UtcDateTime.ToString("HH:mm:ss"), $name)
}

Write-Host "$sent detections sent from $Feed"
