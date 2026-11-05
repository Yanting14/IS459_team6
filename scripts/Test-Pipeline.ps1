param(
    [string]$StartDate = '2022-09-14',
    [string]$Cutoff = '2022-09-15',
    [int]$K = 1000,
    [string]$Bank = '',
    [string]$ExecutionName = ('scoring-test-' + (Get-Date -Format 'yyyyMMdd-HHmmss')),
    [string]$Terraform = 'terraform',
    [int]$MaxWaitSeconds = 21600
)
$ErrorActionPreference = 'Stop'
$env:AWS_PAGER = ''
Set-Location (Split-Path $PSScriptRoot -Parent)
if ($ExecutionName -notmatch '^[A-Za-z0-9_-]{1,80}$') { throw 'Invalid execution name' }
if ($K -lt 1 -or $K -gt 1000000) { throw 'K must be between 1 and 1000000' }
$startDay = [datetime]::ParseExact($StartDate, 'yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
$endDay = [datetime]::ParseExact($Cutoff, 'yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
if (($endDay - $startDay).TotalDays -ne 1 -or $StartDate -lt '2022-09-01' -or $StartDate -gt '2022-09-16') {
    throw 'Choose a single day within 1-16 September 2022, with an exclusive next-day cutoff'
}
if ($Bank -and $Bank -notmatch '^[A-Za-z0-9_.-]{1,64}$') { throw 'Invalid bank identifier' }
function Invoke-Checked {
    param([string]$Program, [string[]]$Arguments)
    $result = & $Program @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Program failed with exit code $LASTEXITCODE" }
    return $result
}
$arn = Invoke-Checked $Terraform @('-chdir=infra', 'output', '-raw', 'pipeline_state_machine_arn')
$region = Invoke-Checked $Terraform @('-chdir=infra', 'output', '-raw', 'region')
$folder = Join-Path 'artifacts' $ExecutionName
if (Test-Path -LiteralPath $folder) { throw "Output already exists: $folder" }
New-Item -ItemType Directory -Path $folder | Out-Null
$inputFile = Join-Path $folder 'input.json'
@{scoring=@{start=$StartDate; cutoff=$Cutoff; k=$K; bank=$Bank}} | ConvertTo-Json -Depth 5 |
    Set-Content -Encoding ascii $inputFile
$started = Invoke-Checked 'aws' @('stepfunctions', 'start-execution', '--region', $region,
    '--state-machine-arn', $arn, '--name', $ExecutionName, '--input', "file://$inputFile", '--output', 'json')
$started | Set-Content -Encoding ascii (Join-Path $folder 'execution.json')
$executionArn = ($started | ConvertFrom-Json).executionArn
Write-Host "Started $executionArn"
$timer = [Diagnostics.Stopwatch]::StartNew()
do {
    $raw = Invoke-Checked 'aws' @('stepfunctions', 'describe-execution', '--region', $region,
        '--execution-arn', $executionArn, '--output', 'json')
    $execution = $raw | ConvertFrom-Json
    Write-Host ((Get-Date -Format 'HH:mm:ss') + ' ' + $execution.status)
    if ($execution.status -ne 'RUNNING') { break }
    if ($timer.Elapsed.TotalSeconds -gt $MaxWaitSeconds) {
        throw "Local wait expired; execution is still running: $executionArn"
    }
    Start-Sleep -Seconds 15
} while ($true)
$raw | Set-Content -Encoding ascii (Join-Path $folder 'result.json')
if ($execution.status -ne 'SUCCEEDED') { throw "Pipeline $($execution.status). Inspect $folder/result.json and Step Functions history." }
$result = $execution.output | ConvertFrom-Json
if (-not $result.scoring_result.result_uri) { throw 'Pipeline completed without scoring. Set enable_scoring=true and apply first.' }
$receiptPath = Join-Path $folder 'receipt.json'
Invoke-Checked 'aws' @('s3', 'cp', $result.scoring_result.result_uri, $receiptPath, '--region', $region) | Out-Null
$receipt = Get-Content -LiteralPath $receiptPath -Raw | ConvertFrom-Json
if ($receipt.status -ne 'completed') { throw 'Gold receipt is not complete' }
$markerPath = Join-Path $folder 'completion.json'
Invoke-Checked 'aws' @('s3', 'cp', $receipt.completion_uri, $markerPath, '--region', $region) | Out-Null
$marker = Get-Content -LiteralPath $markerPath -Raw | ConvertFrom-Json
if ($marker.run_id -ne $receipt.run_id) { throw 'Receipt and completion manifest disagree' }
foreach ($artifact in $marker.artifacts.PSObject.Properties) {
    if ($artifact.Name -notmatch '^[A-Za-z0-9_.-]+$') { throw 'Invalid artifact name' }
    $target = Join-Path $folder $artifact.Name
    Invoke-Checked 'aws' @('s3', 'cp', $artifact.Value.uri, $target, '--region', $region) | Out-Null
    if ((Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLowerInvariant() -ne $artifact.Value.sha256) {
        throw "Checksum mismatch: $($artifact.Name)"
    }
}
$accounts = @(Import-Csv (Join-Path $folder 'accounts.csv'))
if ($accounts.Count -gt $K) { throw 'Review output exceeds requested K' }
if (@($accounts | Where-Object { $_.model_version -ne $receipt.model_version }).Count -gt 0) {
    throw 'Account output model version differs from receipt'
}
Write-Host "PASS: pipeline completed, all artifact checksums match, $($accounts.Count) ranked accounts."
Write-Host "Outputs: $folder"
