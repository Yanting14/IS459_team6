param(
    [Parameter(Mandatory=$true)][ValidateSet('ingest', 'scoring')][string]$Component,
    [Parameter(Mandatory=$true)][string]$Tag,
    [string]$Terraform = 'terraform'
)
$ErrorActionPreference = 'Stop'
$env:AWS_PAGER = ''
Set-Location (Split-Path $PSScriptRoot -Parent)
if ($Tag -notmatch '^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$') { throw 'Invalid image tag' }

function Invoke-Checked {
    param([string]$Program, [string[]]$Arguments)
    & $Program @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Program failed with exit code $LASTEXITCODE" }
}

$repository = Invoke-Checked $Terraform @('-chdir=infra', 'output', '-raw', "${Component}_ecr_repository_url")
$region = Invoke-Checked $Terraform @('-chdir=infra', 'output', '-raw', 'region')
$registry = $repository.Split('/')[0]
$image = "${repository}:${Tag}"
if ($Component -eq 'scoring') {
    Invoke-Checked 'docker' @('build', '--platform', 'linux/amd64', '-f', 'scoring/Dockerfile', '-t', $image, '.')
} else {
    Invoke-Checked 'docker' @('build', '--platform', 'linux/amd64', '-t', $image, 'ingest')
}
# A scoped login avoids depending on the workstation's credential helper (which
# can fail under Windows automation). Never change the user's Docker config.
$artifactRoot = [IO.Path]::GetFullPath((Join-Path (Get-Location).Path 'artifacts'))
New-Item -ItemType Directory -Force $artifactRoot | Out-Null
$loginDirectory = Join-Path $artifactRoot ('docker-login-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $loginDirectory | Out-Null
'{}' | Set-Content -Encoding ascii (Join-Path $loginDirectory 'config.json')
$endpoint = Invoke-Checked 'docker' @('context', 'inspect', '--format', '{{.Endpoints.docker.Host}}')
try {
    $token = Invoke-Checked 'aws' @('ecr', 'get-login-password', '--region', $region)
    # An empty config makes Docker auto-detect wincred again. Supply only this
    # registry's temporary ECR authentication directly, then remove it in finally.
    $dockerAuth = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("AWS:$token"))
    $auths = @{}
    $auths[$registry] = @{auth=$dockerAuth}
    @{auths=$auths} | ConvertTo-Json -Depth 4 |
        Set-Content -Encoding ascii (Join-Path $loginDirectory 'config.json')
    Invoke-Checked 'docker' @('--config', $loginDirectory, '--host', $endpoint, 'push', $image)
} finally {
    $token = $null
    $dockerAuth = $null
    $auths = $null
    $resolvedLogin = [IO.Path]::GetFullPath($loginDirectory)
    if (-not $resolvedLogin.StartsWith($artifactRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'Refusing to remove a Docker login directory outside artifacts'
    }
    Remove-Item -LiteralPath $resolvedLogin -Recurse -Force
}
$repositoryName = $repository.Substring($registry.Length + 1)
$digest = Invoke-Checked 'aws' @('ecr', 'describe-images', '--region', $region, '--repository-name', $repositoryName,
    '--image-ids', "imageTag=$Tag", '--query', 'imageDetails[0].imageDigest', '--output', 'text')
if ($digest -notmatch '^sha256:[a-f0-9]{64}$') { throw 'ECR did not return an image digest' }
New-Item -ItemType Directory -Force artifacts | Out-Null
@{repository=$repository; tag=$Tag; digest=$digest} | ConvertTo-Json |
    Set-Content -Encoding ascii "artifacts/${Component}-image.json"
Write-Host "Published $image"
Write-Host "Digest: $digest"
