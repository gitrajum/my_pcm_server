# Build script for AI Agentic Platform MCP (LOCAL TESTING)
# This script builds the Docker image locally with proxy support
# Usage: .\dockerbuild.ps1 [tag]

param(
    [string]$Tag = "latest"
)

$ErrorActionPreference = "Stop"

# Get the repository root (3 levels up from scripts: scripts -> grafana-mcp -> mcps -> agentic_ai_finops_actuals -> repo root)
$scriptDir = $PSScriptRoot  # scripts folder
$grafanaMcpDir = Split-Path -Parent $scriptDir  # grafana-mcp folder
$mcpsDir = Split-Path -Parent $grafanaMcpDir  # mcps folder
$finopsDir = Split-Path -Parent $mcpsDir  # agentic_ai_finops_actuals folder
$repoRoot = Split-Path -Parent $finopsDir  # repository root

# Proxy configuration (required for Bayer network)
$proxy = "http://10.185.190.70:8080"

# Image details
$imageName = "ai-agentic-platform-mcp-grafana"
$dockerfile = Join-Path $grafanaMcpDir "Dockerfile"

# Clean up existing resources first
Write-Host "`n=== Cleaning up existing Docker resources ===" -ForegroundColor Cyan

# Stop and remove any containers using this image
$allContainers = docker ps -a -q --filter "ancestor=${imageName}:${Tag}" 2>$null
if ($allContainers) {
    Write-Host "Removing containers using ${imageName}:${Tag}..." -ForegroundColor Yellow
    docker rm -f $allContainers 2>$null | Out-Null
}

# Remove image if exists
$existingImage = docker images -q "${imageName}:${Tag}" 2>$null
if ($existingImage) {
    Write-Host "Removing old image: ${imageName}:${Tag}" -ForegroundColor Yellow
    docker rmi "${imageName}:${Tag}" 2>$null | Out-Null
}

# Build arguments with proxy
$buildArgs = @(
    "--build-arg", "HTTP_PROXY=$proxy",
    "--build-arg", "HTTPS_PROXY=$proxy",
    "--build-arg", "http_proxy=$proxy",
    "--build-arg", "https_proxy=$proxy",
    "-f", $dockerfile,
    "-t", "${imageName}:${Tag}",
    $repoRoot
)

Write-Host "`n=== Building ${imageName}:${Tag} ===" -ForegroundColor Cyan
Write-Host "Using proxy: $proxy" -ForegroundColor Yellow

# Build the image
docker build @buildArgs

if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Build successful!" -ForegroundColor Green
Write-Host "Image: ${imageName}:${Tag}"

# Stop and remove existing container if running
Write-Host ""
Write-Host "Checking for existing container..." -ForegroundColor Cyan
$existingContainer = docker ps -a -q --filter "name=mcp-server-grafana" 2>$null
if ($existingContainer) {
    Write-Host "Stopping and removing existing container..." -ForegroundColor Yellow
    docker stop mcp-server-grafana 2>&1 | Out-Null
    docker rm mcp-server-grafana 2>&1 | Out-Null
}
else {
    Write-Host "No existing container found." -ForegroundColor Gray
}

# Run the container
Write-Host "Starting container..." -ForegroundColor Cyan

# Check if .env file exists
$envFile = Join-Path $grafanaMcpDir ".env"
if (Test-Path $envFile) {
    Write-Host "Loading environment variables from .env file..." -ForegroundColor Yellow
    docker run --rm -d --name mcp-server-grafana --env-file $envFile -p 8005:8000 "${imageName}:${Tag}"
} else {
    Write-Host "Warning: .env file not found at $envFile. Running without environment variables..." -ForegroundColor Yellow
    docker run --rm -d --name mcp-server-grafana -p 8005:8000 "${imageName}:${Tag}"
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to start container!" -ForegroundColor Red
    exit 1
}

# Wait for container to start
Start-Sleep -Seconds 2

# Show container status
Write-Host ""
Write-Host "Container started successfully!" -ForegroundColor Green
docker ps --filter "name=mcp-server-grafana"

Write-Host ""
Write-Host "Useful commands:" -ForegroundColor Cyan
"docker logs mcp-server-grafana"
"docker logs -f mcp-server-grafana"
"docker stop mcp-server-grafana"


