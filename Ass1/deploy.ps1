# deploy.ps1
# PowerShell Deployment Script for Distributed System Assignment

$ErrorActionPreference = "Stop"

# Check for .env file
if (-not (Test-Path .env)) {
    Write-Host "Error: .env file not found!" -ForegroundColor Red
    Write-Host "Please create a .env file with S3_BUCKET_NAME and AWS_REGION."
    exit 1
}

Write-Host "Loading configuration from .env..." -ForegroundColor Gray
$EnvContent = Get-Content .env

# Parse S3_BUCKET_NAME
$BucketLine = $EnvContent | Where-Object { $_ -match "^S3_BUCKET_NAME=" }
if ($BucketLine) {
    $S3Bucket = $BucketLine.Split("=")[1].Trim()
} else {
    Write-Host "Error: S3_BUCKET_NAME not defined in .env" -ForegroundColor Red
    exit 1
}

# Parse AWS_REGION
$RegionLine = $EnvContent | Where-Object { $_ -match "^AWS_REGION=" }
if ($RegionLine) {
    $AwsRegion = $RegionLine.Split("=")[1].Trim()
} else {
    Write-Host "Error: AWS_REGION not defined in .env" -ForegroundColor Red
    exit 1
}

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  Distributed Systems - Deploy Script (PS)" -ForegroundColor Cyan
Write-Host "================================================"
Write-Host ""
Write-Host "Region: $AwsRegion"
Write-Host "Bucket: $S3Bucket"
Write-Host ""

# Add AWS CLI to PATH if not present
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    $AwsPath = "C:\Program Files\Amazon\AWSCLIV2"
    if (Test-Path "$AwsPath\aws.exe") {
        Write-Host "Adding AWS CLI to PATH..." -ForegroundColor Yellow
        $env:Path += ";$AwsPath"
    } else {
        Write-Host "Error: AWS CLI not found. Please install it or add to PATH." -ForegroundColor Red
        exit 1
    }
}

# Check AWS credentials
Write-Host "Checking AWS credentials..."
try {
    aws sts get-caller-identity | Out-Null
    Write-Host "AWS credentials valid." -ForegroundColor Green
} catch {
    Write-Host "AWS credentials not configured or expired." -ForegroundColor Red
    exit 1
}

# Create Bucket if not exists
Write-Host "`nChecking S3 bucket..."
try {
    aws s3api head-bucket --bucket $S3Bucket 2>$null
    Write-Host "Bucket exists: $S3Bucket" -ForegroundColor Green
} catch {
    Write-Host "Creating bucket $S3Bucket..." -ForegroundColor Yellow
    aws s3api create-bucket --bucket $S3Bucket --region $AwsRegion
    Write-Host "Bucket created." -ForegroundColor Green
}

# Build and Deploy Manager
Write-Host "`n================================================" -ForegroundColor Cyan
Write-Host "  Building Manager" -ForegroundColor Cyan
Write-Host "================================================"
Push-Location manager
cmd /c "mvn clean package -DskipTests -q"
if ($LASTEXITCODE -eq 0) {
    Write-Host "Manager built successfully." -ForegroundColor Green
    Write-Host "Uploading to S3..."
    aws s3 cp target/manager-1.0-SNAPSHOT.jar "s3://$S3Bucket/manager.jar"
    Write-Host "Uploaded manager.jar" -ForegroundColor Green
} else {
    Write-Host "Manager build failed!" -ForegroundColor Red
    Pop-Location
    exit 1
}
Pop-Location

# Build and Deploy Worker
Write-Host "`n================================================" -ForegroundColor Cyan
Write-Host "  Building Worker" -ForegroundColor Cyan
Write-Host "================================================"
Push-Location worker
cmd /c "mvn clean package -DskipTests -q"
if ($LASTEXITCODE -eq 0) {
    Write-Host "Worker built successfully." -ForegroundColor Green
    Write-Host "Uploading to S3..."
    aws s3 cp target/worker-1.0-SNAPSHOT.jar "s3://$S3Bucket/worker.jar"
    Write-Host "Uploaded worker.jar" -ForegroundColor Green
} else {
    Write-Host "Worker build failed!" -ForegroundColor Red
    Pop-Location
    exit 1
}
Pop-Location

Write-Host "`n================================================" -ForegroundColor Cyan
Write-Host "  Deployment Complete!" -ForegroundColor Green
Write-Host "================================================"
