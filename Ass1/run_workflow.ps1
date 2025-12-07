$ErrorActionPreference = "Stop"

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  Distributed Systems - Full Workflow" -ForegroundColor Cyan
Write-Host "================================================"

# 1. Build all modules and install shared-lib
Write-Host "`nStep 1: Building all modules..." -ForegroundColor Yellow
cmd /c "mvn clean install -DskipTests -q"
if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}
Write-Host "Build successful." -ForegroundColor Green

# 2. Deploy Manager and Worker
Write-Host "`nStep 2: Deploying Manager and Worker..." -ForegroundColor Yellow
if (Test-Path .\deploy.ps1) {
    .\deploy.ps1
} else {
    Write-Host "deploy.ps1 not found!" -ForegroundColor Red
    exit 1
}

# 3. Run LocalApp
Write-Host "`nStep 3: Running LocalApp..." -ForegroundColor Yellow
$InputFile = "input-sample.txt"
$OutputFile = "output.html"
$N = 1
$Terminate = "terminate"

if (-not (Test-Path $InputFile)) {
    Write-Host "Input file '$InputFile' not found!" -ForegroundColor Red
    exit 1
}

$JarPath = "local-app/target/local-app-1.0-SNAPSHOT.jar"
if (-not (Test-Path $JarPath)) {
    Write-Host "LocalApp jar not found at '$JarPath'!" -ForegroundColor Red
    exit 1
}

Write-Host "Launching LocalApp (Input: $InputFile, Output: $OutputFile, N: $N, Terminate: $Terminate)..."
java -jar $JarPath $InputFile $OutputFile $N $Terminate
if ($LASTEXITCODE -ne 0) {
    Write-Host "LocalApp execution failed!" -ForegroundColor Red
    exit 1
}

Write-Host "`nWorkflow completed successfully!" -ForegroundColor Green
