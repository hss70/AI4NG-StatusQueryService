# Build the SAM template
Write-Host "Building SAM template..." -ForegroundColor Cyan
sam build --template-file ./infra/template.yaml

if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed! Exiting." -ForegroundColor Red
    exit 1
}

Write-Host "Build succeeded!" -ForegroundColor Green

# Validate the built template
Write-Host "`nValidating SAM template..." -ForegroundColor Cyan
sam validate --template-file .aws-sam/build/template.yaml

if ($LASTEXITCODE -ne 0) {
    Write-Host "Validation failed! Exiting." -ForegroundColor Red
    exit 1
}

Write-Host "Validation succeeded!" -ForegroundColor Green

# Deploy (includes packaging automatically)
Write-Host "`nDeploying SAM template..." -ForegroundColor Cyan
sam deploy `
    --no-execute-changeset `
    --profile hardeepGmail `
    --config-file ./samconfig.toml

if ($LASTEXITCODE -ne 0) {
    Write-Host "Deployment failed! Exiting." -ForegroundColor Red
    exit 1
}

Write-Host "`nDeployment changeset created successfully!" -ForegroundColor Green