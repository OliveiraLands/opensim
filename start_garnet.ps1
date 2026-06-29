# PowerShell script to start Microsoft Garnet in Docker for CAPS Texture Server testing

$containerName = "garnet-cache"
$port = 6379
$password = "testpassword"

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "Starting Microsoft Garnet Cache Service (Docker)" -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan

# 1. Check if Docker is running
Write-Host "Checking if Docker service is running..."
& docker info > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Docker is not running or not installed!" -ForegroundColor Red
    Write-Host "Please start Docker Desktop and try again." -ForegroundColor Yellow
    Exit 1
}
Write-Host "Docker is running." -ForegroundColor Green

# 2. Check if the container already exists
$existingContainer = & docker ps -a --filter "name=^/${containerName}$" --format "{{.Names}}"
if ($existingContainer -eq $containerName) {
    Write-Host "Container '$containerName' already exists." -ForegroundColor Yellow
    
    # Check if it is already running
    $runningContainer = & docker ps --filter "name=^/${containerName}$" --format "{{.Names}}"
    if ($runningContainer -eq $containerName) {
        Write-Host "Garnet is already running." -ForegroundColor Green
    } else {
        Write-Host "Starting existing Garnet container..."
        & docker start $containerName > $null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Garnet container started successfully." -ForegroundColor Green
        } else {
            Write-Host "Error: Failed to start the container." -ForegroundColor Red
            Exit 1
        }
    }
} else {
    # 3. Create and run a new Garnet container
    Write-Host "Pulling and starting Microsoft Garnet container..."
    Write-Host "Port: $port, Name: $containerName, Password: $password"
    
    # Run Garnet passing the password, volume mount, and AOF persistence parameters
    & docker run -d --name $containerName -p "${port}:6379" -v "garnet-data:/data" --restart unless-stopped ghcr.io/microsoft/garnet --password $password --aof --checkpointdir /data
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Garnet container created and started successfully!" -ForegroundColor Green
    } else {
        Write-Host "Error: Failed to run Garnet container." -ForegroundColor Red
        Exit 1
    }
}

# 4. Display connection instructions and Robust.ini configurations
Write-Host "`n=========================================================" -ForegroundColor Cyan
Write-Host "Connection Details & Robust.ini Configuration" -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "To connect to Garnet, append the following to your Robust.ini [CAPSTextureServer] section:" -ForegroundColor Yellow
Write-Host ""
Write-Host "[CAPSTextureServer]" -ForegroundColor White
Write-Host "    UseRedis = true" -ForegroundColor White
Write-Host "    RedisConnectionString = `"127.0.0.1:${port},password=${password},abortConnect=false`"" -ForegroundColor White
Write-Host "    RedisTTL = 86400" -ForegroundColor White
Write-Host ""
Write-Host "To monitor Garnet in real-time, run:" -ForegroundColor Yellow
Write-Host "docker exec -it $containerName redis-cli -a $password monitor" -ForegroundColor White
Write-Host "=========================================================" -ForegroundColor Cyan
