# Script para distribuir binários para pastas de teste preservando arquivos .ini
$sourceDir = ".\bin"
$destDirs = @(
    "C:\home\dev\OliveiraLands\opensim-testes\bin1",
    "C:\home\dev\OliveiraLands\opensim-testes\bin2",
    "C:\home\dev\OliveiraLands\opensim-testes\bin3",
    "C:\home\dev\OliveiraLands\opensim-testes\bin4"
)

foreach ($dest in $destDirs) {
    if (-not (Test-Path $dest)) {
        New-Item -ItemType Directory -Path $dest -Force | Out-Null
        Write-Host "Criada pasta de destino: $dest"
    }

    Write-Host "Copiando binários para $dest ..."
    
    # Extensões permitidas (binários e arquivos associados como config e json)
    $allowedExtensions = @(".dll", ".exe", ".pdb", ".config", ".json", ".xml")

    # Copia apenas os arquivos com as extensões permitidas
    Get-ChildItem -Path $sourceDir -Recurse | Where-Object { 
        -not $_.PSIsContainer -and ($allowedExtensions -contains $_.Extension.ToLower()) 
    } | ForEach-Object {
        $destPath = Join-Path $dest $_.FullName.Substring($sourceDir.Length + 1)
        $destFileDir = Split-Path $destPath
        
        if (-not (Test-Path $destFileDir)) {
            New-Item -ItemType Directory -Path $destFileDir -Force | Out-Null
        }
        
        Copy-Item -Path $_.FullName -Destination $destPath -Force
    }
}

Write-Host "Distribuição concluída!" -ForegroundColor Green
