# clasp-push-copy.ps1
# Runs clasp push, then copies .js/.html/.json from G2N_Portal to Files_gs
# (.js files are renamed to .gs)

$Source  = "C:\Users\givin\OneDrive\G2N_Google\files\G2N_Portal"
$Dest    = "C:\Users\givin\OneDrive\G2N_Google\files\Files_gs"
$Backup  = "C:\Users\givin\OneDrive\G2N_Google\files\Backups"

# --- Step 1: clasp push ---
Write-Host "Running clasp push..." -ForegroundColor Cyan
Push-Location $Source
try {
    clasp push
    if ($LASTEXITCODE -ne 0) {
        Write-Host "clasp push failed (exit code $LASTEXITCODE)" -ForegroundColor Red
        Pop-Location
        exit 1
    }
    Write-Host "clasp push succeeded." -ForegroundColor Green
} finally {
    Pop-Location
}

# --- Step 2: Ensure destination exists ---
if (-not (Test-Path $Dest)) {
    New-Item -ItemType Directory -Path $Dest -Force | Out-Null
    Write-Host "Created destination: $Dest"
}

# --- Step 3: Backup existing files ---
$answer = Read-Host "Backup existing files in Files_gs before copying? (Y/N)"
if ($answer -match '^[Yy]') {
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $backupDir = Join-Path $Backup $timestamp
    New-Item -ItemType Directory -Path $backupDir -Force | Out-Null

    $backupCount = 0
    Get-ChildItem -Path $Dest -File -Include *.gs, *.html, *.json -Recurse | ForEach-Object {
        Copy-Item -Path $_.FullName -Destination (Join-Path $backupDir $_.Name) -Force
        $backupCount++
    }
    Write-Host "Backed up $backupCount file(s) to $backupDir" -ForegroundColor Yellow
} else {
    Write-Host "Skipping backup." -ForegroundColor Gray
}

# --- Step 4: Copy only changed files ---
$copied = 0
$skipped = 0
Get-ChildItem -Path $Source -File -Include *.js, *.html, *.json -Recurse |
    Where-Object { $_.FullName -notmatch '\\(obj|\.vscode|node_modules)\\' } |
    ForEach-Object {
    if ($_.Extension -eq ".js") {
        $destName = $_.BaseName + ".gs"
    } else {
        $destName = $_.Name
    }
    $destPath = Join-Path $Dest $destName

    # Compare: copy only if dest doesn't exist or source is newer/different size
    $needsCopy = $true
    if (Test-Path $destPath) {
        $destFile = Get-Item $destPath
        if ($_.LastWriteTime -le $destFile.LastWriteTime -and $_.Length -eq $destFile.Length) {
            $needsCopy = $false
        }
    }

    if ($needsCopy) {
        Copy-Item -Path $_.FullName -Destination $destPath -Force
        Write-Host "  $($_.Name) -> $destName" -ForegroundColor White
        $copied++
    } else {
        $skipped++
    }
}

Write-Host "`nDone. $copied file(s) copied, $skipped unchanged. Destination: $Dest" -ForegroundColor Green