# VidgeDB — installation "un clic" sous Windows (PowerShell).
#
#   irm <BASE>/install.ps1 | iex
#
# Télécharge vidgedb-windows-x86_64.zip depuis les releases, l'extrait dans
# %LOCALAPPDATA%\VidgeDB, vérifie que le binaire s'exécute, et explique
# comment lancer le twin.
#
# Note honnête : le binaire Windows n'est pas signé (pas de certificat de
# signature de code). SmartScreen affichera donc un avertissement au premier
# lancement ("Informations complémentaires" → "Exécuter quand même"). C'est
# une limite connue, pas un bug — la signature est un coût (~200-400 €/an).

$ErrorActionPreference = "Stop"

$Base    = if ($env:VIDGEDB_BASE_URL) { $env:VIDGEDB_BASE_URL } else { "https://github.com/Vidge-AI/VidgeDB/releases" }
$Version = if ($env:VIDGEDB_VERSION)  { $env:VIDGEDB_VERSION }  else { "latest" }
$Dest    = Join-Path $env:LOCALAPPDATA "VidgeDB"

if ($Version -eq "latest") {
    $Url = "$Base/latest/download/vidgedb-windows-x86_64.zip"
} else {
    $Url = "$Base/download/$Version/vidgedb-windows-x86_64.zip"
}

Write-Host "vidgedb-install: $Url"
Write-Host "vidgedb-install: destination $Dest"

$Tmp = Join-Path $env:TEMP ("vidgedb-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $Tmp -Force | Out-Null

try {
    $Zip = Join-Path $Tmp "vidgedb.zip"
    try {
        Invoke-WebRequest -Uri $Url -OutFile $Zip -UseBasicParsing
    } catch {
        throw "téléchargement impossible ($Url) — la release existe-t-elle et le dépôt est-il public ?`n$($_.Exception.Message)"
    }

    Expand-Archive -Path $Zip -DestinationPath $Tmp -Force
    $Exe = Get-ChildItem -Path $Tmp -Recurse -Filter "vidgedb.exe" | Select-Object -First 1
    if (-not $Exe) { throw "vidgedb.exe introuvable dans l'archive" }

    New-Item -ItemType Directory -Path $Dest -Force | Out-Null
    Copy-Item $Exe.FullName (Join-Path $Dest "vidgedb.exe") -Force

    # Preuve d'exécution : on n'annonce pas un succès sans l'avoir constaté.
    $VersionOut = & (Join-Path $Dest "vidgedb.exe") --version
    if ($LASTEXITCODE -ne 0) { throw "le binaire installé ne s'exécute pas" }

    Write-Host "vidgedb-install: OK — $VersionOut"
    Write-Host ""
    Write-Host "Lance le twin (console JSON-RPC HTTP) :"
    Write-Host "  `$env:VIDGEDB_TOKEN = -join ((1..48) | ForEach-Object { '{0:x}' -f (Get-Random -Max 16) })"
    Write-Host "  & `"$Dest\vidgedb.exe`" --http `"$Dest\twin.vdg`" --port 8888 --http-token `$env:VIDGEDB_TOKEN --role ingest"
    Write-Host ""
    Write-Host "Puis ouvre http://127.0.0.1:8888/health"
}
finally {
    Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
}
