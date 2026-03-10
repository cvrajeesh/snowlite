# Install snowlite-server on Windows — downloads the pre-built binary.
# Usage (PowerShell): irm https://raw.githubusercontent.com/cvrajeesh/snowlite/main/install.ps1 | iex
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Repo   = "cvrajeesh/snowlite"
$Binary = "snowlite-server"

# Only x86_64 is currently provided for Windows
$Artifact = "${Binary}-windows-x86_64.exe"
$Dest     = "${Binary}.exe"

$Url = "https://github.com/${Repo}/releases/latest/download/${Artifact}"

Write-Host "Downloading ${Artifact} ..."
Invoke-WebRequest -Uri $Url -OutFile $Dest -UseBasicParsing

Write-Host ""
Write-Host "snowlite-server installed -> .\${Dest}"
Write-Host ""
Write-Host "Run it:"
Write-Host "  .\${Dest}              # default port 8765"
Write-Host "  .\${Dest} --port 9000 # custom port"
