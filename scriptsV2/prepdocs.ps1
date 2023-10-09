./scriptsV2/loadenv.ps1

$venvPythonPath = "./scriptsV2/.venv/Scripts/python.exe"
if (Test-Path -Path "/usr") {
  # fallback to Linux venv path
  $venvPythonPath = "./scriptsV2/.venv/bin/python"
}

Write-Host 'Installing dependencies from "requirements.txt" into virtual environment'
Start-Process -FilePath $venvPythonPath -ArgumentList "-m pip install -r ./scriptsV2/requirements.txt" -Wait -NoNewWindow



Write-Host 'Running "prepdocs.py"'
$cwd = (Get-Location)

# Optional Data Lake Storage Gen2 args if using sample data for login and access control
if ($env:AZURE_ADLS_GEN2_STORAGE_ACCOUNT) {
  $adlsGen2StorageAccountArg = "--datalakestorageaccount $env:AZURE_ADLS_GEN2_STORAGE_ACCOUNT"
  $adlsGen2FilesystemPathArg = ""
  if ($env:AZURE_ADLS_GEN2_FILESYSTEM_PATH) {
    $adlsGen2FilesystemPathArg = "--datalakefilesystempath $env:ADLS_GEN2_FILESYSTEM_PATH"
  }
  $adlsGen2FilesystemArg = ""
  if ($env:AZURE_ADLS_GEN2_FILESYSTEM) {
    $adlsGen2FilesystemArg = "--datalakefilesystem $env:ADLS_GEN2_FILESYSTEM"
  }
  $aclArg = "--useacls"
}
$argumentList = "./scriptsV2/prepdocs.py `"$cwd/data/$env:AZURE_ENV_NAME/*`" $adlsGen2StorageAccountArg $adlsGen2FilesystemArg $adlsGen2FilesystemPathArg " + `
"$aclArg --env $env:AZURE_ENV_NAME"
Start-Process -FilePath $venvPythonPath -ArgumentList $argumentList -Wait -NoNewWindow
