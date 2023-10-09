Write-Host "Loading azd .env file from current environment"
$output = azd env get-values
foreach ($line in $output) {
    if (!$line.Contains('=')) {
    continue
    }

    $name, $value = $line.Split("=")
    $value = $value -replace '^\"|\"$'
    [Environment]::SetEnvironmentVariable($name, $value)
}


$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
  # fallback to python3 if python not found
  $pythonCmd = Get-Command python3 -ErrorAction SilentlyContinue
}

Write-Host 'Creating python virtual environment "scriptsV2/.venv"'
Start-Process -FilePath ($pythonCmd).Source -ArgumentList "-m venv ./scriptsV2/.venv" -Wait -NoNewWindow

$venvPythonPath = "./scriptsV2/.venv/scriptsV2/python.exe"
if (Test-Path -Path "/usr") {
  # fallback to Linux venv path
  $venvPythonPath = "./scriptsV2/.venv/bin/python"
}

Write-Host 'Installing dependencies from "requirements.txt" into virtual environment'
Start-Process -FilePath $venvPythonPath -ArgumentList "-m pip install -r scriptsV2/requirements.txt" -Wait -NoNewWindow
