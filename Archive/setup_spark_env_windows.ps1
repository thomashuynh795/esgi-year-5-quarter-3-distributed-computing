#Requires -Version 5.1
# =============================================================================
# setup_spark_env_windows.ps1
#
# Automatic installation script to configure a Windows machine
# for Spark Core usage (PySpark + Scala) in VS Code.
#
# This script installs:
#   - Java 17 (required by Spark 3.x)
#   - Python 3 + pip + venv
#   - Apache Spark
#   - Scala + sbt (optional)
#   - Visual Studio Code + recommended extensions
#   - PySpark and useful Python tools in a local virtual environment
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\setup_spark_env_windows.ps1
#   powershell -ExecutionPolicy Bypass -File .\setup_spark_env_windows.ps1 -NoScala
#   powershell -ExecutionPolicy Bypass -File .\setup_spark_env_windows.ps1 -NoVSCode
#   powershell -ExecutionPolicy Bypass -File .\setup_spark_env_windows.ps1 -Help
#
# If PowerShell 7 is installed, you can also use:
#   pwsh -ExecutionPolicy Bypass -File .\setup_spark_env_windows.ps1
#
# For current shell persistence (env + venv):
#   . .\setup_spark_env_windows.ps1
# =============================================================================

param(
    [Alias("no-scala")]
    [switch]$NoScala,

    [Alias("no-vscode")]
    [switch]$NoVSCode,

    [Alias("h", "help")]
    [switch]$Help
)

$ErrorActionPreference = "Stop"

# Improves HTTPS compatibility on older Windows PowerShell hosts.
try {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
} catch {
    # Ignore when running on hosts where this API is unavailable.
}

$InstallScala = -not $NoScala
$InstallVSCode = -not $NoVSCode

if ($Help) {
    Get-Content -Path $PSCommandPath | Select-Object -First 40
    exit 0
}

foreach ($arg in $args) {
    switch ($arg) {
        "--no-scala" { $InstallScala = $false }
        "--no-vscode" { $InstallVSCode = $false }
        "-h" {
            Get-Content -Path $PSCommandPath | Select-Object -First 30
            exit 0
        }
        "--help" {
            Get-Content -Path $PSCommandPath | Select-Object -First 30
            exit 0
        }
        default {
            Write-Host "Unknown option: $arg" -ForegroundColor Red
            Write-Host "Use --help to see available options." -ForegroundColor Yellow
            exit 1
        }
    }
}

function Log {
    param([string]$Message)
    Write-Host "`n[INFO] $Message" -ForegroundColor Cyan
}

function Warn {
    param([string]$Message)
    Write-Host "`n[WARN] $Message" -ForegroundColor Yellow
}

function Err {
    param([string]$Message)
    Write-Host "`n[ERROR] $Message" -ForegroundColor Red
}

function Test-Command {
    param([Parameter(Mandatory = $true)][string]$Name)
    return [bool](Get-Command $Name -ErrorAction SilentlyContinue)
}

function Ensure-Winget {
    if (-not (Test-Command "winget")) {
        Err "winget not found. Install App Installer from Microsoft Store, then rerun this script."
        exit 1
    }
}

function Test-WingetInstalled {
    param([Parameter(Mandatory = $true)][string]$Id)

    $output = winget list --exact --id $Id --accept-source-agreements 2>$null
    return ($LASTEXITCODE -eq 0 -and ($output -match [regex]::Escape($Id)))
}

function Install-WingetPackageIfMissing {
    param(
        [Parameter(Mandatory = $true)][string]$Id,
        [string]$Name = $Id
    )

    if (Test-WingetInstalled -Id $Id) {
        Log "winget package '$Name' already installed, skipping."
        return
    }

    Log "Installing $Name..."
    winget install --exact --id $Id --accept-package-agreements --accept-source-agreements --silent
    if ($LASTEXITCODE -ne 0) {
        Warn "Unable to install '$Name' automatically. Please install it manually."
    }
}

function Install-PipPackageIfMissing {
    param(
        [Parameter(Mandatory = $true)][string]$PythonExe,
        [Parameter(Mandatory = $true)][string]$PackageName,
        [string]$ImportName = $PackageName
    )

    & $PythonExe -c "import $ImportName" 2>$null
    if ($LASTEXITCODE -eq 0) {
        Log "pip package '$PackageName' already installed in .venv, skipping."
        return
    }

    Log "Installing pip package '$PackageName' in .venv..."
    & $PythonExe -m pip install $PackageName
}

function Add-UserPathEntry {
    param([Parameter(Mandatory = $true)][string]$PathEntry)

    $current = [Environment]::GetEnvironmentVariable("Path", "User")
    if ([string]::IsNullOrWhiteSpace($current)) {
        [Environment]::SetEnvironmentVariable("Path", $PathEntry, "User")
        return
    }

    $parts = $current -split ";" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    if ($parts -contains $PathEntry) {
        return
    }

    $newPath = ($parts + $PathEntry) -join ";"
    [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
}

$ProjectDir = Split-Path -Parent $PSCommandPath
$IsDotSourced = ($MyInvocation.InvocationName -eq ".")

function Test-IsWindowsHost {
    if ($env:OS -eq "Windows_NT") {
        return $true
    }

    try {
        return [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
            [System.Runtime.InteropServices.OSPlatform]::Windows
        )
    } catch {
        return $false
    }
}

Log "Detecting operating system..."
if (-not (Test-IsWindowsHost)) {
    Err "This script is intended for Windows only."
    exit 1
}

# ---------------------------------------------------------------------------
# 1) Ensure package manager
# ---------------------------------------------------------------------------
Log "Ensuring winget is available..."
Ensure-Winget

# ---------------------------------------------------------------------------
# 2) Install Java 17
# ---------------------------------------------------------------------------
Log "Installing Java 17 (required by Spark)..."
Install-WingetPackageIfMissing -Id "Microsoft.OpenJDK.17" -Name "OpenJDK 17"

$javaCmd = Get-Command java -ErrorAction SilentlyContinue
$JavaHomePath = $null
if ($javaCmd) {
    $JavaHomePath = Split-Path -Parent (Split-Path -Parent $javaCmd.Source)
} else {
    # Common fallback path for Microsoft OpenJDK 17 via winget.
    $javaFallback = "${env:ProgramFiles}\Microsoft\jdk-17*"
    $candidate = Get-ChildItem -Path $javaFallback -Directory -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($candidate) {
        $JavaHomePath = $candidate.FullName
    }
}

if ($JavaHomePath) {
    $env:JAVA_HOME = $JavaHomePath
    [Environment]::SetEnvironmentVariable("JAVA_HOME", $JavaHomePath, "User")
    Add-UserPathEntry -PathEntry (Join-Path $JavaHomePath "bin")
    $env:Path = "$JavaHomePath\bin;$env:Path"
} else {
    Warn "Unable to resolve JAVA_HOME automatically."
}

# ---------------------------------------------------------------------------
# 3) Install Python 3
# ---------------------------------------------------------------------------
Log "Installing Python 3..."
Install-WingetPackageIfMissing -Id "Python.Python.3.12" -Name "Python 3.12"

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    Warn "python command not found in current shell; trying py launcher."
}

# ---------------------------------------------------------------------------
# 4) Install Apache Spark
# ---------------------------------------------------------------------------
Log "Installing Apache Spark..."
$SparkVersion = "3.5.1"
$HadoopVersion = "3"
$SparkPkg = "spark-$SparkVersion-bin-hadoop$HadoopVersion"
$SparkBaseDir = Join-Path $env:LOCALAPPDATA "Spark"
$SparkHomePath = Join-Path $SparkBaseDir $SparkPkg

if (-not (Test-Path $SparkHomePath)) {
    Log "Downloading Spark $SparkVersion..."
    if (-not (Test-Command "tar")) {
        Err "'tar' command not found. Install bsdtar/Git for Windows or enable tar in PATH, then rerun."
        exit 1
    }

    $tmpZip = Join-Path $env:TEMP "$SparkPkg.tgz"
    $tmpExtract = Join-Path $env:TEMP "$SparkPkg-extract"

    Invoke-WebRequest -Uri "https://archive.apache.org/dist/spark/spark-$SparkVersion/$SparkPkg.tgz" -OutFile $tmpZip

    if (Test-Path $tmpExtract) {
        Remove-Item -Recurse -Force $tmpExtract
    }
    New-Item -ItemType Directory -Path $tmpExtract | Out-Null
    New-Item -ItemType Directory -Path $SparkBaseDir -Force | Out-Null

    tar -xzf $tmpZip -C $tmpExtract

    if (-not (Test-Path (Join-Path $tmpExtract $SparkPkg))) {
        Err "Spark extraction failed."
        exit 1
    }

    Move-Item -Path (Join-Path $tmpExtract $SparkPkg) -Destination $SparkHomePath
    Remove-Item -Force $tmpZip
    Remove-Item -Recurse -Force $tmpExtract
} else {
    Log "Spark already installed at '$SparkHomePath', skipping."
}

$env:SPARK_HOME = $SparkHomePath
[Environment]::SetEnvironmentVariable("SPARK_HOME", $SparkHomePath, "User")
Add-UserPathEntry -PathEntry (Join-Path $SparkHomePath "bin")
$env:Path = "$SparkHomePath\bin;$env:Path"

# ---------------------------------------------------------------------------
# 5) Install Scala + sbt (optional)
# ---------------------------------------------------------------------------
if ($InstallScala) {
    Log "Installing Scala and sbt..."
    Install-WingetPackageIfMissing -Id "Coursier.Coursier" -Name "Coursier"
    Install-WingetPackageIfMissing -Id "sbt.sbt" -Name "sbt"
} else {
    Log "Skipping Scala/sbt step (--no-scala)."
}

# ---------------------------------------------------------------------------
# 6) Create the project's Python virtual environment
# ---------------------------------------------------------------------------
Log "Creating Python virtual environment (.venv) in $ProjectDir..."
Set-Location $ProjectDir

$venvDir = Join-Path $ProjectDir ".venv"
if (-not (Test-Path $venvDir)) {
    if (Test-Command "python") {
        python -m venv .venv
    } elseif (Test-Command "py") {
        py -3 -m venv .venv
    } else {
        Err "No Python executable found to create .venv."
        exit 1
    }
}

$venvPython = Join-Path $venvDir "Scripts\python.exe"
$venvActivate = Join-Path $venvDir "Scripts\Activate.ps1"

& $venvPython -m pip install --upgrade pip
Install-PipPackageIfMissing -PythonExe $venvPython -PackageName "pyspark==3.5.*" -ImportName "pyspark"
Install-PipPackageIfMissing -PythonExe $venvPython -PackageName "ipykernel"
Install-PipPackageIfMissing -PythonExe $venvPython -PackageName "jupyterlab"
Install-PipPackageIfMissing -PythonExe $venvPython -PackageName "pandas"
Install-PipPackageIfMissing -PythonExe $venvPython -PackageName "pytest"

# ---------------------------------------------------------------------------
# 7) Install VS Code and extensions (optional)
# ---------------------------------------------------------------------------
if ($InstallVSCode) {
    Log "Installing Visual Studio Code..."
    Install-WingetPackageIfMissing -Id "Microsoft.VisualStudioCode" -Name "Visual Studio Code"

    if (Test-Command "code") {
        Log "Installing recommended VS Code extensions..."
        $extensions = @(
            "ms-python.python"
            "ms-python.vscode-pylance"
            "ms-toolsai.jupyter"
            "scalameta.metals"
            "scala-lang.scala"
            "redhat.vscode-yaml"
            "ms-azuretools.vscode-docker"
        )

        foreach ($ext in $extensions) {
            code --install-extension $ext --force | Out-Null
            if ($LASTEXITCODE -ne 0) {
                Warn "Unable to install $ext"
            }
        }
    } else {
        Warn "'code' command not available: extensions were not installed."
        Warn "In VS Code: Ctrl+Shift+P > 'Shell Command: Install code command in PATH'."
    }
} else {
    Log "Skipping VS Code step (--no-vscode)."
}

# ---------------------------------------------------------------------------
# 8) Persist environment variables in PowerShell profile
# ---------------------------------------------------------------------------
Log "Configuring environment variables in PowerShell profile..."
$profilePath = $PROFILE.CurrentUserCurrentHost
$profileDir = Split-Path -Parent $profilePath
if (-not (Test-Path $profileDir)) {
    New-Item -ItemType Directory -Path $profileDir -Force | Out-Null
}
if (-not (Test-Path $profilePath)) {
    New-Item -ItemType File -Path $profilePath -Force | Out-Null
}

$envBlockStart = "# >>> Spark env >>>"
$envBlockEnd = "# <<< Spark env <<<"
$envBlock = @"
# >>> Spark env >>>
`$env:JAVA_HOME = "$JavaHomePath"
`$env:SPARK_HOME = "$SparkHomePath"
if (`$env:Path -notlike "*$JavaHomePath\\bin*") { `$env:Path = "$JavaHomePath\\bin;`$env:Path" }
if (`$env:Path -notlike "*$SparkHomePath\\bin*") { `$env:Path = "$SparkHomePath\\bin;`$env:Path" }
# <<< Spark env <<<
"@

$content = Get-Content $profilePath -Raw
if ($content -match [regex]::Escape($envBlockStart)) {
    $pattern = [regex]::Escape($envBlockStart) + ".*?" + [regex]::Escape($envBlockEnd)
    $updated = [regex]::Replace($content, $pattern, $envBlock, [System.Text.RegularExpressions.RegexOptions]::Singleline)
    Set-Content -Path $profilePath -Value $updated -Encoding UTF8
} else {
    Add-Content -Path $profilePath -Value "`r`n$envBlock"
}

# ---------------------------------------------------------------------------
# 9) Final checks
# ---------------------------------------------------------------------------
Log "Checks:"
try { java -version } catch { Warn "Java not found." }
try {
    if (Test-Command "python") { python --version }
    elseif (Test-Command "py") { py -3 --version }
    else { Warn "Python not found." }
} catch { Warn "Python not found." }

$sparkSubmit = Join-Path $SparkHomePath "bin\spark-submit.cmd"
if (Test-Path $sparkSubmit) {
    try { & $sparkSubmit --version | Select-Object -First 5 } catch { Warn "spark-submit not available." }
} else {
    Warn "spark-submit not available."
}

if ($InstallScala) {
    try { sbt --version } catch { Warn "sbt not found." }
}

# ---------------------------------------------------------------------------
# 10) Load PowerShell profile now and activate virtual environment
# ---------------------------------------------------------------------------
Log "Loading PowerShell profile for this session..."
. $profilePath

Log "Activating Python virtual environment (.venv)..."
if (Test-Path $venvActivate) {
    . $venvActivate
} else {
    Warn "Unable to find activation script at $venvActivate"
}

if (-not $IsDotSourced) {
    Warn "This script was run without dot-sourcing, so env changes do not persist in your current PowerShell session."
    Warn "Run this instead to keep JAVA_HOME/SPARK_HOME/.venv active now: . .\setup_spark_env_windows.ps1"
}

Write-Host @"

============================================================
 Installation completed successfully.

 Next steps:
   1. Open the project in VS Code:
        code "$ProjectDir"
   2. Select the ".venv" Python interpreter in VS Code
      (Ctrl+Shift+P > "Python: Select Interpreter").
   3. Test Spark

 Exported variables:
   JAVA_HOME=$JavaHomePath
   SPARK_HOME=$SparkHomePath
   VIRTUAL_ENV=$venvDir
============================================================
"@
