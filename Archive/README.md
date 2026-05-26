# Utilities

Bootstrap utilities for setting up Data Engineering environments, plus reusable Azure/Databricks helpers.

## Repository layout

- `shell/` — Bash setup scripts for macOS/Linux.
- `powershell/` — PowerShell setup scripts for Windows.
- `azure/dbx/` — Python utilities for Azure Databricks notebooks.

## Available scripts

- shell/setup_env.sh
	- Full macOS bootstrap (CLI, Python/data, infra/cloud, Spark/Scala).
- shell/setup_spark_env.sh
	- Spark-focused setup for macOS and Debian/Ubuntu Linux.
	- Installs Java 17, Python, Spark, optional Scala/sbt, optional VS Code + extensions.
- powershell/setup_spark_env_windows.ps1
	- Windows PowerShell equivalent of setup_spark_env.sh.

## Available Azure utilities

- azure/dbx/folder_stats.py
	- Databricks utility to recursively inspect an Azure Blob Storage container.
	- Reports size (KB/MB/GB/TB), last modification date, file count and folder count per top-level entry.
	- Returns a Pandas DataFrame sorted by most recent modification.

## Where to place and run scripts

- shell/setup_env.sh:
	- Scope: machine-level bootstrap (global tooling).
	- Can be run from Utilities.
	- Does not install Python dependencies into other project repositories.

- shell/setup_spark_env.sh:
	- Scope: one target project only.
	- It creates and manages `.venv` in the directory where the script is located.
	- Place/copy this script in the root of the target project, then run it there.

Example:

- If your Spark exercises are in `../esgi-spark-core-template`, run `setup_spark_env.sh` from `esgi-spark-core-template`, not from `Utilities`.

## Recommended workflow (3 commands)

```bash
# 1) One-time machine bootstrap (global)
cd ~/personnalWorkspace/Utilities && bash shell/setup_env.sh

# 2) Project-level Spark/PySpark setup (run in target project root)
cd ~/personnalWorkspace/esgi-spark-core-template && bash ./setup_spark_env.sh

# 3) Run exercises with project virtualenv
cd ~/personnalWorkspace/esgi-spark-core-template/exercices && ../.venv/bin/python Seance1.py
```

In VS Code, select the Python interpreter from the target project's `.venv`.

## Script 1: setup_env.sh (macOS full bootstrap)

### What it does

1. Ensures Homebrew is installed and available in shell startup files.
2. Installs grouped toolchains with Homebrew and pipx.
3. Adds PATH and shell integration blocks for bash and zsh.
4. Sets Java 17 and Spark environment variables.
5. Manages a reusable aliases block in shell rc files.
6. Prints a detailed summary (installed, skipped, failed, modified files).

### Usage

```bash
bash shell/setup_env.sh
bash shell/setup_env.sh --quick
bash shell/setup_env.sh --skip-aliases
bash shell/setup_env.sh --quick --skip-aliases
bash shell/setup_env.sh --help
```

### Notes

- Mostly idempotent: already-installed packages are skipped.
- In quick mode, heavy optional installs are skipped.

## Script 2: setup_spark_env.sh (macOS/Linux Spark setup)

### Supported OS

- macOS (Homebrew)
- Debian/Ubuntu Linux (apt)

### What it does

1. Detects OS and package manager.
2. Installs Java 17, Python 3 and Apache Spark.
3. Optionally installs Scala + sbt.
4. Creates local .venv and installs Python packages (pyspark, jupyterlab, pandas, pytest, ipykernel).
5. Optionally installs VS Code and recommended extensions.
6. Persists JAVA_HOME and SPARK_HOME in shell rc files.
7. Tries to source shell config and activate .venv at end of run.

### Usage

```bash
bash shell/setup_spark_env.sh
bash shell/setup_spark_env.sh --no-scala
bash shell/setup_spark_env.sh --no-vscode
bash shell/setup_spark_env.sh --help
```

### Keep environment active in current terminal

To keep JAVA_HOME, SPARK_HOME and .venv active in the current shell, run with source:

```bash
source ./setup_spark_env.sh
```

If executed with bash, the script still installs/configures everything, but environment changes cannot persist to the parent shell.

## Script 3: setup_spark_env_windows.ps1 (Windows Spark setup)

### What it does

1. Uses winget for idempotent package installation.
2. Installs Java 17, Python 3.12, Spark 3.5.1.
3. Optionally installs Scala/sbt and VS Code + extensions.
4. Creates .venv and installs Python Spark/data tooling.
5. Persists JAVA_HOME and SPARK_HOME for user scope.
6. Updates and loads PowerShell profile block.
7. Activates .venv at end of run.

### Usage

```powershell
powershell -ExecutionPolicy Bypass -File .\setup_spark_env_windows.ps1
powershell -ExecutionPolicy Bypass -File .\setup_spark_env_windows.ps1 -NoScala
powershell -ExecutionPolicy Bypass -File .\setup_spark_env_windows.ps1 -NoVSCode
powershell -ExecutionPolicy Bypass -File .\setup_spark_env_windows.ps1 -Help
```

If PowerShell 7 is installed, you can use `pwsh` instead of `powershell`.

### Keep environment active in current PowerShell session

Run with dot-sourcing:

```powershell
. .\setup_spark_env_windows.ps1
```

## Script 4: azure/dbx/folder_stats.py (Databricks blob stats)

### What it does

1. Connects to the active Databricks `SparkSession` and uses `dbutils.fs.ls` to walk a container path.
2. Recursively aggregates total size, latest modification time, file count and folder count.
3. Formats sizes in KB/MB/GB/TB and dates as `dd/mm/YYYY`.
4. Returns a Pandas DataFrame sorted by last modification (most recent first).

### Usage

In a Databricks notebook (Python):

```python
%run ./folder_stats
# or: from folder_stats import blob_ls

df = blob_ls("my-container/path/to/inspect", storage_account="mystorageaccount")
display(df)
```

### Notes

- Requires a Databricks runtime (relies on `dbutils` and `pyspark.dbutils`).
- Uses the `wasbs://` scheme; the storage account must already be configured (key, SAS or passthrough credentials).
- Path format: `<container>/<sub/path>` (no scheme prefix).

## Git ignore defaults

The repository .gitignore includes common patterns for:

- Python caches/build artifacts
- Scala/sbt/Metals/Bloop/BSP artifacts
- Local virtual environment (.venv)
- macOS temporary files