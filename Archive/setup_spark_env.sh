#!/usr/bin/env bash
# =============================================================================
# setup_student_env.sh
#
# Automatic installation script to configure a data engineering machine
# for Spark Core usage (PySpark + Scala) in VS Code.
#
# Compatible with macOS (Homebrew) and Debian/Ubuntu Linux (apt).
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
#   bash setup_spark_env.sh
#   bash setup_spark_env.sh --no-scala     # skip Scala/sbt
#   bash setup_spark_env.sh --no-vscode    # skip VS Code and its extensions
#   bash setup_spark_env.sh --help
#
# Important:
#   This script configures PROJECT_DIR = directory where this script is located.
#   Place it in the root of the target project (or copy it there) before running it.
# =============================================================================

set -euo pipefail

INSTALL_SCALA=true
INSTALL_VSCODE=true

for arg in "$@"; do
    case "$arg" in
        --no-scala)   INSTALL_SCALA=false ;;
        --no-vscode)  INSTALL_VSCODE=false ;;
        -h|--help)
            sed -n '2,30p' "$0"
            exit 0
            ;;
        *)
            echo "Unknown option: $arg"
            echo "Use --help to see available options."
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { printf "\n\033[1;34m[INFO]\033[0m %s\n" "$*"; }
warn() { printf "\n\033[1;33m[WARN]\033[0m %s\n" "$*"; }
err()  { printf "\n\033[1;31m[ERROR]\033[0m %s\n" "$*" >&2; }

install_brew_formula_if_missing() {
    local formula="$1"
    if brew list --formula "$formula" >/dev/null 2>&1; then
        log "brew package '$formula' already installed, skipping."
    else
        log "Installing brew package '$formula'..."
        brew install "$formula"
    fi
}

install_apt_package_if_missing() {
    local package="$1"
    if dpkg -s "$package" >/dev/null 2>&1; then
        log "apt package '$package' already installed, skipping."
    else
        log "Installing apt package '$package'..."
        sudo apt-get install -y "$package"
    fi
}

install_pip_package_if_missing() {
    local package="$1"
    if pip show "$package" >/dev/null 2>&1; then
        log "pip package '$package' already installed in .venv, skipping."
    else
        log "Installing pip package '$package' in .venv..."
        pip install "$package"
    fi
}

OS="$(uname -s)"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

log "Target project directory: $PROJECT_DIR"
log "This script installs .venv and PySpark for this directory only."
log "If your exercises are in another repository, place/copy this script in that repository root first."

# ---------------------------------------------------------------------------
# 1) Detect and prepare the package manager
# ---------------------------------------------------------------------------
log "Detecting operating system: $OS"

PKG=""
if [[ "$OS" == "Darwin" ]]; then
    PKG="brew"
    if ! command -v brew >/dev/null 2>&1; then
        log "Homebrew not found. Installing Homebrew..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi
    if   [[ -x /opt/homebrew/bin/brew ]]; then eval "$(/opt/homebrew/bin/brew shellenv)"
    elif [[ -x /usr/local/bin/brew    ]]; then eval "$(/usr/local/bin/brew shellenv)"
    fi
elif [[ "$OS" == "Linux" ]]; then
    if command -v apt-get >/dev/null 2>&1; then
        PKG="apt"
        log "Updating apt packages..."
        sudo apt-get update -y
    else
        err "Linux distribution not supported automatically (apt required)."
        err "Install manually: openjdk-17, python3, python3-venv, spark, scala, sbt."
        exit 1
    fi
else
    err "Unsupported system: $OS"
    exit 1
fi

# ---------------------------------------------------------------------------
# 2) Install Java 17
# ---------------------------------------------------------------------------
log "Installing Java 17 (required by Spark)..."
if [[ "$PKG" == "brew" ]]; then
    install_brew_formula_if_missing "openjdk@17"
    JAVA_HOME_PATH="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
else
    install_apt_package_if_missing "openjdk-17-jdk"
    JAVA_HOME_PATH="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
fi
export JAVA_HOME="$JAVA_HOME_PATH"
export PATH="$JAVA_HOME/bin:$PATH"

# ---------------------------------------------------------------------------
# 3) Install Python 3
# ---------------------------------------------------------------------------
log "Installing Python 3..."
if [[ "$PKG" == "brew" ]]; then
    install_brew_formula_if_missing "python@3.12"
else
    install_apt_package_if_missing "python3"
    install_apt_package_if_missing "python3-pip"
    install_apt_package_if_missing "python3-venv"
fi

# ---------------------------------------------------------------------------
# 4) Install Apache Spark
# ---------------------------------------------------------------------------
log "Installing Apache Spark..."
if [[ "$PKG" == "brew" ]]; then
    install_brew_formula_if_missing "apache-spark"
    SPARK_HOME_PATH="$(brew --prefix)/opt/apache-spark/libexec"
else
    SPARK_VERSION="3.5.1"
    HADOOP_VERSION="3"
    SPARK_PKG="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
    SPARK_HOME_PATH="/opt/${SPARK_PKG}"
    if [[ ! -d "$SPARK_HOME_PATH" ]]; then
        log "Downloading Spark ${SPARK_VERSION}..."
        TMP_TGZ="/tmp/${SPARK_PKG}.tgz"
        curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PKG}.tgz" -o "$TMP_TGZ"
        sudo tar -xzf "$TMP_TGZ" -C /opt/
        rm -f "$TMP_TGZ"
    fi
fi
export SPARK_HOME="$SPARK_HOME_PATH"
export PATH="$SPARK_HOME/bin:$PATH"

# ---------------------------------------------------------------------------
# 5) Install Scala + sbt (optional)
# ---------------------------------------------------------------------------
if [[ "$INSTALL_SCALA" == true ]]; then
    log "Installing Scala and sbt..."
    if [[ "$PKG" == "brew" ]]; then
        install_brew_formula_if_missing "scala"
        install_brew_formula_if_missing "sbt"
    else
        install_apt_package_if_missing "scala"
        if ! command -v sbt >/dev/null 2>&1; then
            echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" \
                | sudo tee /etc/apt/sources.list.d/sbt.list
            curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" \
                | sudo gpg --dearmor -o /usr/share/keyrings/sbt.gpg
            sudo apt-get update -y
            sudo apt-get install -y sbt
        fi
    fi
else
    log "Skipping Scala/sbt step (--no-scala)."
fi

# ---------------------------------------------------------------------------
# 6) Create the project's Python virtual environment
# ---------------------------------------------------------------------------
log "Creating Python virtual environment (.venv) in $PROJECT_DIR..."
cd "$PROJECT_DIR"
if [[ ! -d ".venv" ]]; then
    python3 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate
pip install --upgrade pip
install_pip_package_if_missing "pyspark"
install_pip_package_if_missing "ipykernel"
install_pip_package_if_missing "jupyterlab"
install_pip_package_if_missing "pandas"
install_pip_package_if_missing "pytest"
deactivate

# ---------------------------------------------------------------------------
# 7) Install VS Code and extensions (optional)
# ---------------------------------------------------------------------------
if [[ "$INSTALL_VSCODE" == true ]]; then
    log "Installing Visual Studio Code..."
    if [[ "$PKG" == "brew" ]]; then
        if ! command -v code >/dev/null 2>&1; then
            brew install --cask visual-studio-code || warn "Failed to install VS Code via brew."
        fi
    else
        if ! command -v code >/dev/null 2>&1; then
            install_apt_package_if_missing "wget"
            install_apt_package_if_missing "gpg"
            install_apt_package_if_missing "apt-transport-https"
            wget -qO- https://packages.microsoft.com/keys/microsoft.asc \
                | gpg --dearmor > /tmp/packages.microsoft.gpg
            sudo install -D -o root -g root -m 644 /tmp/packages.microsoft.gpg \
                /etc/apt/keyrings/packages.microsoft.gpg
            echo "deb [arch=amd64,arm64,armhf signed-by=/etc/apt/keyrings/packages.microsoft.gpg] https://packages.microsoft.com/repos/code stable main" \
                | sudo tee /etc/apt/sources.list.d/vscode.list >/dev/null
            rm -f /tmp/packages.microsoft.gpg
            sudo apt-get update -y
            sudo apt-get install -y code
        fi
    fi

    if command -v code >/dev/null 2>&1; then
        log "Installing recommended VS Code extensions..."
        EXTENSIONS=(
            ms-python.python
            ms-python.vscode-pylance
            ms-toolsai.jupyter
            scalameta.metals
            scala-lang.scala
            redhat.vscode-yaml
            ms-azuretools.vscode-docker
        )
        for ext in "${EXTENSIONS[@]}"; do
            code --install-extension "$ext" --force || warn "Unable to install $ext"
        done
    else
        warn "'code' command not available: extensions were not installed."
        warn "On macOS: VS Code > Cmd+Shift+P > 'Shell Command: Install code in PATH'."
    fi
else
    log "Skipping VS Code step (--no-vscode)."
fi

# ---------------------------------------------------------------------------
# 8) Persist environment variables in the shell
# ---------------------------------------------------------------------------
log "Configuring environment variables (JAVA_HOME, SPARK_HOME)..."

ENV_BLOCK="$(cat <<EOF
# >>> Spark env >>>
export JAVA_HOME="$JAVA_HOME_PATH"
export SPARK_HOME="$SPARK_HOME_PATH"
export PATH="\$JAVA_HOME/bin:\$SPARK_HOME/bin:\$PATH"
# <<< Spark env <<<
EOF
)"

for rc in "$HOME/.zshrc" "$HOME/.bashrc" "$HOME/.bash_profile"; do
    [[ -f "$rc" ]] || touch "$rc"
    if grep -q "# >>> Spark env >>>" "$rc"; then
        # Replace the existing block
        awk -v block="$ENV_BLOCK" '
            /# >>> Spark env >>>/ {print block; skip=1; next}
            /# <<< Spark env <<</ {skip=0; next}
            !skip {print}
        ' "$rc" > "$rc.tmp" && mv "$rc.tmp" "$rc"
    else
        printf "\n%s\n" "$ENV_BLOCK" >> "$rc"
    fi
done

# ---------------------------------------------------------------------------
# 9) Final checks
# ---------------------------------------------------------------------------
log "Checks:"
java -version || warn "Java not found."
python3 --version || warn "Python not found."
"$SPARK_HOME/bin/spark-submit" --version 2>&1 | head -n 5 || warn "spark-submit not available."
if [[ "$INSTALL_SCALA" == true ]]; then
    scala -version 2>&1 || warn "Scala not found."
fi

# ---------------------------------------------------------------------------
# 10) Load shell config now and activate virtual environment
# ---------------------------------------------------------------------------
log "Loading shell configuration for this session..."
if [[ -n "${ZSH_VERSION:-}" ]]; then
    if [[ -f "$HOME/.zshrc" ]]; then
        # shellcheck disable=SC1090
        source "$HOME/.zshrc"
    fi
elif [[ -n "${BASH_VERSION:-}" ]]; then
    if [[ -f "$HOME/.bashrc" ]]; then
        # shellcheck disable=SC1090
        source "$HOME/.bashrc"
    elif [[ -f "$HOME/.bash_profile" ]]; then
        # shellcheck disable=SC1090
        source "$HOME/.bash_profile"
    fi
fi

log "Activating Python virtual environment (.venv)..."
# shellcheck disable=SC1091
source "$PROJECT_DIR/.venv/bin/activate"

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    warn "This script was run with 'bash', so env changes do not persist in your current terminal."
    warn "Run this instead to keep JAVA_HOME/SPARK_HOME/.venv active now: source ./setup_spark_env.sh"
fi

cat <<EOF

============================================================
 Installation completed successfully.

 Next steps:
   1. Open the project in VS Code:
        code "$PROJECT_DIR"
   2. Select the ".venv" Python interpreter in VS Code
      (Cmd/Ctrl+Shift+P > "Python: Select Interpreter").
   3. Test Spark

 Exported variables:
   JAVA_HOME=$JAVA_HOME_PATH
   SPARK_HOME=$SPARK_HOME_PATH
   VIRTUAL_ENV=${VIRTUAL_ENV:-$PROJECT_DIR/.venv}
============================================================
EOF