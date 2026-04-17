#!/usr/bin/env bash
# ============================================================
# rdb2graph — Master Orchestrator Shell Script
# ============================================================
# Usage:
#   ./ingest.sh                        Full pipeline
#   ./ingest.sh --config my.yaml       Custom config
#   ./ingest.sh --only er_parse        Single stage
#   ./ingest.sh --skip etl_run         Skip a stage
#   ./ingest.sh --validate             Check connections only
#   ./ingest.sh --install              Install Python deps
# ============================================================

set -euo pipefail

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

log_info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*"; }
log_section() { echo -e "\n${BOLD}${BLUE}══ $* ══${NC}"; }

# ── Defaults ──────────────────────────────────────────────────────────────────
CONFIG="config.yaml"
ONLY_STAGE=""
SKIP_STAGE=""
VALIDATE=false
INSTALL=false
VENV_DIR=".venv"

# ── Arg parse ─────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        -c|--config)   CONFIG="$2"; shift 2 ;;
        --only)        ONLY_STAGE="$2"; shift 2 ;;
        --skip)        SKIP_STAGE="$2"; shift 2 ;;
        --validate)    VALIDATE=true; shift ;;
        --install)     INSTALL=true; shift ;;
        -h|--help)
            grep '^#' "$0" | cut -c3-
            exit 0
            ;;
        *) log_error "Unknown arg: $1"; exit 1 ;;
    esac
done

# ── Banner ────────────────────────────────────────────────────────────────────
echo -e "${BOLD}${CYAN}"
cat << 'EOF'
  ____  ____  ____ ____   ____  ____  ____  ____  _  _
 |  _ \|  _ \| __ )___ \ / ___|  _ \|  _ \|  _ \| || |
 | |_) | | | |  _ \ __) | |  _| |_) | |_) | |_) | || |_
 |  _ <| |_| | |_) / __/| |_| |  _ <|  __/|  __/|__   _|
 |_| \_\____/|____/_____|\____|_| \_\_|   |_|      |_|

  Relational DB + ER Diagram → Neo4j Knowledge Graph
EOF
echo -e "${NC}"

# ── Install ───────────────────────────────────────────────────────────────────
if [ "$INSTALL" = true ]; then
    log_section "Installing Dependencies"
    
    # Check Python
    python_cmd=$(command -v python3 || command -v python || echo "")
    if [ -z "$python_cmd" ]; then
        log_error "Python 3.8+ required but not found"
        exit 1
    fi
    log_info "Python: $($python_cmd --version)"

    # Create venv if not exists
    if [ ! -d "$VENV_DIR" ]; then
        log_info "Creating virtualenv at $VENV_DIR ..."
        $python_cmd -m venv "$VENV_DIR"
    fi
    
    source "$VENV_DIR/bin/activate"
    pip install --upgrade pip -q
    pip install -r requirements.txt -q
    log_info "✓ Dependencies installed"
    exit 0
fi

# ── Check Python env ──────────────────────────────────────────────────────────
log_section "Environment Check"

if [ -d "$VENV_DIR" ]; then
    source "$VENV_DIR/bin/activate"
    log_info "Using virtualenv: $VENV_DIR"
fi

python_cmd=$(command -v python3 || command -v python || echo "")
if [ -z "$python_cmd" ]; then
    log_error "Python not found. Run: ./ingest.sh --install"
    exit 1
fi
log_info "Python: $($python_cmd --version)"

# Check config
if [ ! -f "$CONFIG" ]; then
    log_error "Config not found: $CONFIG"
    exit 1
fi
log_info "Config: $CONFIG"

# Check neo4j-etl JAR (optional)
JAR_PATH=$(python3 -c "import yaml; c=yaml.safe_load(open('$CONFIG')); print(c.get('neo4j_etl',{}).get('jar_path','./neo4j-etl/neo4j-etl.jar'))" 2>/dev/null || echo "./neo4j-etl/neo4j-etl.jar")
if [ -f "$JAR_PATH" ]; then
    log_info "neo4j-etl JAR: $JAR_PATH ✓"
else
    log_warn "neo4j-etl JAR not found at $JAR_PATH — will use Python direct loader"
fi

# ── Validate mode ─────────────────────────────────────────────────────────────
if [ "$VALIDATE" = true ]; then
    log_section "Validating Connections"
    cd src && $python_cmd main.py -c "../$CONFIG" --validate
    exit $?
fi

# ── Build Python args ─────────────────────────────────────────────────────────
PYTHON_ARGS="-c ../$CONFIG"
if [ -n "$ONLY_STAGE" ]; then
    PYTHON_ARGS="$PYTHON_ARGS --stages $ONLY_STAGE"
    log_info "Running single stage: $ONLY_STAGE"
fi
if [ -n "$SKIP_STAGE" ]; then
    PYTHON_ARGS="$PYTHON_ARGS --skip $SKIP_STAGE"
    log_info "Skipping stage: $SKIP_STAGE"
fi

# ── Pre-flight ────────────────────────────────────────────────────────────────
log_section "Pre-flight"
mkdir -p mappings logs

# Check ER file
ER_PATH=$(python3 -c "import yaml; c=yaml.safe_load(open('$CONFIG')); print(c['er_diagram']['path'])" 2>/dev/null || echo "")
if [ -n "$ER_PATH" ] && [ ! -f "$ER_PATH" ]; then
    log_warn "ER diagram not found at: $ER_PATH"
    log_warn "Set the correct path in config.yaml → er_diagram.path"
fi

# ── Run pipeline ──────────────────────────────────────────────────────────────
log_section "Running Pipeline"

START_TIME=$(date +%s)
cd src

if $python_cmd main.py $PYTHON_ARGS 2>&1 | tee "../logs/pipeline_$(date +%Y%m%d_%H%M%S).log"; then
    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))
    echo ""
    log_info "✓ Pipeline completed in ${ELAPSED}s"
    echo -e "${GREEN}${BOLD}"
    echo "  ┌─────────────────────────────────────────────┐"
    echo "  │  Knowledge Graph ready in Neo4j!            │"
    echo "  │  Open Neo4j Browser → http://localhost:7474 │"
    echo "  │  Try: MATCH (n) RETURN n LIMIT 25           │"
    echo "  └─────────────────────────────────────────────┘"
    echo -e "${NC}"
else
    log_error "Pipeline failed — check logs/ for details"
    exit 1
fi
