#!/bin/bash
#
# Validate all parquet files in test-files-wild directory
# Usage: ./validate-wild.sh [--all] [--verbose] [directory]
#
# Options:
#   --all, -a      Show all files (default: only unexpected results)
#   --verbose, -v  Show detailed output for each file
#
# Exit codes:
#   0 - All results match expectations
#   1 - Regressions or unexpected results detected
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
WILD_DIR="$PROJECT_ROOT/test-files-wild"
PQ_BIN="$SCRIPT_DIR/zig-out/bin/pqi"
EXPECTED_FAILURES="$WILD_DIR/expected-failures.txt"

VERBOSE=false
SHOW_ALL=false
for arg in "$@"; do
    case "$arg" in
        --verbose|-v) VERBOSE=true ;;
        --all|-a) SHOW_ALL=true ;;
        -*) echo "Unknown option: $arg"; exit 1 ;;
        *) WILD_DIR="$arg" ;;
    esac
done

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# Returns 0 if the file is in expected-failures.txt, 1 otherwise.
# Prints the expected error pattern on match.
is_expected_failure() {
    local rel_path="$1"
    if [[ -f "$EXPECTED_FAILURES" ]]; then
        local match
        match=$(grep -v '^#' "$EXPECTED_FAILURES" | grep -v '^$' | awk -v p="$rel_path" '$1 == p {print $2; exit}')
        if [[ -n "$match" ]]; then
            echo "$match"
            return 0
        fi
    fi
    return 1
}

extract_error() {
    local output="$1"
    local err=""
    err=$(echo "$output" | grep -oE "FAILED \([A-Za-z]+\)" | head -1 || true)
    if [[ -z "$err" ]]; then
        err=$(echo "$output" | grep -oE "panic: [^$]+" | head -1 | cut -c1-60 || true)
    fi
    if [[ -z "$err" ]]; then
        err=$(echo "$output" | grep -E "Segmentation fault" | head -1 || true)
    fi
    if [[ -z "$err" ]]; then
        err=$(echo "$output" | grep -v -E "^(Validating:|  Magic|  Metadata:|  Row group)" | grep -v "^$" | head -1 | cut -c1-80 || true)
    fi
    echo "${err:-unknown error}"
}

if [[ ! -x "$PQ_BIN" ]]; then
    echo "Building pqi..."
    (cd "$SCRIPT_DIR" && zig build)
fi

if [[ ! -d "$WILD_DIR" ]]; then
    echo -e "${RED}Error: Directory not found: $WILD_DIR${NC}"
    exit 1
fi

FILES=$(find "$WILD_DIR" -name "*.parquet" -type f 2>/dev/null | sort)

if [[ -z "$FILES" ]]; then
    echo -e "${YELLOW}No parquet files found in $WILD_DIR${NC}"
    exit 0
fi

EXPECTED_FAIL_COUNT=0
if [[ -f "$EXPECTED_FAILURES" ]]; then
    EXPECTED_FAIL_COUNT=$(grep -v '^#' "$EXPECTED_FAILURES" | grep -v '^$' | wc -l | tr -d ' ')
fi

FILE_COUNT=$(echo "$FILES" | wc -l | tr -d ' ')
echo "Validating $FILE_COUNT parquet files ($EXPECTED_FAIL_COUNT expected failures)"
echo "=============================================="
echo ""

PASS=0
EXPECTED_FAIL=0
REGRESSION_COUNT=0
FIX_COUNT=0

# Temp files for tracking unexpected results
REGRESSIONS_FILE=$(mktemp)
FIXES_FILE=$(mktemp)
trap "rm -f $REGRESSIONS_FILE $FIXES_FILE" EXIT

while IFS= read -r file; do
    rel_path="${file#$WILD_DIR/}"
    display_path="test-files-wild/$rel_path"

    expected_pattern=""
    if ep=$(is_expected_failure "$rel_path"); then
        expected_pattern="$ep"
    fi

    if output=$("$PQ_BIN" validate "$file" 2>&1); then
        if [[ -n "$expected_pattern" ]]; then
            FIX_COUNT=$((FIX_COUNT + 1))
            echo "$display_path" >> "$FIXES_FILE"
            echo -e "${CYAN}!${NC} $display_path"
            echo -e "    ${CYAN}→ UNEXPECTED FIX${NC} (expected $expected_pattern)"
        else
            PASS=$((PASS + 1))
            if $VERBOSE || $SHOW_ALL; then
                echo -e "${GREEN}✓${NC} $display_path"
            fi
        fi
    else
        error_line=$(extract_error "$output")

        if [[ -n "$expected_pattern" ]]; then
            EXPECTED_FAIL=$((EXPECTED_FAIL + 1))
            if $VERBOSE; then
                echo -e "${YELLOW}✗${NC} $display_path (expected)"
                echo -e "    ${YELLOW}→${NC} $error_line"
            elif $SHOW_ALL; then
                echo -e "${YELLOW}✗${NC} $display_path (expected: $expected_pattern)"
            fi
        else
            REGRESSION_COUNT=$((REGRESSION_COUNT + 1))
            echo "$display_path	$error_line" >> "$REGRESSIONS_FILE"
            echo -e "${RED}✗${NC} $display_path"
            echo -e "    ${RED}→ REGRESSION:${NC} $error_line"
            if $VERBOSE; then
                echo "$output" | head -5 | sed 's/^/    /'
            fi
        fi
    fi
done <<< "$FILES"

echo ""
echo "=============================================="
echo -e "Results: ${GREEN}$PASS passed${NC}, ${YELLOW}$EXPECTED_FAIL expected failures${NC}"

HAS_PROBLEMS=false

if [[ $REGRESSION_COUNT -gt 0 ]]; then
    HAS_PROBLEMS=true
    echo ""
    echo -e "${RED}REGRESSIONS ($REGRESSION_COUNT):${NC}"
    while IFS=$'\t' read -r rfile reason; do
        echo -e "  ${RED}✗${NC} $rfile"
        echo "    → $reason"
    done < "$REGRESSIONS_FILE"
fi

if [[ $FIX_COUNT -gt 0 ]]; then
    HAS_PROBLEMS=true
    echo ""
    echo -e "${CYAN}UNEXPECTED FIXES ($FIX_COUNT):${NC}"
    echo "  Remove from expected-failures.txt:"
    while IFS= read -r ffile; do
        echo -e "  ${CYAN}!${NC} $ffile"
    done < "$FIXES_FILE"
fi

if $HAS_PROBLEMS; then
    exit 1
fi

exit 0
