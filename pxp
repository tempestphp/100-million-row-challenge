#!/bin/bash
set -u

# pxp.sh — PXP (Plan eXecution Protocol) executor
# Usage: ./pxp.sh changeset.yml

if [[ $# -ne 1 ]]; then
  printf "Usage: %s <changeset.yml>\n" "$0"
  exit 1
fi

FILE="$1"

if [[ ! -f "$FILE" ]]; then
  printf "Error: File '%s' not found.\n" "$FILE"
  exit 1
fi

if [[ -t 1 ]]; then
  RED='\033[0;31m'
  GREEN='\033[0;32m'
  CYAN='\033[0;36m'
  BOLD='\033[1m'
  RESET='\033[0m'
else
  RED='' GREEN='' CYAN='' BOLD='' RESET=''
fi

NAME=$(grep '^name:' "$FILE" | head -1 | sed 's/^name:[[:space:]]*//')
if [[ -n "$NAME" ]]; then
  printf "${BOLD}${CYAN}▶ Changeset: %s${RESET}\n\n" "$NAME"
fi

MODE=""
STEP_NUM=0
STEP_NAME=""
PATCH_BUF=""
PATCH_INDENT=""
RUN_CMD=""
IN_STEPS=0

flush_patch() {
  if [[ -n "$PATCH_BUF" ]]; then
    local tmpfile tmperr
    tmpfile=$(mktemp)
    tmperr=$(mktemp)
    printf '%s\n' "$PATCH_BUF" > "$tmpfile"
    if ! git apply --check "$tmpfile" 2>"$tmperr"; then
      printf "${RED}  ✗ Patch failed:${RESET}\n"
      sed 's/^/    /' "$tmperr"
      rm -f "$tmpfile" "$tmperr"
      exit 1
    fi
    git apply "$tmpfile"
    rm -f "$tmpfile" "$tmperr"
    printf "${GREEN}  ✓ Done${RESET}\n"
    PATCH_BUF=""
    PATCH_INDENT=""
  fi
}

flush_run() {
  if [[ -n "$RUN_CMD" ]]; then
    eval "$RUN_CMD"
    local rc=$?
    if [[ $rc -ne 0 ]]; then
      printf "${RED}  ✗ Failed (exit %d)${RESET}\n" "$rc"
      exit "$rc"
    fi
    printf "${GREEN}  ✓ Done${RESET}\n"
    RUN_CMD=""
  fi
}

flush() {
  case "$MODE" in
    patch) flush_patch ;;
    run)   flush_run ;;
  esac
  MODE=""
}

while IFS= read -r line || [[ -n "$line" ]]; do
  # Detect steps: section
  if printf '%s' "$line" | grep -qE '^steps:'; then
    IN_STEPS=1
    continue
  fi

  [[ $IN_STEPS -eq 0 ]] && continue

  # New list item "- name:" — starts a new step block, capture name
  if printf '%s' "$line" | grep -qE '^[[:space:]]*-[[:space:]]+name:'; then
    flush
    STEP_NAME=$(printf '%s' "$line" | sed 's/^[[:space:]]*-[[:space:]]*name:[[:space:]]*//')
    continue
  fi

  # Continuation "name:" (no dash) within a step block
  if printf '%s' "$line" | grep -qE '^[[:space:]]+name:' && [[ "$MODE" != "patch" ]]; then
    if ! printf '%s' "$line" | grep -qE '^[[:space:]]*-'; then
      STEP_NAME=$(printf '%s' "$line" | sed 's/^[[:space:]]*name:[[:space:]]*//')
      continue
    fi
  fi

  # Run step: "- run:" or "run:"
  if printf '%s' "$line" | grep -qE '^[[:space:]]*(- )?run:'; then
    flush
    STEP_NUM=$((STEP_NUM + 1))
    RUN_CMD=$(printf '%s' "$line" | sed 's/^[[:space:]]*-\?[[:space:]]*run:[[:space:]]*//')
    MODE="run"
    if [[ -n "$STEP_NAME" ]]; then
      printf "${BOLD}[%d] run:${RESET} %s\n" "$STEP_NUM" "$STEP_NAME"
    else
      printf "${BOLD}[%d] run:${RESET} %s\n" "$STEP_NUM" "$RUN_CMD"
    fi
    STEP_NAME=""
    continue
  fi

  # Patch step: "- patch: |" or "patch: |"
  if printf '%s' "$line" | grep -qE '^[[:space:]]*(- )?patch:[[:space:]]*\|'; then
    flush
    STEP_NUM=$((STEP_NUM + 1))
    MODE="patch"
    PATCH_BUF=""
    PATCH_INDENT=""
    if [[ -n "$STEP_NAME" ]]; then
      printf "${BOLD}[%d] patch:${RESET} %s\n" "$STEP_NUM" "$STEP_NAME"
    else
      printf "${BOLD}[%d] patch:${RESET}\n" "$STEP_NUM"
    fi
    STEP_NAME=""
    continue
  fi

  # Collect patch content
  if [[ "$MODE" == "patch" ]]; then
    # Detect indent from first non-empty patch line
    if [[ -z "$PATCH_INDENT" && -n "$line" ]]; then
      local_stripped="${line#"${line%%[! ]*}"}"
      PATCH_INDENT=$((${#line} - ${#local_stripped}))
    fi

    if [[ -n "$PATCH_INDENT" && "$PATCH_INDENT" -gt 0 ]]; then
      stripped=$(printf '%s' "$line" | sed "s/^[[:space:]]\\{1,${PATCH_INDENT}\\}//")
    else
      stripped="$line"
    fi

    if [[ -n "$PATCH_BUF" ]]; then
      PATCH_BUF="${PATCH_BUF}"$'\n'"${stripped}"
    else
      PATCH_BUF="$stripped"
    fi
  fi
done < "$FILE"

flush

printf "\n${BOLD}${GREEN}✓ All %d steps completed.${RESET}\n" "$STEP_NUM"
