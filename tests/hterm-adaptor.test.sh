#!/usr/bin/env bash
set -euo pipefail

TEST_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ADAPTOR="$TEST_ROOT/hterm-adaptor/hterm-herdr-sync.sh"
FIXTURES="$TEST_ROOT/tests/fixtures"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT
mkdir -p "$TMP_ROOT/bin" "$TMP_ROOT/state/runs" "$TMP_ROOT/state/inbox" "$TMP_ROOT/state/pool/leases"

printf '%s\n' '{"result":{"agents":[{"agent":"hercules-01","pane_id":"pane-hercules"}]}}' > "$TMP_ROOT/agents.json"
printf '%s\n' '{"status":"PASS"}' > "$TMP_ROOT/state/runs/receipt.json"
touch "$TMP_ROOT/state/inbox/one" "$TMP_ROOT/state/pool/leases/one"

cat > "$TMP_ROOT/bin/tmux" <<'EOF'
#!/usr/bin/env bash
if [[ "$*" == *"hercules-01"* && "${HTERM_TEST_NO_TMUX:-0}" != 1 ]]; then exit 0; fi
exit 1
EOF
cat > "$TMP_ROOT/bin/herdr" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "${HTERM_TEST_LOG}"
if [[ "$1 $2" == "agent list" ]]; then
  printf '%s\n' '{"result":{"agents":[{"agent":"hercules-01","pane_id":"pane-hercules"}]}}'
fi
EOF
chmod +x "$TMP_ROOT/bin/tmux" "$TMP_ROOT/bin/herdr"

passed=0
blocked=0
wrong=0

run_case() {
  local turn="$1" expected="$2"
  : > "$TMP_ROOT/log"
  HTERM_TEST_LOG="$TMP_ROOT/log" HTERM_ADAPTOR_ROOT="$TMP_ROOT" \
    HTERM_MEMBERS_FILE="$FIXTURES/hterm-members.json" \
    HERDR_AGENT_LIST_FILE="$TMP_ROOT/agents.json" PATH="$TMP_ROOT/bin:$PATH" \
    HTERM_ADAPTOR_ONCE=1 MAX_LAUNCHES=18 \
    "$ADAPTOR" >/dev/null
  if grep -q -- "--state $expected" "$TMP_ROOT/log"; then
    passed=$((passed + 1))
    if [[ "$expected" == blocked ]]; then
      blocked=$((blocked + 1))
    fi
  else
    wrong=$((wrong + 1))
  fi
}

for pair in \
  "nobet-notu blocked" \
  "olcum working" \
  "kapanis idle"; do
  set -- $pair
  cp "$FIXTURES/hterm-$1-tail-1.json" "$TMP_ROOT/state/runs/tail-1.json"
  run_case "$1" "$2"
done

: > "$TMP_ROOT/log"
HTERM_TEST_LOG="$TMP_ROOT/log" HTERM_ADAPTOR_ROOT="$TMP_ROOT" \
  HTERM_MEMBERS_FILE="$FIXTURES/hterm-members.json" \
  HERDR_AGENT_LIST_FILE="$TMP_ROOT/agents.json" HTERM_TEST_NO_TMUX=1 \
  PATH="$TMP_ROOT/bin:$PATH" HTERM_ADAPTOR_ONCE=1 \
  "$ADAPTOR" >/dev/null
if grep -q -- '--state unknown' "$TMP_ROOT/log" && grep -q -- 'tmux oturumu yok' "$TMP_ROOT/log"; then
  passed=$((passed + 1))
else
  wrong=$((wrong + 1))
fi

printf 'passed=%s\n' "$passed"
printf 'blocked=%s\n' "$blocked"
printf 'wrong=%s\n' "$wrong"
((wrong == 0))
