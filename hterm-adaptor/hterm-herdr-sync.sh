#!/usr/bin/env bash
set -euo pipefail

# Synchronise hterm's logical members into an existing Herdr session.  The
# command boundaries are overridable so the same adaptor can be exercised by
# a fixture without a running hterm, tmux, or Herdr server.
ROOT_DIR="${HTERM_ADAPTOR_ROOT:-/root/ocak}"
MAP_FILE="${HTERM_ADAPTOR_MAP:-$(dirname "$0")/tur-durum.json}"
HERDR_BIN="${HERDR_BIN_PATH:-herdr}"
WORKSPACE_ID="${HERDR_WORKSPACE_ID:-default}"
SOURCE="${HTERM_ADAPTOR_SOURCE:-hterm-adaptor}"
RUN_ONCE="${HTERM_ADAPTOR_ONCE:-0}"
TMUX_ATTACH_CMD="${TMUX_ATTACH_CMD:-tmux attach -r -t \"\$1\"}"

members_json() {
  if [[ -n "${HTERM_MEMBERS_FILE:-}" ]]; then
    jq -c '.' "$HTERM_MEMBERS_FILE"
  elif [[ -n "${HTERM_MEMBERS_CMD:-}" ]]; then
    bash -c "$HTERM_MEMBERS_CMD"
  else
    hterm --terminaller logical_members --format json
  fi
}

agent_list_json() {
  if [[ -n "${HERDR_AGENT_LIST_FILE:-}" ]]; then
    jq -c '.' "$HERDR_AGENT_LIST_FILE"
  elif [[ -n "${HERDR_AGENT_LIST_CMD:-}" ]]; then
    bash -c "$HERDR_AGENT_LIST_CMD"
  else
    "$HERDR_BIN" agent list
  fi
}

member_names() {
  jq -r '(.logical_members // .members // .) | .[] | if type == "string" then . else (.name // .agent // .member) end'
}

agent_pane() {
  local name="$1"
  agent_list_json | jq -r --arg name "$name" '
    (.result.agents // .agents // .result // [])[] |
    select((.agent // .name // .label) == $name) | (.pane_id // .pane // .id // empty)' | head -n 1
}

turn_file() {
  find "${ROOT_DIR}/state/runs" -type f -name '*tail-1*' -o -type f -name '*.json' 2>/dev/null |
    while IFS= read -r file; do [[ -f "$file" ]] && printf '%s\t%s\n' "$(stat -c %Y "$file" 2>/dev/null || stat -f %m "$file")" "$file"; done |
    sort -nr | head -n 1 | cut -f2-
}

turn_value() {
  local file="$1"
  [[ -n "$file" && -f "$file" ]] || { printf '%s\n' unknown; return; }
  jq -r '(.turn // .round // .kind // .label // .memory // empty)' "$file" 2>/dev/null || sed -n '1p' "$file"
}

state_for_turn() {
  jq -r --arg turn "$1" '.states[$turn] // .default_state // "unknown"' "$MAP_FILE"
}

memory_summary() {
  local file="$1"
  [[ -n "$file" && -f "$file" ]] || return 0
  jq -r '(.memory // .summary // .message // empty)' "$file" 2>/dev/null | tr '\n' ' ' | cut -c1-72
}

report_member() {
  local member="$1" turn="$2" state="$3" pane_id="$4" summary="$5" now="$6"
  [[ -n "$pane_id" ]] || return 0
  "$HERDR_BIN" pane report-agent "$pane_id" --source "$SOURCE" --agent "$member" \
    --state "$state" --message "${summary:-tmux oturumu yok}" --seq "$now" >/dev/null
  "$HERDR_BIN" pane report-metadata "$pane_id" --source "$SOURCE" --agent "$member" \
    --title "$member · $turn · $now" --token "summary=${summary:-unknown}" --seq "$now" >/dev/null
}

sync_once() {
  local members turn_file_path turn state now
  members="$(members_json)"
  turn_file_path="$(turn_file)"
  turn="$(turn_value "$turn_file_path")"
  state="$(state_for_turn "$turn")"
  now="$(date +%s)"

  while IFS= read -r member; do
    [[ -n "$member" ]] || continue
    local pane_id member_state
    member_state="$state"
    pane_id="$(agent_pane "$member")"
    if ! tmux has-session -t "$member" 2>/dev/null; then
      member_state=unknown
      [[ -n "$pane_id" ]] && "$HERDR_BIN" pane report-agent "$pane_id" --source "$SOURCE" --agent "$member" \
        --state unknown --message "tmux oturumu yok" --seq "$now" >/dev/null || true
      continue
    fi
    if [[ "${HTERM_ATTACH:-0}" == 1 ]]; then
      bash -c "$TMUX_ATTACH_CMD" -- "$member" >/dev/null 2>&1 || true
    fi
    report_member "$member" "$turn" "$member_state" "$pane_id" "$(memory_summary "$turn_file_path")" "$now"
  done < <(printf '%s\n' "$members" | member_names)

  local inbox leases max_launches
  inbox="$(find "${ROOT_DIR}/state/inbox" -type f 2>/dev/null | wc -l | tr -d ' ')"
  leases="$(find "${ROOT_DIR}/state/pool/leases" -type f 2>/dev/null | wc -l | tr -d ' ')"
  max_launches="${MAX_LAUNCHES:-$(cat "${ROOT_DIR}/state/pool/MAX_LAUNCHES" 2>/dev/null || printf 'unknown')}"
  "$HERDR_BIN" workspace report-metadata "$WORKSPACE_ID" --source "$SOURCE" \
    --token "inbox=$inbox" --token "leases=$leases" --token "slots=$max_launches" --seq "$now" >/dev/null

  local receipt receipt_status receipt_age
  receipt="$(find "${ROOT_DIR}/state/runs" -type f 2>/dev/null | while IFS= read -r file; do [[ -f "$file" ]] && printf '%s\t%s\n' "$(stat -c %Y "$file" 2>/dev/null || stat -f %m "$file")" "$file"; done | sort -nr | head -n 1 | cut -f2-)"
  receipt_status="$(jq -r '.status // .result // empty' "$receipt" 2>/dev/null || true)"
  receipt_age=999999
  if [[ -n "$receipt" ]]; then
    receipt_age=$((now - $(stat -c %Y "$receipt" 2>/dev/null || stat -f %m "$receipt")))
  fi
  if [[ "$receipt_status" == PASS && "$receipt_age" -le 60 ]]; then
    "$HERDR_BIN" notification show "PASS" --body "hterm receipt ${receipt_age}s önce" --sound done >/dev/null
  elif [[ "$receipt_status" == BLOCKED ]]; then
    "$HERDR_BIN" notification show "BLOCKED" --body "$(jq -r '.error_class // .message // "unknown"' "$receipt")" --sound request >/dev/null
  fi
}

while :; do
  sync_once
  [[ "$RUN_ONCE" == 1 ]] && break
  sleep "${HTERM_ADAPTOR_INTERVAL:-5}"
done
