#!/usr/bin/env bash
# Release gate for one exact tuple of kagent, ADK/MCP SDK, Substrate, and
# agentgateway versions. Run it against a cluster with the probe fixtures
# installed (see README.md) before declaring that tuple supported.
#
# Every check prints PASS or FAIL and fails closed: evidence it cannot
# observe is a FAIL. The script exits non-zero on any FAIL.
set -uo pipefail

: "${KUBECONFIG:?set KUBECONFIG to the test cluster}"
ORKA=${ORKA:-./orka.sh}                 # orka CLI already bound to the server and a token
ORKA_NS=${ORKA_NS:-orka-system}
KAGENT_NS=${KAGENT_NS:-kagent}
PROBE_NS=${PROBE_NS:-probe}
OBSERVED_AGENT=${OBSERVED_AGENT:-probe}
GOVERNED_AGENT=${GOVERNED_AGENT:-probe-gov}
GOVERNED_TOOL=${GOVERNED_TOOL:-probe-orka}
GOVERNED_KAGENT_AGENT=${GOVERNED_KAGENT_AGENT:-probe-gov}
RELAY_SERVICE=${RELAY_SERVICE:-probe-kagent-relay}
RELAY_SERVER=${RELAY_SERVER:-orka-relay}        # the governed Agent's RemoteMCPServer
CPFILTER_SERVICE=${CPFILTER_SERVICE:-probe-cpfilter}
CPFILTER_URL=${CPFILTER_URL:-http://$CPFILTER_SERVICE.$PROBE_NS.svc:8083}
KAGENT_CONTROLLER_CONFIG=${KAGENT_CONTROLLER_CONFIG:-kagent-controller}
PG_DEPLOY=${PG_DEPLOY:-kagent-postgresql}
# Provider hosts kagent always allows; accepted in the governance plan.
PROVIDER_DEFAULT_HOSTS=${PROVIDER_DEFAULT_HOSTS:-https://api.openai.com:443}
LONG=${LONG:-0}                         # 1 also runs the 10-minute approval-expiry check
RUN=${RUN:-gate-$(date +%s)}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"; [[ -n ${FORWARD_PID:-} ]] && kill "$FORWARD_PID" 2>/dev/null' EXIT

failures=0
pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s: %s\n' "$1" "$2"; failures=$((failures + 1)); }

pg() { kubectl -n "$KAGENT_NS" exec -i "deploy/$PG_DEPLOY" -c postgresql -- \
  sh -c 'psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-kagent}" -At -F " | "'; }

# forward <namespace> <target> <remote-port> starts a port-forward on a free
# local port and waits until this kubectl reports it bound. It sets
# FORWARD_PORT and FORWARD_PID, and fails if the forward did not come up.
forward() {
  FORWARD_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
  kubectl -n "$1" port-forward "$2" "$FORWARD_PORT:$3" >"$WORK/forward.log" 2>&1 &
  FORWARD_PID=$!
  for _ in $(seq 1 40); do
    grep -q "Forwarding from 127.0.0.1:$FORWARD_PORT" "$WORK/forward.log" && kill -0 "$FORWARD_PID" 2>/dev/null && return 0
    kill -0 "$FORWARD_PID" 2>/dev/null || return 1
    sleep 0.25
  done
  return 1
}

stop_forward() { kill "$FORWARD_PID" 2>/dev/null; wait "$FORWARD_PID" 2>/dev/null; FORWARD_PID=""; }

# http_code prints curl's status code, or "000" when no response arrived.
http_code() {
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$@") || code=000
  printf '%s' "$code"
}

probe_sleep() {
  forward "$PROBE_NS" deploy/probe-mcp 8080 || return 1
  local reply; reply=$(curl -s --max-time 10 "http://127.0.0.1:$FORWARD_PORT/admin/sleep?s=$1")
  stop_forward
  [[ $reply == "sleep=$1" ]]
}

task_phase() { kubectl -n "$ORKA_NS" get tasks.core.orka.ai "$1" -o jsonpath='{.status.phase}' 2>/dev/null; }

wait_terminal() {
  local phase=""
  for _ in $(seq 1 "${2:-120}"); do
    phase=$(task_phase "$1")
    case "$phase" in Succeeded|Failed|Cancelled|TimedOut) break ;; esac
    sleep 5
  done
  printf '%s' "$phase"
}

create_task() { # create_task <name> <agent> <timeout> <allowed-tools-json>
  kubectl apply -f - >/dev/null <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata: {name: $1, namespace: $ORKA_NS}
spec:
  type: agent
  agentRef: {name: $2}
  timeout: $3
  workspace: {intent: read}
  agentRuntime: {allowedTools: $4}
  prompt: Call your tool once and report what it returns.
YAML
}

# events prints the Task's event types, space-separated.
events() { $ORKA task events "$1" 2>/dev/null | awk 'NR > 1 {print $2}' | tr '\n' ' '; }

wait_tool_started() {
  for _ in $(seq 1 60); do
    [[ $(events "$1") == *ToolCallStarted* ]] && return 0
    sleep 2
  done
  return 1
}

# cancelled_tool_user prints the kagent user whose probe tool call started
# after a time, only if that same user's call was then cancelled. kagent
# user IDs are per Orka session, so this ties the cancellation to the call
# under test.
cancelled_tool_user() {
  local logs user
  logs=$(kubectl -n "$PROBE_NS" logs deploy/probe-mcp --since-time="$1") || return 1
  user=$(printf '%s\n' "$logs" | grep '"rpc":"tools/call"' | grep -o '"x_user_id":"orka-[0-9a-f]\{40\}"' | head -1 | cut -d'"' -f4)
  [[ -n $user ]] && printf '%s\n' "$logs" | grep '"tools/call cancelled"' | grep -q "\"x_user_id\":\"$user\"" && printf '%s' "$user"
}

# yaml_leaves prints "a.b.c=value" for each scalar in a block-style YAML map.
yaml_leaves() {
  awk '
    /^[[:space:]]*(#|$)/ { next }
    /^[[:space:]]*-/ { next }
    {
      indent = match($0, /[^ ]/) - 1
      line = substr($0, indent + 1)
      key = line; sub(/:.*/, "", key)
      value = line; sub(/^[^:]*:[[:space:]]*/, "", value)
      while (depth > 0 && indents[depth] >= indent) depth--
      depth++; indents[depth] = indent; keys[depth] = key
      if (value != "") {
        path = keys[1]; for (i = 2; i <= depth; i++) path = path "." keys[i]
        gsub(/^["\x27]|["\x27]$/, "", value)
        print path "=" value
      }
    }'
}

check_controller_settings() {
  local leaves; leaves=$(kubectl -n "$KAGENT_NS" get configmap "$KAGENT_CONTROLLER_CONFIG" -o jsonpath='{.data.controller\.yaml}' | yaml_leaves)
  local bash tokens url
  bash=$(printf '%s\n' "$leaves" | sed -n 's/^runtimeDefaults\.skillTools\.disableBash=//p')
  tokens=$(printf '%s\n' "$leaves" | sed -n 's/^runtimeDefaults\.tokenPropagation\.enabled=//p')
  url=$(printf '%s\n' "$leaves" | sed -n 's/^runtimeDefaults\.controlPlaneUrl=//p')
  if [[ $bash == true && $tokens == true && $url == "$CPFILTER_URL" ]]; then
    pass "kagent disables skill bash, forwards the turn token, and sends agents through the filter"
  else
    fail "kagent controller settings" "disableBash=${bash:-<unset>} tokenPropagation=${tokens:-<unset>} controlPlaneUrl=${url:-<unset>}"
  fi
}

check_observed_turn() {
  local name="$RUN-turn"
  create_task "$name" "$OBSERVED_AGENT" 5m '[]'
  local phase; phase=$(wait_terminal "$name")
  local seen; seen=$(events "$name")
  if [[ $phase == Succeeded && $seen == *ToolCallStarted*ToolCallCompleted* ]]; then
    pass "observed turn runs kagent's tool and settles"
  else
    fail "observed turn" "phase=$phase events=$seen"
  fi
}

check_deadline_cancel() {
  local name="$RUN-deadline" start started=no
  probe_sleep 400 || { fail "deadline cancellation" "could not make the probe tool wait"; return; }
  start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  create_task "$name" "$OBSERVED_AGENT" 1m '[]'
  wait_tool_started "$name" && started=yes
  local phase; phase=$(wait_terminal "$name" 60)
  local user; user=$(cancelled_tool_user "$start")
  probe_sleep 0 || fail "deadline cancellation" "could not reset the probe tool"
  if [[ $started == yes && $phase == Cancelled && -n $user ]]; then
    pass "a Task deadline cancels the in-flight kagent tool call"
  else
    fail "deadline cancellation" "tool-started=$started phase=$phase cancelled-user=${user:-<none>}"
  fi
}

check_delete_cancel() {
  local name="$RUN-delete" start
  probe_sleep 400 || { fail "delete cancellation" "could not make the probe tool wait"; return; }
  start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  create_task "$name" "$OBSERVED_AGENT" 5m '[]'
  if ! wait_tool_started "$name"; then
    fail "delete cancellation" "the tool call never started"
    kubectl -n "$ORKA_NS" delete tasks.core.orka.ai "$name" --wait=false >/dev/null 2>&1
    probe_sleep 0
    return
  fi
  local deleted=no
  kubectl -n "$ORKA_NS" delete tasks.core.orka.ai "$name" --wait=true --timeout=4m >/dev/null 2>&1 && deleted=yes
  local user; user=$(cancelled_tool_user "$start")
  probe_sleep 0 || fail "delete cancellation" "could not reset the probe tool"
  if [[ $deleted == yes && -n $user ]]; then
    pass "deleting a Task mid-tool cancels the kagent work and finishes cleanup"
  else
    fail "delete cancellation" "deleted=$deleted cancelled-user=${user:-<none>}"
  fi
}

check_governed_turn() {
  local name="$RUN-governed"
  create_task "$name" "$GOVERNED_AGENT" 10m "[$GOVERNED_TOOL]"
  $ORKA task approvals "$name" --watch --timeout 4m >/dev/null 2>&1
  local id approved=no
  id=$($ORKA task approvals "$name" 2>/dev/null | awk '/pending/ {print $1; exit}')
  [[ -n $id ]] && $ORKA task approve "$name" "$id" --reason "release gate" >/dev/null 2>&1 && approved=yes
  local phase; phase=$(wait_terminal "$name")
  local seen; seen=$(events "$name")
  local usage; usage=$($ORKA task events "$name" -o json 2>/dev/null |
    jq '[if type == "array" then .[] else (.events // .items // [])[] end | select(.type == "ModelUsageUpdated") | .inputTokens // 0] | add // 0')
  if [[ $approved == yes && $phase == Succeeded && $seen == *ApprovalApproved*ToolCallCompleted* && ${usage:-0} -gt 0 ]]; then
    pass "governed turn: Orka gates, executes, and counts the kagent Agent's call"
  else
    fail "governed turn" "approved=$approved phase=$phase usage=${usage:-0} events=$seen"
  fi
}

check_approval_expiry() {
  [[ $LONG == 1 ]] || { printf 'SKIP  approval expiry fails closed (LONG=1 runs it)\n'; return; }
  local name="$RUN-expiry"
  create_task "$name" "$GOVERNED_AGENT" 12m "[$GOVERNED_TOOL]"
  local phase; phase=$(wait_terminal "$name" 160)
  local seen; seen=$(events "$name")
  if [[ $phase =~ ^(Failed|Cancelled|TimedOut)$ && $seen == *ApprovalExpired* && $seen != *ToolCallCompleted* ]]; then
    pass "an expired approval never executes the tool"
  else
    fail "approval expiry" "phase=$phase events=$seen"
  fi
}

check_relay_rejects_strangers() {
  forward "$ORKA_NS" "svc/$RELAY_SERVICE" 8092 || { fail "relay authentication" "port-forward to the relay did not start"; return; }
  local stranger; stranger=$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=')
  local results="" body='{"jsonrpc":"2.0","id":1,"method":"initialize"}'
  for path in /mcp /v1/chat/completions; do
    results="$results $path:none=$(http_code -X POST -d "$body" "http://127.0.0.1:$FORWARD_PORT$path")"
    results="$results $path:stranger=$(http_code -X POST -H "Authorization: Bearer $stranger" -d "$body" "http://127.0.0.1:$FORWARD_PORT$path")"
  done
  stop_forward
  if [[ $results != *=* || $(printf '%s' "$results" | tr ' ' '\n' | grep -c '=401$') != 4 ]]; then
    fail "relay authentication" "$results"
  else
    pass "the relay refuses callers without a live turn token on every endpoint"
  fi
}

check_control_plane_filter() {
  forward "$PROBE_NS" "svc/$CPFILTER_SERVICE" 8083 || { fail "control-plane filter" "port-forward to the filter did not start"; return; }
  # grpc prints "<http status> <grpc-status trailer>" for one empty call.
  grpc() {
    local headers="$WORK/grpc-headers" code
    code=$(curl -s -o /dev/null -D "$headers" -w '%{http_code}' --max-time 10 --http2-prior-knowledge -X POST \
      -H 'Content-Type: application/grpc' --data-binary @/dev/null "http://127.0.0.1:$FORWARD_PORT$1") || code=000
    printf '%s %s' "$code" "$(tr -d '\r' < "$headers" 2>/dev/null | sed -n 's/^grpc-status: *//Ip' | tail -1)"
  }
  local service=/kagent.runtime.v1.TaskStoreService problems=""
  for method in CreateTask GetTask UpdateTask ListTasks SettleTask; do
    # Reaching kagent's server is a 200 whose gRPC status is neither
    # UNIMPLEMENTED (12) nor UNAVAILABLE (14).
    local result; result=$(grpc "$service/$method")
    [[ $result =~ ^200\ ([0-9]+)$ && ${BASH_REMATCH[1]} != 12 && ${BASH_REMATCH[1]} != 14 ]] || problems="$problems $method=[$result]"
  done
  for denied in /kagent.runtime.v1.MemoryService/Search "$service/GetTaskX" "$service/" "${service}X/GetTask" /kagent.api.v1.SessionService/ListSessions; do
    local result; result=$(grpc "$denied")
    [[ $result == "403 "* ]] || problems="$problems $denied=[$result]"
  done
  stop_forward
  if [[ -z $problems ]]; then
    pass "agents reach exactly the five TaskStore methods; memory and every other path are denied"
  else
    fail "control-plane filter" "unexpected:$problems"
  fi
}

check_compiled_egress() {
  local revision relay
  revision=$(kubectl -n "$KAGENT_NS" get agents.api.kagent.dev "$GOVERNED_KAGENT_AGENT" -o jsonpath='{.status.latestSuccessfulRevision}')
  [[ $revision =~ ^[0-9a-f]{64}$ ]] || { fail "compiled egress" "no pinned revision for $GOVERNED_KAGENT_AGENT"; return; }
  relay="http://$RELAY_SERVICE.$ORKA_NS.svc:8092"
  local egress; egress=$(printf "select array_to_string(egress_destinations, ' ') from kagent.agent_runtime_revision where revision='%s';\n" "$revision" | pg)
  local stray=""
  for destination in $egress; do
    case " $relay $CPFILTER_URL $PROVIDER_DEFAULT_HOSTS " in
      *" $destination "*) ;;
      *) stray="$stray $destination" ;;
    esac
  done
  if [[ -n $egress && -z $stray ]]; then
    pass "the governed revision's egress names only the relay, the filter, and accepted provider hosts"
  else
    fail "compiled egress" "revision=$revision unexpected:${stray:- <no egress found>}"
  fi
}

check_timeout_chain() {
  local timeout; timeout=$(kubectl -n "$KAGENT_NS" get remotemcpservers.api.kagent.dev "$RELAY_SERVER" -o jsonpath='{.spec.timeout}')
  local seconds; seconds=$(python3 -c "import re,sys; m=re.fullmatch(r'(?:(\d+)h)?(?:(\d+)m)?(?:(\d+(?:\.\d+)?)s)?', sys.argv[1]); print(int(int(m[1] or 0)*3600+int(m[2] or 0)*60+float(m[3] or 0)) if m and sys.argv[1] else 0)" "$timeout")
  if [[ ${seconds:-0} -gt 1200 ]]; then
    pass "kagent waits ${timeout} for a relayed tool call, longer than the relay's 20m"
  else
    fail "timeout chain" "RemoteMCPServer $RELAY_SERVER timeout=${timeout:-<unset>}; it must exceed 20m"
  fi
}

check_sessions_retired() {
  # Orka-created sessions belong to per-session users named orka-<40 hex>.
  local live; live=$(printf "select count(*) from kagent.session where encode(data,'escape') ~ 'orka-[0-9a-f]{40}' and deletion_reason is null;\n" | pg)
  if [[ $live == 0 ]]; then
    pass "every kagent session Orka created has been deleted"
  else
    fail "session retirement" "${live:-<unreadable>} Orka-created kagent sessions are still live"
  fi
}

check_controller_settings
check_observed_turn
check_deadline_cancel
check_delete_cancel
check_governed_turn
check_approval_expiry
check_relay_rejects_strangers
check_control_plane_filter
check_compiled_egress
check_timeout_chain
check_sessions_retired

if [[ $failures -gt 0 ]]; then
  printf '\n%d check(s) failed; this tuple is not supported.\n' "$failures"
  exit 1
fi
printf '\nAll checks passed for this tuple.\n'
