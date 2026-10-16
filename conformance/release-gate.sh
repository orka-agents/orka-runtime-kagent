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
skipped=0
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

# tool_calls prints the probe's "<boot> <count>" of /tool executions. The
# counter lives in the probe process, so log rotation cannot hide a call; a
# changed boot means a restart lost the count.
tool_calls() {
  forward "$PROBE_NS" deploy/probe-mcp 8080 || return 1
  local reply; reply=$(curl -s --max-time 10 "http://127.0.0.1:$FORWARD_PORT/admin/tool-calls")
  stop_forward
  printf '%s' "$reply" | jq -er '"\(.boot) \(.count)"'
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

# probe_identity prints the probe-mcp pod's name, UID, and restart count, or
# nothing unless exactly one pod serves it. Logs read later are complete
# only if this identity has not changed.
probe_identity() {
  kubectl -n "$PROBE_NS" get pods -l app=probe-mcp -o json 2>/dev/null |
    jq -r 'if (.items | length) == 1 then .items[0] | "\(.metadata.name):\(.metadata.uid):\([.status.containerStatuses[]?.restartCount] | add)" else empty end'
}

# cancelled_tool_user prints the kagent user whose probe tool call started
# after a time, only if that user's call was then cancelled. The gate needs
# the probe to itself: kagent user IDs are per Orka session, and if more
# than one session calls the probe in the window, or a call carries no
# valid user, the cancellation cannot be tied to the call under test.
cancelled_tool_user() { # cancelled_tool_user <since> <probe identity at the start>
  local logs calls users user
  [[ -n $2 ]] || return 1
  logs=$(kubectl -n "$PROBE_NS" logs deploy/probe-mcp --since-time="$1") || return 1
  # The logs are complete only from the same, unrestarted, single pod.
  [[ $(probe_identity) == "$2" ]] || return 1
  calls=$(printf '%s\n' "$logs" | grep '"rpc":"tools/call"')
  [[ -n $calls ]] || return 1
  users=$(printf '%s\n' "$calls" | grep -o '"x_user_id":"[^"]*"' | sort -u)
  [[ $(printf '%s\n' "$calls" | wc -l) == $(printf '%s\n' "$calls" | grep -c '"x_user_id":"orka-[0-9a-f]\{40\}"') ]] || return 1
  [[ $(printf '%s\n' "$users" | wc -l) == 1 ]] || return 1
  user=$(printf '%s' "$users" | cut -d'"' -f4)
  printf '%s\n' "$logs" | grep '"tools/call cancelled"' | grep -q "\"x_user_id\":\"$user\"" && printf '%s' "$user"
}

# governed_revision prints the governed Agent's pinned kagent revision.
governed_revision() {
  kubectl -n "$KAGENT_NS" get agents.api.kagent.dev "$GOVERNED_KAGENT_AGENT" -o jsonpath='{.status.latestSuccessfulRevision}'
}

check_compiled_settings() {
  # Runtime defaults are compiled into each revision's actor template, so
  # this reads what the pinned revision runs with, not the desired config.
  local revision; revision=$(governed_revision)
  [[ $revision =~ ^[0-9a-f]{64}$ ]] || { fail "compiled runtime settings" "no pinned revision for $GOVERNED_KAGENT_AGENT"; return; }
  local query="select encode(t.proto, 'escape') from kagent.agent_runtime_revision r join substrate.actor_templates t
    on t.name = r.actor_template_name and t.atespace = r.actor_template_atespace where r.revision = '$revision';"
  local compiled; compiled=$(printf '%s\n' "$query" | pg | tr -d '\n')
  local problems=""
  [[ $compiled =~ \"skill_tools\":\{[^}]*\"disable_bash\":true ]] || problems="$problems skill-bash-enabled"
  [[ $compiled =~ \"token_propagation\":\{[^}]*\"enabled\":true ]] || problems="$problems no-token-propagation"
  [[ $compiled == *"\"control_plane_url\":\"$CPFILTER_URL\""* ]] || problems="$problems control-plane-not-filtered"
  if [[ -n $compiled && -z $problems ]]; then
    pass "the pinned revision disables skill bash, forwards the turn token, and uses the filter"
  else
    fail "compiled runtime settings" "revision=$revision${problems:- <no compiled template found>}"
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
  local name="$RUN-deadline" start started=no identity
  probe_sleep 400 || { fail "deadline cancellation" "could not make the probe tool wait"; return; }
  identity=$(probe_identity)
  start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  create_task "$name" "$OBSERVED_AGENT" 1m '[]'
  wait_tool_started "$name" && started=yes
  local phase; phase=$(wait_terminal "$name" 60)
  local user; user=$(cancelled_tool_user "$start" "$identity")
  probe_sleep 0 || fail "deadline cancellation" "could not reset the probe tool"
  if [[ $started == yes && $phase == Cancelled && -n $user ]]; then
    pass "a Task deadline cancels the in-flight kagent tool call"
  else
    fail "deadline cancellation" "tool-started=$started phase=$phase cancelled-user=${user:-<none>}"
  fi
}

check_delete_cancel() {
  local name="$RUN-delete" start identity
  probe_sleep 400 || { fail "delete cancellation" "could not make the probe tool wait"; return; }
  identity=$(probe_identity)
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
  local user; user=$(cancelled_tool_user "$start" "$identity")
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
  [[ $LONG == 1 ]] || { printf 'SKIP  approval expiry fails closed (LONG=1 runs it)\n'; skipped=$((skipped + 1)); return; }
  local name="$RUN-expiry" identity before
  identity=$(probe_identity)
  [[ -n $identity ]] || { fail "approval expiry" "the probe tool backend is not exactly one pod"; return; }
  before=$(tool_calls) || { fail "approval expiry" "could not read the probe's tool-call counter"; return; }
  create_task "$name" "$GOVERNED_AGENT" 12m "[$GOVERNED_TOOL]"
  local phase; phase=$(wait_terminal "$name" 160)
  local seen; seen=$(events "$name")
  # The governed tool's backend is the probe's /tool endpoint: any call
  # there after an expired approval is an execution. The Task itself may
  # still succeed, since the model can finish after an error tool result.
  # The count is complete only from the same, unrestarted probe process.
  local after executions=unknown
  if after=$(tool_calls) && [[ ${after%% *} == "${before%% *}" && $(probe_identity) == "$identity" ]]; then
    executions=$((${after##* } - ${before##* }))
  fi
  if [[ $phase =~ ^(Succeeded|Failed|Cancelled|TimedOut)$ && $seen == *ApprovalExpired* && $executions == 0 ]]; then
    pass "an expired approval never executes the tool"
  else
    fail "approval expiry" "phase=$phase executions=$executions events=$seen"
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
  # grpc prints "<http status> <grpc-status> <grpc-message>" for one empty call.
  grpc() {
    local headers="$WORK/grpc-headers" code
    code=$(curl -s -o /dev/null -D "$headers" -w '%{http_code}' --max-time 10 --http2-prior-knowledge -X POST \
      -H 'Content-Type: application/grpc' --data-binary @/dev/null "http://127.0.0.1:$FORWARD_PORT$1") || code=000
    printf '%s %s %s' "$code" "$(tr -d '\r' < "$headers" 2>/dev/null | sed -n 's/^grpc-status: *//Ip' | tail -1)" \
      "$(tr -d '\r' < "$headers" 2>/dev/null | sed -n 's/^grpc-message: *//Ip' | tail -1)"
  }
  local service=/kagent.runtime.v1.TaskStoreService problems=""
  for method in CreateTask GetTask UpdateTask ListTasks SettleTask; do
    # Only kagent's gRPC server, after it has resolved the method, rejects an
    # empty unary call as a cardinality violation (INTERNAL, 13). Any other
    # status, such as a filter's PERMISSION_DENIED (7), is not proof.
    local result; result=$(grpc "$service/$method")
    [[ $result == "200 13 "*"cardinality violation"* ]] || problems="$problems $method=[$result]"
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
  local stray="" missing=""
  for destination in $egress; do
    case " $relay $CPFILTER_URL $PROVIDER_DEFAULT_HOSTS " in
      *" $destination "*) ;;
      *) stray="$stray $destination" ;;
    esac
  done
  # The governed setup needs both; a list of provider hosts alone is not it.
  for required in "$relay" "$CPFILTER_URL"; do
    [[ " $egress " == *" $required "* ]] || missing="$missing $required"
  done
  if [[ -n $egress && -z $stray && -z $missing ]]; then
    pass "the governed revision's egress names only the relay, the filter, and accepted provider hosts"
  else
    fail "compiled egress" "revision=$revision unexpected:${stray:- none} missing:${missing:- none} egress:${egress:- <none found>}"
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
  # Only the terminal state proves deletion; a deletion reason is recorded
  # when deletion is merely admitted.
  local live; live=$(printf "select count(*) from kagent.runtime_instance where kind = 'agent' and user_id ~ '^orka-[0-9a-f]{40}\$' and state <> 'RUNTIME_STATE_DELETED';\n" | pg)
  if [[ $live == 0 ]]; then
    pass "every kagent session Orka created has been deleted"
  else
    fail "session retirement" "${live:-<unreadable>} Orka-created kagent sessions are still live"
  fi
}

check_compiled_settings
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
if [[ $skipped -gt 0 ]]; then
  # Only a run of every check qualifies a tuple.
  printf '\nThe checks that ran passed, but %d were skipped; run with LONG=1 to qualify this tuple.\n' "$skipped"
  exit 2
fi
printf '\nAll checks passed; this tuple is supported.\n'
