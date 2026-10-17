# kagent runtime adapter for Orka

This adapter lets Orka govern an agent that runs on [kagent](https://github.com/kagent-dev/kagent).
kagent keeps running the agent's reasoning loop. Orka owns how work reaches the
agent, its identity and lifecycle, and the audit record.

It supports two kagent lines:

- **kagent v1** (`api.kagent.dev/v1alpha3`), the Substrate-based design. Most
  of this README describes it.
- **kagent v0.x** (`kagent.dev/v1alpha2`), which runs each Agent as a
  Deployment. See [kagent v0.x](#kagent-v0x) for what differs.

> **Status:** experimental. v1 is tested against kagent `1eb89fb1`
> (v1.0.0-alpha13+31) with kagent's Substrate fork v0.5.0-alpha2. v0 is tested
> against kagent v0.10.3 with the Go runtime.

## How it fits together

```
Orka supervisor ──ACP──► child ──Responses──► supervisor provider proxy ──► broker ──gRPC──► kagent
 (stock Orka)          (no kagent        (loopback, prompt-scoped,       (kagent identity,  (Sessions + A2A)
                         credentials)      stamps the owner context)       durable ledger)
```

One image provides both processes:

- **ACP child** (`--protocol acp`, the default command). The Orka supervisor
  launches it for each runtime session. It sends one Responses request per
  prompt through the supervisor's loopback provider proxy, then relays the
  broker's event stream as ACP updates. It holds no kagent credentials.
- **Lifecycle broker** (`--protocol broker`). This sidecar speaks Orka's
  lifecycle broker protocol (`orka.foundry.broker.v1`), the same contract the
  Foundry adapter uses. It maps each Orka runtime session to one kagent
  Session and keeps a durable, content-free ledger.

## Guarantees

- **One kagent identity per Orka session.** The broker derives a kagent user
  ID from the session fence, so Orka sessions never share a kagent user.
- **Revision pinning.** kagent pins each new Session to the Agent's latest
  successful revision. Before every dispatch the broker reads the session
  again. It refuses any revision other than `expectedRevision`, and it refuses
  a session that kagent has deleted, expired, or begun deleting. A gone session
  is never recreated: kagent keeps a tombstone for each session's request ID.
- **No blind resends.** The broker persists a deterministic message ID before
  each send. It retries only when kagent explicitly reports
  `KAGENT_SEND_NOT_ACCEPTED`. Any other uncertain send is resolved from kagent's
  task history and never sent again.
- **Proven settlement.** Settlement is reported only once one of these holds:
  - the message was never dispatched;
  - kagent refused it;
  - the task is terminal. Unfinished and waiting tasks are cancelled first;
  - kagent reports the session itself deleted.

  If a send's outcome is unknown and no task appears within 150 seconds, the
  broker deletes the kagent session rather than trusting the task's absence.
  That Orka session cannot continue afterwards, and its later prompts fail
  with `kagent_session_gone`.
- **Proven retirement.** Retirement is reported only after `DeleteSession`
  succeeds and `GetSession` returns NotFound.
- **Observed tools by default.** In observed mode kagent runs its own tools.
  Orka sees each tool's name and status, never its arguments or results.
  [Governed mode](#governed-mode) moves tools and the model under Orka. An `input-required` or
  `auth-required` pause cancels the kagent task and fails the turn, because a
  wrapped agent cannot answer kagent approval prompts.

Orka requires external runtimes to register strict workspace governance. The
Orka supervisor provides those guarantees. Two of the claims also depend on
the wrapped kagent Agent:

- `noDirectSCMPublication`
- `orkaOwnedCleanRoomPublication`

The operator must attest that the Agent has no tool able to publish to source
control directly.

## Governed mode

In governed mode, Orka executes the Agent's tools and serves its model. The
`governed` section of `kagent.json` turns it on:

```json
"governed": {
  "upstreamBaseURL": "http://<orka model endpoint>/v1",
  "upstreamModel": "<model to call>"
}
```

The broker then opens a relay (`ORKA_KAGENT_RELAY_ADDR`, default `:8092`) for
kagent to call:

- `/mcp` forwards MCP tool calls to the turn's Orka session MCP server. Orka's
  policy, approvals, credentials, and audit apply to every call.
- `/v1/chat/completions` forwards model calls to `upstreamBaseURL`. It pins
  `upstreamModel`, adds Orka's credential (`ORKA_KAGENT_MODEL_UPSTREAM_TOKEN`),
  and totals token usage for Orka. The relay sends that credential on every
  model call, so use an `https` `upstreamBaseURL` unless the network path to
  it is trusted.

### Turn tokens

For each turn the broker mints a random token and sends it as the A2A
caller's bearer. kagent forwards that bearer to the Agent's model and MCP
calls. The relay accepts a token only while its turn is live and only for that
turn's kagent user. Tokens exist only in memory and die with the turn.

### What the kagent side must look like

The import step checks this before it pins `expectedRevision`. The revision
digest then keeps it fixed.

- The kagent controller runs with `tokenPropagation.enabled` and the ADK
  harness with `skillTools.disableBash`. Memory is off.
- Every model is a `ModelConfig` with provider `OpenAI`, `baseUrl` set to the
  relay's `/v1`, and `apiKeyPassthrough: true`.
- Every tool comes from one `RemoteMCPServer`:
  - its URL is the relay's `/mcp`;
  - its protocol is `STREAMABLE_HTTP`;
  - its `timeout` is longer than `20m`;
  - it carries the `kagent.dev/discovery=disabled` label, because the relay
    answers only turn-scoped callers.
- The revision's egress list names only the relay and the control-plane
  filter. kagent also always allows its provider's default host, which is an
  accepted risk.
- A NetworkPolicy lets only the kagent Agent pods reach the relay.

On the Orka side, render the profile with the Orka tools the Agent may call
(`--allowed-tools`, `--approval-required-tools`).

### Timeouts

The timeouts nest:

1. Orka holds an approval-gated call for up to 900 seconds.
2. The relay waits up to 20 minutes.
3. The `RemoteMCPServer` timeout must be longer still.
4. The turn's lease outlives all of these.

### Audit

Orka's tool broker records every governed call; that record is authoritative.
The `response.kagent_tool.*` events Orka shows for kagent's own tool activity
are observations.

### Retries

A tool call's Orka operation ID comes from the turn, the relay's MCP session,
and kagent's request ID. A retried request therefore never runs twice. If
kagent re-issues a call on a new MCP session, Orka sees it as a new operation.
That can happen after a sandbox crash.

## Supervisor restart recovery

The broker implements Orka's boot-retirement contract, the same one the Foundry
broker uses:

- `GET /internal/v1/identity` returns the ledger's identity. Every ledger has
  its own identity, so a replaced ledger cannot prove an older one clean.
- `POST /internal/v1/retire-boot` permanently seals a dead supervisor boot.
  Seals last as long as the ledger. A ledger holds up to 4096 seals; past
  that, retiring another boot fails closed until the ledger is replaced.
  It then retires every session under that boot through the normal proven
  path. It answers `retired` only once all of them are retired.

To let Orka recover from a supervisor restart:

1. Set `ORKA_ACP_FOUNDRY_RECOVERY_PROFILE_DIGEST` on the supervisor to the
   runtime profile digest.
2. Register the AgentRuntime with `deployment.kubernetesRecovery`.
3. Keep the pod to exactly the `supervisor` and `broker` containers.

## Quarantine runbook

The broker gives cleanup ten minutes to prove settlement. A prompt still
unproven after that quarantines its session:

- The session accepts no new prompts.
- Orka sees the state `quarantined`, which never counts as settled.
- The broker logs one `kagent session quarantined` line with the owner digest,
  the kagent user and session IDs, and the unfinished task IDs.

Cleanup keeps retrying every 30 seconds, so quarantine ends by itself once
kagent can prove the outcome. The usual cause is an agent that crashed before
kagent recorded the task's end, or a lost kagent controller claim. To resolve it:

1. Use the logged IDs to inspect the task in kagent.
2. If the task cannot finish, delete the kagent session as the logged user.

Deleting the session proves that nothing in it can still change. The broker
then settles the prompt and retires the session normally.

## Configuration

`/agent/kagent.json` is baked into the configured image. Both processes check
its SHA-256 against `ORKA_KAGENT_ACP_AGENT_CONFIGURATION_DIGEST`.

```json
{
  "model": "kagent/kagent/probe",
  "kagent": {
    "controlPlaneURL": "http://kagent-controller.kagent.svc:8083",
    "agent": {"namespace": "kagent", "name": "probe"},
    "expectedRevision": "<64-hex prepared revision>",
    "userIDPrefix": "orka"
  }
}
```

- `model` is the profile model string Orka pins. kagent owns the real model.
- `controlPlaneURL` must name a DNS host with no path.
- `kagent.version` is `v1` (the default) or `v0`. For v0, `expectedRevision`
  is the decimal `kagent.dev/config-hash` of the Agent's pod.

| Process | Variable | Purpose |
| --- | --- | --- |
| both | `ORKA_KAGENT_ACP_MODEL`, `ORKA_KAGENT_ACP_AGENT_CONFIGURATION_DIGEST` | Must match the baked config |
| child | `ORKA_KAGENT_ACP_PROVIDER_BASE_URL`, `ORKA_KAGENT_ACP_PROVIDER_TOKEN` | Set by the supervisor (loopback proxy) |
| broker | `ORKA_KAGENT_BROKER_ADDR` | Loopback listen address, default `127.0.0.1:8091` |
| broker | `ORKA_KAGENT_BROKER_STATE_DIR` | Absolute path on a broker-only persistent volume |
| broker | `ORKA_KAGENT_BROKER_BEARER_TOKEN` | At least 32 bytes; shared only with the supervisor's provider token file |
| broker, governed | `ORKA_KAGENT_RELAY_ADDR` | Relay listen address, default `:8092` |
| broker, governed | `ORKA_KAGENT_MODEL_UPSTREAM_TOKEN` | Orka's credential for `upstreamBaseURL`; broker only |

The supervisor runs with `ORKA_ACP_PROVIDER=kagent` and with
`ORKA_ACP_PROVIDER_PROXY_BASE_URL=http://127.0.0.1:8091/v1`. Its provider token
file contains the broker bearer.

### Recommended kagent install settings

Run governed agents in a dedicated kagent install, with these settings. The
first one applies to observed agents too: without it, Orka does not control
who can dispatch work to any wrapped Agent.

- Admit only kagent's controller to Substrate's actor router. Each kagent
  Session is a Substrate actor that the router reaches by a request header,
  and the agent runtime trusts whoever calls it. Without this, any pod in the
  cluster can drive an Orka-owned Session directly, bypassing both kagent's
  gateway and the broker. For example:

  ```yaml
  apiVersion: networking.k8s.io/v1
  kind: NetworkPolicy
  metadata:
    name: atenet-router-ingress
    namespace: ate-system
  spec:
    podSelector:
      matchLabels:
        app: atenet-router
    policyTypes: [Ingress]
    ingress:
      - from:
          - namespaceSelector:
              matchLabels:
                kubernetes.io/metadata.name: kagent
            podSelector:
              matchLabels:
                app.kubernetes.io/name: kagent
                app.kubernetes.io/component: controller
  ```

  A NetworkPolicy limits who reaches the router, not which actors they
  target: anything it admits reaches every actor in that Substrate. So admit
  only kagent's controller, and don't share the Substrate install with other
  Substrate clients unless they are equally trusted. Examples are Orka's
  Substrate workspace provider (`--substrate-router-url`) and google/ax.
- Allow only the broker to reach the public API.
- Set `config.runtimeDefaults.controlPlaneUrl` to a proxy that forwards
  exactly the five `kagent.runtime.v1.TaskStoreService` methods: `CreateTask`,
  `GetTask`, `UpdateTask`, `ListTasks`, and `SettleTask`. Match exact paths, not
  a prefix; `kagent.runtime.v1.MemoryService` uses ordinary public auth.
- Before supporting a new version tuple, run the
  [release gate](conformance/README.md).
- Restart the kagent controller after changing its config.

## kagent v0.x

Set `"version": "v0"` in the `kagent` section of `kagent.json`; the import step
does this for `kagent.dev/v1alpha2` Agents. Everything above applies except
where this section says otherwise.

### How v0 differs

- **Transport.** The broker uses the controller's REST session API and A2A
  over JSON-RPC at `/api/a2a/<namespace>/<name>/`, not v1's gRPC services.
- **Pinning.** v0 has no revisions. `expectedRevision` is the
  `kagent.dev/config-hash` annotation on the Agent's pod: a hash of the
  compiled config, agent card, and secrets, so it changes when the Agent or
  anything it references changes. Before every dispatch the broker requires
  exactly one ready pod object with that hash. A rollout or restart in
  progress, terminating pods included, refuses the turn.
- **One replica.** A cancel reaches only the pod that runs the task, so a
  wrapped v0 Agent must run a single replica.
- **Settlement.** Deleting a v0 session only marks its row; a running turn
  finishes its current model or tool call and then fails at its next session
  write. So deletion never settles a known task. kagent v0 also records a
  cancellation before the execution has unwound, so an unfinished or
  canceled task settles only once a cancel call returns: that call waits for
  a running execution to end. A `completed` or `failed` task settles as
  written, because kagent writes those only as the executor's last action.
  (kagent v0.10's controller cannot relay a refusal to cancel a finished
  task; it answers an internal error.)
- **Ambiguous sends.** When a send's outcome is unknown and no task holds the
  message after the dispatch window, the broker deletes the session, which
  stops any turn before its next tool. A deleted session's tasks can no
  longer be listed, so:
  - a **governed** send settles at once: its turn token died with the turn,
    so a late execution reaches neither Orka's tools nor its model;
  - an **observed** send settles once every Agent pod that existed at the
    deletion is gone. Restart the Agent's Deployment to resolve it sooner.
    This trusts that pods are deleted gracefully: never force-delete a
    wrapped Agent's pod while a send waits on it.
- **Cancellation** stops a running turn only on the Go runtime, and a client
  disconnect never does. The import refuses Python-runtime Agents: they cannot
  cancel, and they store every request header, including the turn token, in
  kagent's database.
- **MCP sessions.** The Go runtime opens one MCP session per process and
  reuses it across turns and Orka sessions. The relay authorizes each request
  by its turn token, not by the session.

### What the v0 install must look like

Run wrapped Agents in a dedicated kagent install, as for v1, with the Go
runtime (`declarative.runtime: go`) and one replica. Only kagent's controller
may change the Agents' Deployments: the import checks that each Deployment
loads its compiled configuration as kagent renders it, but the broker pins
only the config hash at run time. A governed verdict also trusts the Go
runtime image your kagent install selects (`goAgentImage`). The import checks
the image's name, not where it came from.

- **Isolation.** v0 ships no NetworkPolicies, its default `unsecure` auth mode
  accepts any `X-User-Id`, and its authorization is a no-op. Without these
  policies any pod can drive an Orka-owned session or read its tasks:

  ```yaml
  # Agent pods: only kagent's controller, which proxies A2A, reaches them.
  apiVersion: networking.k8s.io/v1
  kind: NetworkPolicy
  metadata: {name: kagent-agents-ingress, namespace: kagent}
  spec:
    podSelector:
      matchLabels: {app: kagent, app.kubernetes.io/managed-by: kagent}
    policyTypes: [Ingress]
    ingress:
      - from:
          - podSelector:
              matchLabels: {app.kubernetes.io/name: kagent, app.kubernetes.io/component: controller}
        ports: [{port: 8080, protocol: TCP}]
  ---
  # Controller API: the Agents, which kagent points at it, and the brokers.
  apiVersion: networking.k8s.io/v1
  kind: NetworkPolicy
  metadata: {name: kagent-controller-ingress, namespace: kagent}
  spec:
    podSelector:
      matchLabels: {app.kubernetes.io/name: kagent, app.kubernetes.io/component: controller}
    policyTypes: [Ingress]
    ingress:
      - from:
          - podSelector:
              matchLabels: {app: kagent, app.kubernetes.io/managed-by: kagent}
          - namespaceSelector:
              matchLabels: {kubernetes.io/metadata.name: orka-system}
            podSelector:
              matchLabels: {orka.ai/kagent-broker: "true"}
        ports: [{port: 8083, protocol: TCP}]
  ---
  # Bundled PostgreSQL, whose credentials are fixed: only the controller.
  apiVersion: networking.k8s.io/v1
  kind: NetworkPolicy
  metadata: {name: kagent-postgresql-ingress, namespace: kagent}
  spec:
    podSelector:
      matchLabels: {app.kubernetes.io/name: kagent, app.kubernetes.io/component: database}
    policyTypes: [Ingress]
    ingress:
      - from:
          - podSelector:
              matchLabels: {app.kubernetes.io/name: kagent, app.kubernetes.io/component: controller}
        ports: [{port: 5432, protocol: TCP}]
  ```

  Label each wrapper pod `orka.ai/kagent-broker: "true"`. Turn off kagent's
  UI or keep it off this network: it calls the controller as any user.
- **Code in Agent pods.** The controller policy cannot narrow what Agent pods
  call. kagent v0 points every Agent at the controller Service itself, so
  unlike v1's `controlPlaneUrl` there is no place for a filtering proxy, and
  the session and task routes the runtime needs accept any `X-User-Id`.
  Anything in an Agent pod that reaches the controller can therefore act as
  any user, including the users of other Orka sessions. kagent runs the
  model's skill commands and code blocks in a sandbox that has no network
  unless the Agent sets `spec.sandbox.network.allowedDomains`. The import
  refuses such Agents in either mode. Add nothing else to Agent pods that
  could reach the controller.
- **Broker access to the Agent's pods.** The broker lists the Agent's pods
  with its pod's service account. Grant `get` and `list` on `pods` in the
  Agent's namespace, and mount a token into the broker container only:

  ```yaml
  # In the wrapper pod spec; automountServiceAccountToken stays false.
  volumes:
    - name: broker-kube
      projected:
        sources:
          - serviceAccountToken: {path: token, expirationSeconds: 3600}
          - configMap: {name: kube-root-ca.crt, items: [{key: ca.crt, path: ca.crt}]}
  # In the broker container:
  volumeMounts:
    - {name: broker-kube, mountPath: /var/run/secrets/kubernetes.io/serviceaccount, readOnly: true}
  ```
- **Governed v0 Agents** (the import checks these):
  - the model is an OpenAI `ModelConfig` with `openAI.baseUrl` set to the
    relay's `/v1`, `apiKeyPassthrough: true`, and the chat completions format;
  - every tool binding names a `RemoteMCPServer` at the relay's `/mcp`, with
    protocol `STREAMABLE_HTTP` and a `timeout` longer than `20m`;
  - each binding sets `allowedHeaders: [Authorization]`, so kagent forwards
    the turn token, and nothing sets `headersFrom`, which would override it;
  - no `requireApproval`, skills, `executeCodeBlocks`, memory, `shareTools`,
    Agent tools, or deployment environment overrides.

### v0 quarantine runbook

The broker logs the same `kagent session quarantined` line. Deleting the
session does not resolve a v0 quarantine, because it does not stop a running
turn:

1. If the logged tasks are still running, cancel them with A2A `CancelTask`
   as the logged user.
2. If the Agent's pod has restarted since, its executions are gone, and the
   broker's next cancel settles the task.
3. An observed send that left no task settles once the Agent's pods that
   existed at the fence are gone; restart the Agent's Deployment.

## Build

```sh
docker build --build-arg KAGENT_CONFIG=examples/kagent.json -t <registry>/kagent-configured:<tag> .
```

Then compose the Orka runtime image from an Orka checkout:

```sh
make docker-build-acp-kagent-runtime \
  KAGENT_RUNTIME_IMAGE=<registry>/kagent-configured@sha256:<digest> \
  KAGENT_ADAPTER_DIGEST=sha256:<same-digest> \
  ACP_KAGENT_RUNTIME_IMG=<registry>/kagent-runtime:<tag>
```

## Limits

- **Single replica.** Run one replica with the `Recreate` strategy and the
  ledger on a broker-only volume.
- **Bounded ledger.**
  - Retired sessions are pruned 24 hours after retirement.
  - A ledger holds at most 4096 live sessions.
  - Each session holds at most 4096 prompts. Settled prompts count until the
    session is retired.
  - New prompts, new sessions, and lease renewals stop when the ledger reaches
    24 MiB, and the broker answers 503. The last 8 MiB stays for settling and
    retiring what the ledger already holds.
- **One request per prompt.** Each prompt sends a single request to the broker.
- **kagent v0 shares one Agent process across every Orka session.** Sessions
  are separated by kagent user and session ID, not by process.
- **Claude and Codex harnesses are observed only.** They reject
  `apiKeyPassthrough`, so the relay cannot tell which session is calling.
- **kagent prerelease constraints apply:**
  - task-store identity is an unsigned header until Substrate #1660;
  - idle kagent Sessions expire after seven days by default.
