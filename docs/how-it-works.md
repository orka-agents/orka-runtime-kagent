# How it works

This page explains what the adapter does under the hood and what it
guarantees. To set it up, start with the [README](../README.md).

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
  each send. It retries only when the send never reached kagent, or when kagent
  explicitly reports `KAGENT_SEND_NOT_ACCEPTED`. Any other uncertain send is
  resolved from kagent's task history and never sent again.
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
- **Token usage.** A governed turn reports the usage the relay metered. An
  observed turn's model calls bypass Orka, so it reports the usage kagent put
  on the turn, summed once per model call: kagent v0 puts it on the status
  message each model call produced, and kagent v1 on the artifact. The
  finished task keeps both, so a turn whose stream broke still reports the
  calls the stream missed. kagent measured those numbers, not Orka.

## Admission checks

Before each turn the broker checks the kagent install, and refuses the turn if
either check fails. Cleanup of existing sessions never waits on them.

- **Verified build.** The broker reads the kagent controller's git commit
  (v0 `GET /version`, v1 `SystemService.GetVersion`). It accepts only the
  builds this release passed the [release gate](../conformance/README.md)
  against, and any commit listed in `kagent.acceptedCommits`. The broker
  depends on behavior read from those exact builds. Other builds fail with
  `kagent_build_unverified`; list a build in `acceptedCommits` only after the
  release gate passes against it.
- **Isolation.** The broker tries to connect to each `host:port` in
  `unreachable`, such as Substrate's actor router or kagent's database. It
  should reach neither: the install should admit only kagent's controller to
  them. A completed connection fails turns with `kagent_isolation_broken`; a
  refused or dropped one passes. This catches an install with no policies at
  all. It does not prove that no other pod gets through.

A passing check is reused for five minutes. A failing check is repeated on
the next turn, and each change is logged.

## Governed mode

In governed mode, Orka executes the Agent's tools and serves its model. The
`governed` section of `kagent.json` turns it on:

```json
"governed": {
  "upstreamBaseURL": "http://<orka model endpoint>/v1",
  "upstreamModel": "<model to call>",
  "maxOutputTokens": 4096
}
```

`maxOutputTokens` is optional. When set, the relay bounds every model call's
output the way Orka's provider proxy does: the request keeps one of
`max_tokens` or `max_completion_tokens`, set to the smaller of the limit and
what the request asked for. Orka's own model limits do not apply to a kagent
runtime, because the supervisor never sees kagent's model calls.

The broker then opens a relay (`ORKA_KAGENT_RELAY_ADDR`, default `:8092`) for
kagent to call:

- `/mcp` forwards MCP tool calls to the turn's Orka session MCP server. Orka's
  policy, approvals, credentials, and audit apply to every call.
- `/v1/chat/completions` forwards model calls to `upstreamBaseURL`. It pins
  `upstreamModel`, adds Orka's credential (`ORKA_KAGENT_MODEL_UPSTREAM_TOKEN`),
  and totals token usage for Orka. The relay sends that credential on every
  model call, so use an `https` `upstreamBaseURL` unless the network path to
  it is trusted.

The relay closes every connection after its response. kagent reaches it
through a Service that moves to the new runtime on a re-pin, and a kept-alive
connection would keep reaching the old broker.

The import checks that the kagent side is set up for governed mode; see
[Governed Agents](kagent-install.md#governed-agents) for what it requires.

### Turn tokens

For each turn the broker mints a random token and sends it as the A2A
caller's bearer. kagent forwards that bearer to the Agent's model and MCP
calls. The relay accepts a token only while its turn is live and only for that
turn's kagent user. Tokens exist only in memory and die with the turn.

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

## kagent v0.x

Set `"version": "v0"` in the `kagent` section of `kagent.json`; the import step
does this for `kagent.dev/v1alpha2` Agents. Everything above applies except
where this section says otherwise.

### How v0 differs

- **Transport.** The broker uses the controller's REST session API and its
  A2A route at `/api/a2a/<namespace>/<name>/` for task reads, not v1's gRPC
  services. Sends and cancels go to the pinned pod itself, over the A2A 0.3
  JSON-RPC its Go runtime serves, with the `X-User-Id` and `Authorization`
  headers kagent's controller would forward. The pod saves its sessions and
  tasks to the controller, so reads see them.
- **Pinning.** v0 has no revisions. `expectedRevision` is the
  `kagent.dev/config-hash` annotation on the Agent's pod: a hash of the
  compiled config, agent card, and secrets, so it changes when the Agent or
  anything it references changes. The hash does not cover the image, so
  `expectedImageDigest` pins the image the pod's container reports. Before
  every dispatch the broker requires exactly one ready pod object with that
  hash and image. A rollout or restart in progress, terminating pods
  included, refuses the turn.
- **Binding the send.** The broker sends to that pod's own address, not the
  Agent's Service, so the turn cannot land on a replacement. First it reads
  the pod's agent card on a fresh connection: the card's URL names the Agent
  and its namespace, so a listener of another workload that took the address
  is refused. The send then reuses that connection and never opens another.
  A pod keeps its address for its whole life, so once the send may have
  arrived, finding the same pod still at that address proves the turn runs
  there. The ledger records each send as unbound before it is made, and
  clears that only once this check passes. If the pod changed in between, or
  the broker stopped first, the send settles only as an
  [ambiguous send](#how-v0-differs) that waits for the pods to be replaced:
  which pod runs it is unknown, so a cancel answered by the current pod
  proves nothing, and a governed turn's revoked token proves nothing either.
  A pod change fails the turn with `kagent_revision_mismatch`, and that Orka
  session cannot continue. A send that never reached the pod is retried.
- **One replica.** A cancel reaches only the pod that runs the task, so a
  wrapped v0 Agent must run a single replica.
- **Settlement.** Deleting a v0 session only marks its row; a running turn
  finishes its current model or tool call and then fails at its next session
  write. So deletion never settles a known task. kagent v0 also records a
  cancellation before the execution has unwound, so an unfinished or
  canceled task settles only once a cancel call returns: that call waits for
  a running execution to end. A `completed` or `failed` task settles as
  written, because kagent writes those only as the executor's last action.
- **Ambiguous sends.** When a send's outcome is unknown and no task holds the
  message after the dispatch window, the broker deletes the session, which
  stops any turn before its next tool. A deleted session's tasks can no
  longer be listed, so:
  - a **governed** send settles at once: its turn token died with the turn,
    so a late execution reaches neither Orka's tools nor its model. A send
    whose pinned pod changed during it is the exception: the pod that got it
    may run a config that never uses the token, so it settles as an observed
    send does;
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
- **Unanswered creates.** v0 answers 400 to every session create once the
  Agent is deleted, without saying whether an earlier create committed. v0
  creates the session under the broker's ID, though, and only on the create
  request's own context. So when Orka retires a session whose create was never
  answered, the broker stops creating, waits five minutes past its last
  attempt, then deletes that ID and requires it gone.

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
