# kagent runtime adapter for Orka

This adapter lets Orka govern an agent that runs on [kagent](https://github.com/kagent-dev/kagent)
v1, the Substrate-based design. kagent keeps running the agent's reasoning loop.
Orka owns how work reaches the agent, its identity and lifecycle, and the audit record.

> **Status:** experimental and local only. It is tested against kagent
> `1eb89fb1` (v1.0.0-alpha13+31) with kagent's Substrate fork v0.5.0-alpha2.

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
- **Observed tools only.** kagent runs its own tools. Orka sees each tool's
  name and status, never its arguments or results. An `input-required` or
  `auth-required` pause cancels the kagent task and fails the turn, because a
  wrapped agent cannot answer kagent approval prompts.

Orka requires external runtimes to register strict workspace governance. The
Orka supervisor provides those guarantees. Two of the claims also depend on
the wrapped kagent Agent:

- `noDirectSCMPublication`
- `orkaOwnedCleanRoomPublication`

The operator must attest that the Agent has no tool able to publish to source
control directly.

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

| Process | Variable | Purpose |
| --- | --- | --- |
| both | `ORKA_KAGENT_ACP_MODEL`, `ORKA_KAGENT_ACP_AGENT_CONFIGURATION_DIGEST` | Must match the baked config |
| child | `ORKA_KAGENT_ACP_PROVIDER_BASE_URL`, `ORKA_KAGENT_ACP_PROVIDER_TOKEN` | Set by the supervisor (loopback proxy) |
| broker | `ORKA_KAGENT_BROKER_ADDR` | Loopback listen address, default `127.0.0.1:8091` |
| broker | `ORKA_KAGENT_BROKER_STATE_DIR` | Absolute path on a broker-only persistent volume |
| broker | `ORKA_KAGENT_BROKER_BEARER_TOKEN` | At least 32 bytes; shared only with the supervisor's provider token file |

The supervisor runs with `ORKA_ACP_PROVIDER=kagent` and with
`ORKA_ACP_PROVIDER_PROXY_BASE_URL=http://127.0.0.1:8091/v1`. Its provider token
file contains the broker bearer.

### Recommended kagent install settings

Run governed agents in a dedicated kagent install:

- Allow only the broker to reach the public API.
- Set `config.runtimeDefaults.controlPlaneUrl` to a proxy that forwards only
  the `kagent.runtime.v1.TaskStoreService` methods.
- Restart the kagent controller after changing its config.

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
  - Each session holds at most 4096 prompts.
- **One request per prompt.** Each prompt sends a single request; brokered Orka
  tools are not wired yet.
- **kagent prerelease constraints apply:**
  - task-store identity is an unsigned header until Substrate #1660;
  - idle kagent Sessions expire after seven days by default.
