# kagent runtime for Orka

Run your [kagent](https://github.com/kagent-dev/kagent) Agents under
[Orka](https://github.com/orka-agents/orka). kagent keeps running the Agent.
Orka adds what a shared platform needs around it: one way to send work, a
fresh session for every conversation, approvals before sensitive tool calls,
and an audit record of what happened.

> **Status: experimental.** kagent v0.10.3 is supported. kagent v1 is
> supported for one pinned prerelease build until kagent v1.0.0. See
> [Supported kagent versions](#supported-kagent-versions).

## New to kagent or Orka?

- **kagent** runs AI agents on Kubernetes. You describe an Agent, with its
  prompt, model, and tools, as a Kubernetes resource, and kagent runs it.
- **Orka** runs agent work on Kubernetes as Tasks, with policy, approvals, and
  audit built in.
- **This adapter** connects the two. Each kagent Agent gets an Orka Agent in
  front of it. You send work to the Orka Agent, and the adapter hands it to
  kagent and reports back what happened.

```
 you ──Task──► Orka ──────────────► adapter ──────────► your kagent Agent
               dispatch, approvals,  one kagent session
               audit, cleanup        per Orka session
```

## Already using kagent?

Most of what you have stays as it is:

- **Your Agents stay in kagent.** Agents, ModelConfigs, and MCP servers keep
  their kagent definitions. Orka imports them as they are, and an observed
  Agent needs no changes at all.
- **Work arrives through Orka.** Send it as an Orka Task instead of through
  kagent's UI or A2A endpoint. Each Orka session gets its own kagent user and
  session, and the adapter deletes that session when Orka is done with it.
- **The Agent is pinned.** The adapter runs exactly the Agent it was set up
  for. After you edit the Agent or its ModelConfig, or upgrade kagent, you
  re-pin it with a short scripted step. Until then its turns fail with
  `kagent_revision_mismatch`.
- **Approvals move to Orka.** kagent's own approval prompts
  (`requireApproval`) can't be answered through the adapter, so Agents that
  use them are refused. Use Orka approvals in [governed mode](#observed-or-governed)
  instead.
- **Your install needs a few NetworkPolicies.** kagent ships without them,
  and without them any pod could talk to an Orka-owned session directly. See
  [Prepare your kagent install](docs/kagent-install.md).
- **kagent v0 Agents run on the Go runtime with one replica.**

## Observed or governed

Every wrapped Agent runs in one of two modes. The import tells you which mode
each Agent qualifies for, and why.

| | Observed | Governed |
| --- | --- | --- |
| Changes to your Agent | none | its model and tools point at the adapter's relay |
| Who runs tool calls | kagent | Orka |
| Approvals before a tool runs | no | yes, for the tools you choose |
| What Orka records about tools | name and status | every call, in Orka's audit log |
| Model calls | kagent calls your provider | routed through Orka's model endpoint |
| Token usage | as kagent reports it | metered by Orka |

Start with observed: it works with your Agent as it is, and Orka still owns
dispatch, sessions, and cleanup. Move an Agent to governed when you want
Orka to approve, run, and record its tool calls.

## Supported kagent versions

| kagent | API | Status |
| --- | --- | --- |
| v0.10.3, Go runtime | `kagent.dev/v1alpha2` | supported |
| `1eb89fb1` (v1.0.0-alpha13+31) with Substrate fork v0.5.0-alpha2 | `api.kagent.dev/v1alpha3` | experimental until kagent v1.0.0 |

Before each turn, the adapter checks which kagent controller build it is
talking to, and refuses builds it was not verified against with
`kagent_build_unverified`. To use another build, run the
[release gate](conformance/README.md) against it, then accept its commit
with `--accept-kagent-commit` when you import.

## Getting started

You need:

- a Kubernetes cluster with a supported kagent and Orka installed;
- `kubectl` and the `orka` CLI;
- a checkout of [Orka](https://github.com/orka-agents/orka), for
  `scripts/kagent-runtime.sh`, and a checkout of this repository, which the
  script builds the adapter image from;
- a container registry your cluster can pull from.

The steps:

1. **Prepare your kagent install.** Apply the NetworkPolicies for your kagent
   version: [Prepare your kagent install](docs/kagent-install.md).

2. **Preview the import.** Export your kagent Agents and the objects they
   use, then run a dry run. It reports each Agent as observed or governed,
   with the reasons, and writes a deployment bundle:

   ```bash
   orka agent import kagent -f export.yaml --dry-run --bundle-dir bundle \
     --control-plane-url http://kagent-controller.kagent.svc:8083 \
     --unreachable kagent-postgresql.kagent.svc:5432
   ```

3. **Deploy the runtime.** This builds the adapter image, deploys it, and
   waits until Orka reports it Ready. A governed Agent also needs
   `MODEL_UPSTREAM_TOKEN_FILE`, a file holding Orka's credential for its model
   endpoint:

   ```bash
   KUBECTL_CONTEXT=<context> WRAPPER_SRC=<this checkout> IMAGE_REPO=<registry> \
     scripts/kagent-runtime.sh deploy bundle/<agent>
   ```

4. **Activate it.** Point the Orka Agent at the new runtime, using the import
   flags from step 2 without `--dry-run`:

   ```bash
   KUBECTL_CONTEXT=<context> scripts/kagent-runtime.sh activate bundle/<agent> -- \
     -f export.yaml <the other import flags>
   ```

5. **Send it work.**

   ```bash
   orka task create "Summarize the open alerts" --agent <agent>
   ```

   For a governed Agent, approve its tool calls as they come up:

   ```bash
   orka task approvals <task> --watch
   orka task approve <task> <approval-id>
   ```

The [Orka kagent guide](https://orka-agents.github.io/orka/docs/guides/kagent)
walks through every flag, including the governed-mode flags `--relay-url`,
`--model-upstream-url`, and `--allowed-tool`.

## Changing an Agent later

Edited the Agent, its ModelConfig, or kagent itself? Re-pin it with the same
steps: write a new bundle, `deploy`, `activate`, then `retire` the old
runtime. Old and new runtimes run side by side until you switch, and `retire`
waits for the old one's sessions to finish. The
[Orka kagent guide](https://orka-agents.github.io/orka/docs/guides/kagent#deploy-and-re-pin)
has the details.

## When a turn fails

A failed turn carries a short code:

| Code | What it means | What to do |
| --- | --- | --- |
| `kagent_revision_mismatch` | The Agent is no longer the one that was pinned, or, on kagent v0, its pod changed during the turn. | Re-pin the Agent if you changed it, then start a new Orka session. |
| `kagent_build_unverified` | The kagent controller build isn't one this adapter was verified against. | Use a supported kagent, or verify the build with the release gate and accept it with `--accept-kagent-commit`. |
| `kagent_isolation_broken` | The adapter reached a kagent component that should admit only kagent's controller. | Apply the NetworkPolicies in [Prepare your kagent install](docs/kagent-install.md). |
| `kagent_input_required` | The Agent paused for approval or input in kagent. | Remove kagent approval prompts; use Orka approvals instead. |
| `kagent_session_gone` | kagent deleted the session, or it expired after sitting idle. | Start a new Orka session. |
| `kagent_outcome_unknown` | The adapter couldn't confirm how the turn ended, for example because the Agent's pod died. | Retry in a new Orka session. Cleanup settles the old turn. |
| `kagent_turn_failed` | kagent reported that the turn failed. | Check the Agent's logs in kagent. |

A session that shows `quarantined` is waiting on proof of how its last turn
ended. [Troubleshooting](docs/troubleshooting.md) explains how to resolve it.

## Limitations

- **No repository work.** Wrapped Agents receive only the prompt, not the
  Task's repository. To have an Agent change code, translate it into a native
  Orka Agent with `orka agent import kagent --translate`.
- **No kagent approval prompts.** Use Orka approvals in governed mode.
- **One replica.** Each adapter runs one replica, and kagent v0 Agents must
  run one too.
- **Claude and Codex harnesses** on kagent v1 can be observed, not governed.
- **Idle kagent v1 sessions expire** after seven days by default.

## Learn more

- [How it works](docs/how-it-works.md): architecture, guarantees, governed
  mode, and how kagent v0 differs.
- [Prepare your kagent install](docs/kagent-install.md): NetworkPolicies and
  settings for kagent v1 and v0.
- [Configuration](docs/configuration.md): `kagent.json` and environment
  variables.
- [Troubleshooting](docs/troubleshooting.md): failure codes and quarantine
  runbooks.
- [Development](docs/development.md): building, testing, and the release gate.

## License

[MIT](LICENSE)
