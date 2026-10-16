# Release gate

`release-gate.sh` decides whether one exact version tuple is supported:

- kagent
- the ADK and MCP SDK in its harness image
- Substrate
- agentgateway

Run it on a test cluster before declaring a tuple supported. A tuple that has
not passed it is unsupported, whatever its version numbers.

## What it checks

| Check | Proves |
| --- | --- |
| controller settings | kagent disables skill bash, forwards the turn token, and sends agents through the control-plane filter. |
| observed turn | kagent runs its own tool; Orka sees the call and settles the turn. |
| deadline cancellation | A Task deadline cancels the in-flight kagent tool call. |
| delete cancellation | Deleting a Task mid-tool settles the kagent work and retires the session. |
| governed turn | Orka gates the governed Agent's tool call on approval, executes it, and counts the model's tokens. |
| approval expiry (`LONG=1`) | An expired approval never executes the tool. |
| relay authentication | The relay refuses callers without a live turn token. |
| control-plane filter | Agents reach exactly the five TaskStore methods, each answered by kagent's own server. `MemoryService` and unlisted TaskStore methods are denied. |
| compiled egress | The governed revision's egress lists the relay and the control-plane filter, and nothing else but the accepted provider default hosts. |
| timeout chain | The governed `RemoteMCPServer` waits strictly longer than the relay's 20 minutes. |
| router ingress | Only kagent's controller can reach Substrate's actor router; a pod elsewhere gets no response. |
| session retirement | Every kagent session Orka created has been deleted. |

The broker's unit tests cover what the cluster cannot inject, including:

- crashes at each lifecycle point;
- turn tokens reused across sessions, across turns, and after cancellation;
- ambiguous sends and deleted sessions.

## Fixtures

The gate expects the probe fixtures used during verification:

- **`probe-model`:** an OpenAI-compatible model stub that calls any tool
  whose name contains `probe`.
- **`probe-mcp`:** an MCP tool with an adjustable delay
  (`/admin/sleep?s=N`), plus `/tool`, which backs the Orka HTTP Tool. It
  counts `/tool` calls in memory (`/admin/tool-calls`), and it logs the
  calling kagent user on every call and cancellation.
- **`probe-cpfilter`:** the control-plane filter, with `ALLOW_PATHS` set to
  the five TaskStore methods.
- **Observed setup:** an Agent wrapped by an observed runtime.
- **Governed setup:** an Agent wrapped by a governed runtime, plus the
  approval-gated Orka Tool it calls.

## Requirements on the run

The gate needs the cluster to itself. It ties each piece of evidence to the
Task under test by the kagent user, which is unique per Orka session. It fails
rather than guesses if other sessions call the probe fixtures during a check.

The router-ingress check starts a short-lived curl pod in `OUTSIDER_NS`
(default `default`), a namespace the router must not admit, and deletes it
afterwards. That pod must be able to reach `OUTSIDER_CONTROL_URL` (default:
the control-plane filter), which proves it has networking. The check first
confirms through `kubectl port-forward` that the router listens on every
Service port, then probes the Service's cluster IP so that no name has to
resolve.

## Running it

```sh
export KUBECONFIG=<test cluster>
LONG=1 ORKA=./orka.sh conformance/release-gate.sh   # qualifies a tuple, about 20 minutes
ORKA=./orka.sh conformance/release-gate.sh          # quick check, skips approval expiry
```

Only a `LONG=1` run that passes every check qualifies a tuple. The script
exits 0 then, 1 when any check fails, and 2 when checks were skipped.

`ORKA` must be an `orka` CLI command already bound to the Orka API server and
a token that can create Tasks and decide approvals. The other settings, such
as namespaces and Agent names, default to the verification cluster's names.
Override them through the environment variables listed at the top of the
script.

## kagent v0.x

`KAGENT_VERSION=v0` runs the checks for a kagent v0.x tuple: kagent, the Go
ADK image, and Kubernetes. The v1 checks for the control-plane filter, the
compiled egress, and the router do not apply. These run instead:

| Check | Proves |
| --- | --- |
| compiled v0 settings | The governed Agent runs one Go replica. Its compiled config forwards the turn token to the relay for the model and every tool, and enables no approvals, code execution, memory, or Agent tools. |
| observed, deadline, and delete checks | As for v1. |
| governed turn, twice | The second turn reuses the MCP session the Agent's process opened in the first. |
| approval expiry and relay authentication | As for v1. |
| v0 ingress | A pod elsewhere reaches neither the Agents, the controller, nor its database. |
| timeout chain | As for v1, on the `kagent.dev` RemoteMCPServer. |
| config pin | Changing the observed Agent's ModelConfig changes its config hash, and the wrapper refuses the Agent until the original config is back. The check restores the ModelConfig. |
| session retirement | Every kagent session Orka created is marked deleted. |

The observed Agent's tool binding must set `allowedHeaders: [X-User-Id]`.
kagent v0 forwards the calling user to an MCP server only when a binding
allows it, and the gate ties probe evidence to that user.

```sh
KAGENT_VERSION=v0 LONG=1 ORKA=./orka.sh conformance/release-gate.sh
```
