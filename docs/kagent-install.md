# Prepare your kagent install

kagent ships without NetworkPolicies, and kagent v0's default authentication
accepts any caller's user ID. A wrapped Agent relies on the settings below,
so apply the ones for your kagent version before you wrap an Agent. The
adapter checks part of this before every turn; see
[Admission checks](how-it-works.md#admission-checks).

`orka agent-runtime kagent deploy` creates each runtime's own objects: its
pod, labeled `orka.ai/kagent-broker: "true"` for the policies below, a
governed Agent's relay and the NetworkPolicy in front of it, and, for kagent
v0, the broker's read access to the Agent's pods. This page covers what you
set up once per kagent install.

## kagent v1

### Install settings

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
  [release gate](../conformance/README.md), then list its controller commit in
  `kagent.acceptedCommits`.
- List the router and kagent's database in `unreachable`, so the broker
  notices if this isolation is missing.
- Restart the kagent controller after changing its config.

### Governed Agents

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
- Only the kagent Agent's traffic reaches the relay. Deploy creates that
  NetworkPolicy.

On the Orka side, render the profile with the Orka tools the Agent may call
(`--allowed-tool`, `--approval-required-tool`).

## kagent v0

Run wrapped Agents in a dedicated kagent install, as for v1, with the Go
runtime (`declarative.runtime: go`) and one replica. Only kagent's controller
may change the Agents' Deployments: the import checks that each Deployment
loads its compiled configuration as kagent renders it, and the broker pins
the config hash and the image digest at run time. A governed verdict trusts
the Go runtime image your kagent install selected (`goAgentImage`) at import;
the import checks the image's name, and the pin keeps that exact image.

- **Isolation.** v0 ships no NetworkPolicies, its default `unsecure` auth mode
  accepts any `X-User-Id`, and its authorization is a no-op. Without these
  policies any pod can drive an Orka-owned session or read its tasks:

  ```yaml
  # Agent pods: kagent's controller, which proxies A2A, and the brokers,
  # which send to the pinned pod directly.
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
          - namespaceSelector:
              matchLabels: {kubernetes.io/metadata.name: orka-system}
            podSelector:
              matchLabels: {orka.ai/kagent-broker: "true"}
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

  Deploy labels each runtime pod `orka.ai/kagent-broker: "true"`. Turn off
  kagent's UI or keep it off this network: it calls the controller as any
  user.

  These policies trust any pod in the Orka namespace that carries that
  label, so only operators may create or label pods there, as for every
  other Orka component in it.
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
  with its pod's service account. Deploy grants it `get` and `list` on
  `pods` in the Agent's namespace, and mounts the token into the broker
  container only.
- **Governed v0 Agents** (the import checks these):
  - the model is an OpenAI `ModelConfig` with `openAI.baseUrl` set to the
    relay's `/v1`, `apiKeyPassthrough: true`, and the chat completions format;
  - every tool binding names a `RemoteMCPServer` at the relay's `/mcp`, with
    protocol `STREAMABLE_HTTP` and a `timeout` longer than `20m`;
  - each binding sets `allowedHeaders: [Authorization]`, so kagent forwards
    the turn token, and nothing sets `headersFrom`, which would override it;
  - no `requireApproval`, skills, `executeCodeBlocks`, memory, `shareTools`,
    Agent tools, or deployment environment overrides.

## Source control

Orka requires external runtimes to register strict workspace governance. The
Orka supervisor provides those guarantees. Two of the claims also depend on
the wrapped kagent Agent:

- `noDirectSCMPublication`
- `orkaOwnedCleanRoomPublication`

The operator must attest that the Agent has no tool able to publish to source
control directly.
