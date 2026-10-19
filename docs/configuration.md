# Configuration

`orka agent import kagent` and `orka agent-runtime kagent deploy` write this
configuration for you. This page is a reference for reading or debugging a
deployed runtime.

Each runtime mounts its Agent's `/agent/kagent.json` from a ConfigMap. Both
processes check its SHA-256 against
`ORKA_KAGENT_ACP_AGENT_CONFIGURATION_DIGEST`.

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
  is the decimal `kagent.dev/config-hash` of the Agent's pod, and
  `kagent.expectedImageDigest` is the `sha256:` digest its container status
  reports.
- `kagent.acceptedCommits` lists extra controller builds, by git commit, that
  the operator verified. See [Admission checks](how-it-works.md#admission-checks).
- `unreachable` lists `host:port` addresses the broker must fail to connect
  to, for example `atenet-router.ate-system.svc:8080` and
  `kagent-postgresql.kagent.svc:5432`.

| Process | Variable | Purpose |
| --- | --- | --- |
| both | `ORKA_KAGENT_ACP_MODEL`, `ORKA_KAGENT_ACP_AGENT_CONFIGURATION_DIGEST` | Must match the mounted configuration |
| child | `ORKA_KAGENT_ACP_PROVIDER_BASE_URL`, `ORKA_KAGENT_ACP_PROVIDER_TOKEN` | Set by the supervisor (loopback proxy) |
| broker | `ORKA_KAGENT_BROKER_ADDR` | Loopback listen address, default `127.0.0.1:8091` |
| broker | `ORKA_KAGENT_BROKER_STATE_DIR` | Absolute path on a broker-only persistent volume |
| broker | `ORKA_KAGENT_BROKER_BEARER_TOKEN` | At least 32 bytes; shared only with the supervisor's provider token file |
| broker, governed | `ORKA_KAGENT_RELAY_ADDR` | Relay listen address, default `:8092` |
| broker, governed | `ORKA_KAGENT_MODEL_UPSTREAM_TOKEN` | Orka's credential for `upstreamBaseURL`; broker only |

The supervisor runs with `ORKA_ACP_PROVIDER=kagent` and with
`ORKA_ACP_PROVIDER_PROXY_BASE_URL=http://127.0.0.1:8091/v1`. Its provider token
file contains the broker bearer.

The `governed` section is described in
[Governed mode](how-it-works.md#governed-mode).
