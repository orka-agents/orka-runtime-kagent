# Development

## Repository layout

| Path | What it holds |
| --- | --- |
| `cmd/orka-runtime-kagent` | The binary. It runs as the ACP child or as the lifecycle broker. |
| `internal/acp` | The ACP child the Orka supervisor launches for each runtime session. |
| `internal/broker` | The lifecycle broker: the session ledger, turn dispatch, and the governed-mode relay. |
| `internal/kagent` | Clients for kagent v1 and v0, and usage parsing. |
| `internal/config` | Decoding of the mounted `kagent.json`. |
| `internal/strictjson`, `internal/durablestore` | Copied from [orka-runtime-foundry](https://github.com/orka-agents/orka-runtime-foundry). |
| `conformance` | The [release gate](../conformance/README.md) that qualifies a kagent version tuple. |
| `examples` | A sample `kagent.json`. |

## Checks

CI runs these on every pull request:

```sh
test -z "$(gofmt -l .)"
go vet ./...
go test -race ./...
golangci-lint run          # v2.14.0
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
hack/check-copied-code.sh <orka-runtime-foundry checkout>
```

The non-test files of `internal/strictjson` and `internal/durablestore` must
match orka-runtime-foundry apart from the module path. CI compares them against the
commit they were last synced from, pinned in `.github/workflows/ci.yml`. To
change them, change orka-runtime-foundry first, copy the packages again, and
move that pin.

## Build

The image is the adapter alone; each runtime mounts its configuration.

```sh
docker build -t <registry>/orka-runtime-kagent:<tag> .
```

Orka's kagent runtime image layers the supervisor onto it, pinned by digest:

```sh
make docker-build-acp-kagent-runtime \
  KAGENT_ADAPTER_IMAGE=<registry>/orka-runtime-kagent@sha256:<digest> \
  KAGENT_ADAPTER_DIGEST=sha256:<same-digest> \
  ACP_KAGENT_RUNTIME_IMG=<registry>/acp-kagent-runtime:<tag>
```

Then deploy a bundle with it: `orka agent-runtime kagent deploy <bundle>
--image <registry>/acp-kagent-runtime:<tag>`.

## Publishing

The Publish workflow pushes `ghcr.io/orka-agents/orka-runtime-kagent` for
linux/amd64 and linux/arm64: every main commit as `sha-<commit>`, and every
`v*` tag as its version without the v. Orka pins the digest it builds its
kagent runtime image on in `workers/acp/images/kagent/Dockerfile`.

## Supporting a new kagent version

Run the [release gate](../conformance/README.md) against the new version
tuple on a test cluster. A tuple is supported only after a `LONG=1` run
passes every check. Then add its controller commit to `verifiedCommits` in
`internal/kagent/versions.go`.

## Design notes

- [Repository workflow](repository-workflow.md): a proposed design for letting
  governed Agents change repositories through Orka.
