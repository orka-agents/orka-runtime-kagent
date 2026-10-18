module github.com/orka-agents/orka-runtime-kagent

go 1.27.0

toolchain go1.27.2

require (
	github.com/a2aproject/a2a-go/v2 v2.6.0
	github.com/kagent-dev/kagent/go v0.0.0-20261015233236-1eb89fb1b670
	google.golang.org/grpc v1.84.0
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260825204119-511051f7f437.2 // indirect
	github.com/agent-substrate/substrate v0.0.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	golang.org/x/mod v0.42.0 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20261005182115-fad411399dd8 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261005182115-fad411399dd8 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

// kagent v1 builds against its Substrate fork; its own replace is not inherited.
replace github.com/agent-substrate/substrate => github.com/kagent-dev/substrate v0.5.0-alpha2
