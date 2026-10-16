# syntax=docker/dockerfile:1
# Configured source image for one wrapped kagent Agent. The same image runs the
# ACP child (default command) and the lifecycle broker sidecar
# (--protocol broker). Orka layers its supervisor onto it with
# workers/acp/images/kagent/Dockerfile.
FROM --platform=$BUILDPLATFORM golang:1.27.2-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -buildvcs=false -trimpath -ldflags='-s -w' -o /out/orka-runtime-kagent ./cmd/orka-runtime-kagent
ARG KAGENT_CONFIG=examples/kagent.json
COPY ${KAGENT_CONFIG} /out/rootfs/agent/kagent.json
RUN chmod 0555 /out/rootfs/agent && chmod 0444 /out/rootfs/agent/kagent.json

FROM gcr.io/distroless/static:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7
COPY --from=build --chown=0:0 --chmod=0555 /out/orka-runtime-kagent /orka-runtime-kagent
COPY --from=build --chown=0:0 /out/rootfs/ /
USER 65532:65532
ENTRYPOINT ["/orka-runtime-kagent"]
CMD ["--protocol", "acp", "--config", "/agent/kagent.json"]
