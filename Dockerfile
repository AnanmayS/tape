# Tape ingester.
#
# Two stages. The build stage is a full Go toolchain; the final image is
# distroless static, which is a root filesystem containing the CA bundle, a
# passwd entry for a non-root user, /tmp, and nothing else — no shell, no
# package manager, no libc. Nothing in this binary needs any of them: it is a
# static CGO-free Go program, and the one thing it does need from a base image
# is the certificate bundle, without which every wss:// dial to the exchange and
# every HTTPS call to S3 and CloudWatch fails certificate verification.

FROM golang:1.27 AS build

WORKDIR /src

# Dependencies first, so a source-only change does not re-download the module
# graph. go.sum is committed, so this step is reproducible.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 is what makes the binary static and therefore what makes a
# distroless base possible. -trimpath keeps build paths out of the binary so
# that two builds of the same commit produce the same bytes; this project cares
# about that property elsewhere and there is no reason to abandon it here.
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/tape ./cmd/tape

# Fail the build rather than the deploy if the binary will not run. Skipped
# when cross-compiling, where the builder cannot execute what it just produced.
RUN if [ "${TARGETARCH}" = "$(go env GOHOSTARCH)" ]; then /out/tape help >/dev/null; fi

FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="tape" \
      org.opencontainers.image.description="Market data capture and deterministic replay" \
      org.opencontainers.image.source="https://github.com/AnanmayS/tape"

COPY --from=build /out/tape /usr/local/bin/tape

# 65532:65532, the distroless nonroot user. The ingester opens outbound sockets
# and writes files under -dir; neither needs a privileged uid, and on Fargate
# the alternative is a root process on a task that talks to the public internet.
USER nonroot:nonroot

# The entrypoint is the subcommand; everything else is flags, which the ECS task
# definition supplies as the container command. Configuration lives in the
# deployment, so changing a rotation window is an apply rather than a rebuild.
ENTRYPOINT ["/usr/local/bin/tape", "capture"]

# Defaults for a bare `docker run`, overridden in full by the task definition.
# -dir is under /tmp because that is the one writable path in this image, and on
# Fargate it is ephemeral task storage in any case.
CMD ["-dir", "/tmp/tape", "-log", "json"]
