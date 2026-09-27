# Static binary on a distroless base.
#
# Never built yet. The `image` job in .github/workflows/ci.yml is configured
# to build it and run `--demo --once` inside it, but that job has not run
# (the branch that adds it has not been pushed), and there is no Docker
# daemon on the development machine. No image is published.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
        -o /out/ib-slurm-exporter ./cmd/ib-slurm-exporter \
    && mkdir -p /out/tmp && chmod 1777 /out/tmp

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ib-slurm-exporter /ib-slurm-exporter
# --demo writes its synthetic tree under $TMPDIR.
COPY --from=build /out/tmp /tmp
# Root on purpose: reading other users' /proc/<pid>/fd needs
# CAP_DAC_READ_SEARCH (to list the mode-0500 fd directory) and
# CAP_SYS_PTRACE (to read its links), and a non-root process only gets those
# through file or ambient capabilities, which this image does not set. The
# DaemonSet in deploy/kubernetes drops every capability but those two.
USER 0
EXPOSE 9836
ENTRYPOINT ["/ib-slurm-exporter"]
