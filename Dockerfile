# Build Stage — compile on the native CI runner instead of emulating the target.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /praetor-executor .

# NOTE: the executor does NOT build or ship a host-runner binary. Since the daemon
# ships INSIDE the Execution Pack (installed on the target from
# /opt/praetor/packs/<pack>/bin/praetor-host-runner), the pack — version-pinned by
# its spec's `host_runner` field and checksum-verified — is the SINGLE source of
# the daemon a target runs. A binary baked here would be a second, drifting source.

# Run Stage
FROM python:3.14-slim@sha256:cea0e6040540fb2b965b6e7fb5ffa00871e632eef63719f0ea54bca189ce14a6

# Install system dependencies
# git: for cloning
# openssh-client: for ssh connections
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Install Ansible into the system python (dedicated container). The executor uses
# ansible-inventory here for inventory-sync runs; playbook execution happens in the
# pushed Execution Pack, not this image. Versions are PINNED for reproducible
# builds (issue #27); ansible-core matches the pack's engine (build/execpack specs).
RUN pip install --no-cache-dir \
        pip==26.0.1 \
        setuptools==82.0.1 \
        wheel==0.47.0 \
    && pip install --no-cache-dir \
        ansible-core==2.19.11 \
        ansible-runner==2.4.1 \
        PyMySQL==1.1.1

WORKDIR /

# Create a non-root user
RUN useradd -m -u 1000 praetor
RUN mkdir -p /home/praetor/.ssh && chown -R praetor:praetor /home/praetor/.ssh && chmod 700 /home/praetor/.ssh

ENV HOME=/home/praetor

# Install locked collections before copying frequently changing application
# artifacts, so normal Go/plugin edits retain the expensive runtime cache.
COPY deploy/collections-requirements.yml /tmp/build/collections-requirements.yml
USER praetor
RUN ansible-galaxy collection install --no-deps -r /tmp/build/collections-requirements.yml
USER root

COPY --from=builder /praetor-executor /praetor-executor

# Ansible plugins shipped to target hosts at bootstrap (checkpoint callback for
# task-level resume). The executor is the source of truth for what a target runs,
# since bootstrap_runner pushes this file to the host at connect time.
COPY deploy/plugins /tmp/build/plugins

COPY deploy/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/praetor-executor"]
