# Build Stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /praetor-executor .

# NOTE: the executor does NOT build or ship a host-runner binary. Since the daemon
# ships INSIDE the Execution Pack (installed on the target from
# /opt/praetor/packs/<pack>/bin/praetor-host-runner), the pack — version-pinned by
# its spec's `host_runner` field and checksum-verified — is the SINGLE source of
# the daemon a target runs. A binary baked here would be a second, drifting source.

# Run Stage
FROM python:3.11-slim

# Install system dependencies
# git: for cloning
# openssh-client: for ssh connections
# libssl-dev, libffi-dev, build-essential: for compiling python deps if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    openssh-client \
    build-essential \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Ansible into the system python (dedicated container). The executor uses
# ansible-inventory here for inventory-sync runs; playbook execution happens in the
# pushed Execution Pack, not this image. Versions are PINNED for reproducible
# builds (issue #27); ansible-core matches the pack's engine (build/execpack specs).
RUN pip install --no-cache-dir pip==24.2 \
    && pip install --no-cache-dir \
        ansible-core==2.19.11 \
        ansible-runner==2.4.1 \
        PyMySQL==1.1.1

WORKDIR /

COPY --from=builder /praetor-executor /praetor-executor

# Ansible plugins shipped to target hosts at bootstrap (checkpoint callback for
# task-level resume). The executor is the source of truth for what a target runs,
# since bootstrap_runner pushes this file to the host at connect time.
COPY deploy/plugins /tmp/build/plugins

# Create a non-root user
RUN useradd -m -u 1000 praetor
RUN mkdir -p /home/praetor/.ssh && chown -R praetor:praetor /home/praetor/.ssh && chmod 700 /home/praetor/.ssh

ENV HOME=/home/praetor

# gosu lets the entrypoint drop privileges to the praetor user after fixing up
# root-owned volume mounts.
RUN apt-get update && apt-get install -y --no-install-recommends gosu && rm -rf /var/lib/apt/lists/*

COPY deploy/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Install collections as praetor user to ensure they are in ~/.ansible
USER praetor
RUN ansible-galaxy collection install ansible.posix community.general community.mysql
USER root

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/praetor-executor"]
