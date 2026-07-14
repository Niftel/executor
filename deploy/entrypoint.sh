#!/bin/sh
set -e

# NOTE: the executor does NOT stage a shared SSH private key into its home. Each
# job's connection key comes from its Machine credential, resolved at dispatch and
# written to a per-run ephemeral file (/tmp/cred-key-<run_id>, removed after the
# run) — see services/executor/core/bootstrap_runner.go. A job with no Machine
# credential is rejected, so there is no default-identity fallback to stage here.
# Keeping the key out of the container's home shrinks the key-exposure surface.

mkdir -p /home/praetor/.ssh
chown -R praetor:praetor /home/praetor/.ssh

# SSH host-key policy: trust-on-first-use. accept-new records an unknown host's
# key on first connect but REJECTS a changed key thereafter (MITM protection),
# and known_hosts lives on the persisted praetor-ssh volume so trust survives
# restarts. (Previously StrictHostKeyChecking=no + UserKnownHostsFile=/dev/null
# disabled verification entirely, defeating that volume.)
cat <<EOF > /home/praetor/.ssh/config
Host *
    StrictHostKeyChecking accept-new
    LogLevel ERROR
EOF
chmod 600 /home/praetor/.ssh/config
chown praetor:praetor /home/praetor/.ssh/config

# Localhost jobs run the host-runner on the executor itself: it writes job dirs
# under /var/lib/praetor and extracts the Execution Pack under /opt/praetor/packs.
# Both are persistent volumes (for crash recovery, #45) and are mounted root-owned,
# so chown the mount points to the runtime user (the process runs as praetor via
# gosu) or pack extraction and WAL writes fail.
mkdir -p /var/lib/praetor /opt/praetor/packs
chown praetor:praetor /var/lib/praetor /opt/praetor /opt/praetor/packs

# Drop privileges and exec
exec gosu praetor "$@"
