# executor

Praetor's **executor** service — the worker that turns a job launch into a real
Ansible run on a target host.

It binds the durable pull consumer on the [`eventbus`](https://github.com/praetordev/eventbus),
and for each `ExecutionRequest` it:

- bootstraps the target over SSH — pushing the checkpoint/resume callback plugin,
  the Execution Pack, and the run manifest (`core/bootstrap_runner.go`),
- runs the play through the pushed pack (the engine ships in the pack, never
  installed on the target), and
- streams job events and log chunks back to the ingestion service via
  [`ingestclient`](ingestclient/), which the consumer then persists.

It is a leaf deployable: nothing imports it. It depends only on the shared
`praetordev/*` libraries (`eventbus`, `events`, `hostconn`, `runtoken`, `metrics`,
`env`, `plog`).

## Layout

```
main.go            entrypoint
core/              agent loop, bootstrap, runner, publishers, inventory sync
ingestclient/      HTTP client for the ingestion service
deploy/            image assets: entrypoint.sh + the checkpoint callback plugin
```

`deploy/plugins/callback/praetor_checkpoint.py` is the target-side Ansible callback
that records task-level checkpoints for resume. The executor is its source of
truth: `bootstrap_runner` pushes it to each host at connect time, so the version
baked into this image is the version a target runs.

## Build the image

```
docker build -t praetor-executor:latest .
```

Stable image name (`praetor-executor`) so the Helm chart and k3d/kind load step are
unaffected by the repo split.

## Tests

```
go test ./...
```

## Secure run claims

New scheduler dispatches include a unique dispatch ID. Before processing one,
the executor claims the run through the scheduler's dedicated TLS 1.3 mutual-TLS
endpoint. A failed claim, missing client configuration, or invalid certificate
stops the run before credentials, inventory, or bootstrap data are accessed.
After a successful claim, credential-backed secure runs resolve their injectors
directly from Praetor Secrets over mTLS. Plaintext credentials do not pass
through the scheduler or ingestion service.

Configure the executor with:

| Setting | Purpose |
| --- | --- |
| `PRAETOR_SCHEDULER_CLAIM_URL` | HTTPS base URL of the scheduler claim listener |
| `PRAETOR_SCHEDULER_CA_FILE` | CA used to verify the scheduler listener |
| `PRAETOR_EXECUTOR_CERT_FILE` | Executor workload certificate containing its SPIFFE URI SAN |
| `PRAETOR_EXECUTOR_KEY_FILE` | Executor workload private-key file |
| `PRAETOR_SECRETS_URL` | HTTPS base URL of the Praetor Secrets service |
| `PRAETOR_SECRETS_CA_FILE` | CA used to verify the Praetor Secrets service |

The private key is accepted only as a file path. The executor requires TLS 1.3,
does not follow redirects, and does not send an identity header or bearer token;
the scheduler and secrets service derive the executor identity from the verified
certificate. The scheduler and secrets service may use different server CAs;
the executor workload certificate must be trusted by both services.
