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
