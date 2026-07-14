package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/praetordev/events"
	"github.com/praetordev/executor/ingestclient"
	"github.com/praetordev/hostconn"
	"github.com/praetordev/runtoken"
	"golang.org/x/crypto/ssh"
)

// BootstrapRunner deploys the self-contained Execution Pack (host-runner daemon +
// Python + Ansible) to a target host directly over SSH — no Ansible and no Python
// are required on the target. It pushes the pack, installs the daemon from it, and
// copies the checkpoint plugin, manifest and key over SSH, then launches the
// daemon. The target only needs sshd, a POSIX shell and tar.
type BootstrapRunner struct {
	// GiteaURL is the in-cluster base URL of the pack artifact store (generic
	// package registry). When set, packs are pulled from it over HTTP; empty
	// disables it and packs come only from RuntimeDir.
	GiteaURL   string
	GiteaOwner string
	// RuntimeDir is the legacy shared dir holding <pack>-linux-<arch>.tar.gz — a
	// fallback for packs not in the registry (execpack CLI / pre-built artifacts).
	RuntimeDir string
	// IngestionURL is where inventory-sync results are POSTed (the executor runs
	// those itself, with no host-runner). CallbackURL is the address the pushed
	// host-runner reports events/logs back to; it defaults to IngestionURL but can
	// differ when the target reaches ingestion by a different name than the executor.
	IngestionURL string
	CallbackURL  string
	// ingest is the shared ingestion client used for the run-scoped pre-flight
	// (runnable) and just-in-time credential resolution.
	ingest *ingestclient.Client
	// internalToken is the shared cluster secret (PRAETOR_INTERNAL_TOKEN). The
	// executor uses it both to mint each run's host-runner ingestion token (see
	// pkg/runtoken) and to authenticate its own in-cluster ingestion calls (e.g.
	// the inventory-sync upsert).
	internalToken string
}

// NewBootstrapRunner constructs the runner from resolved config values. All
// environment resolution happens in cmd/executor/main.go (the composition root),
// so this core type stays free of os.Getenv and testable with plain values.
func NewBootstrapRunner(giteaURL, giteaOwner, runtimeDir, ingestionURL, callbackURL, internalToken string, ingest *ingestclient.Client) *BootstrapRunner {
	if runtimeDir == "" {
		runtimeDir = "/tmp/build/runtime"
	}
	if giteaOwner == "" {
		giteaOwner = "praetor"
	}
	if callbackURL == "" {
		callbackURL = ingestionURL
	}
	return &BootstrapRunner{
		GiteaURL:      giteaURL,
		GiteaOwner:    giteaOwner,
		RuntimeDir:    runtimeDir,
		IngestionURL:  ingestionURL,
		CallbackURL:   callbackURL,
		ingest:        ingest,
		internalToken: internalToken,
	}
}

// fetchPackTarball resolves the pack's tarball for arch to a local file path,
// preferring the Gitea generic registry (the published artifact store) and
// falling back to the shared RuntimeDir (execpack CLI / pre-built packs). The
// returned cleanup removes any temp file it created (a no-op for the shared dir).
func (r *BootstrapRunner) fetchPackTarball(pack, arch string) (string, func(), error) {
	file := fmt.Sprintf("%s-linux-%s.tar.gz", pack, arch)
	noop := func() {}

	// 1. Gitea registry: GET .../generic/execpack-<pack>/current/<file> (anon).
	if r.GiteaURL != "" {
		url := fmt.Sprintf("%s/api/packages/%s/generic/execpack-%s/current/%s",
			strings.TrimRight(r.GiteaURL, "/"), r.GiteaOwner, pack, file)
		if resp, err := http.Get(url); err == nil {
			if resp.StatusCode == http.StatusOK {
				tmp, terr := os.CreateTemp("", "pack-*-"+file)
				if terr != nil {
					resp.Body.Close()
					return "", noop, terr
				}
				_, cerr := io.Copy(tmp, resp.Body)
				resp.Body.Close()
				tmp.Close()
				if cerr != nil {
					os.Remove(tmp.Name())
					return "", noop, fmt.Errorf("download Execution Pack %q (%s) from Gitea: %w", pack, arch, cerr)
				}
				logger.Info("fetched execution pack from Gitea", "pack", pack, "arch", arch)
				return tmp.Name(), func() { os.Remove(tmp.Name()) }, nil
			}
			resp.Body.Close() // non-200 (e.g. not published): fall through to shared dir
		}
	}

	// 2. Shared runtime dir fallback.
	shared := filepath.Join(r.RuntimeDir, file)
	if _, err := os.Stat(shared); err == nil {
		return shared, noop, nil
	}
	return "", noop, fmt.Errorf("Execution Pack %q (%s) not found: not in Gitea (%s) and no %s in runtime dir %s",
		pack, arch, r.GiteaURL, file, r.RuntimeDir)
}

func (r *BootstrapRunner) Run(req *events.ExecutionRequest, eventChan chan<- events.JobEvent) (err error) {
	// Bootstrap metrics by mode, observed on return.
	mode := "remote"
	if req.JobManifest.InventorySync {
		mode = "inventory_sync"
	} else if req.JobManifest.RunnerHost == "" {
		mode = "local"
	}
	bootStart := time.Now()
	BootstrapTotal.WithLabelValues(mode).Inc()
	defer func() {
		BootstrapDuration.WithLabelValues(mode).Observe(time.Since(bootStart).Seconds())
		if err != nil {
			BootstrapFailures.WithLabelValues(mode).Inc()
		}
	}()

	// Pre-flight: don't bootstrap a run the control plane has already given up on.
	// The executor is DB-free, so it asks ingestion (which owns the DB) whether the
	// run is still runnable. This closes the "ghost run" window where a launch was
	// reaped by the queued-timeout (or canceled) while sitting in the work queue and
	// is then delivered to a recovering executor. Fail-open: Runnable returns true
	// on any transport error, so a transient issue never blocks a legitimate job.
	if r.ingest != nil {
		if runnable, _ := r.ingest.Runnable(context.Background(), req.ExecutionRunID.String()); !runnable {
			logger.Info("run no longer runnable (already terminal); skipping bootstrap", "run_id", req.ExecutionRunID)
			return nil
		}
	}

	// Resolve the run's Machine credential just-in-time. The scheduler put only the
	// credential id in the manifest (no plaintext at rest in the outbox/NATS); the
	// executor is DB-free, so ingestion decrypts server-side and returns the
	// injectors, which we hold only in this in-memory manifest copy. A resolve
	// failure is fatal — bootstrapping without credentials would fail on the target
	// anyway, and failing here reports a clear cause.
	if req.JobManifest.CredentialID != 0 {
		if r.ingest == nil {
			return fmt.Errorf("run %s needs credential %d but no ingestion client is configured", req.ExecutionRunID, req.JobManifest.CredentialID)
		}
		creds, cerr := r.ingest.ResolveCredentials(context.Background(), req.ExecutionRunID.String())
		if cerr != nil {
			return fmt.Errorf("resolve credential %d for run %s: %w", req.JobManifest.CredentialID, req.ExecutionRunID, cerr)
		}
		req.JobManifest.CredentialEnv = creds.Env
		req.JobManifest.CredentialFiles = creds.Files
	}

	// Inventory-sync runs don't bootstrap a host-runner: the executor runs
	// ansible-inventory locally and upserts the result via ingestion.
	if req.JobManifest.InventorySync {
		return r.syncInventory(req, eventChan)
	}

	// Resolve the inventory just-in-time. The scheduler ships only the inventory id
	// (no large INI at rest in the outbox/NATS, #13); the executor is DB-free, so
	// ingestion renders the INI server-side. We hold it only in this in-memory
	// manifest copy, which is then pushed to the host-runner. A resolve failure is
	// fatal — the play can't run without its inventory.
	if req.JobManifest.InventoryID != 0 && req.JobManifest.Inventory == "" {
		if r.ingest == nil {
			return fmt.Errorf("run %s needs inventory %d but no ingestion client is configured", req.ExecutionRunID, req.JobManifest.InventoryID)
		}
		ini, ierr := r.ingest.ResolveInventory(context.Background(), req.JobManifest.InventoryID)
		if ierr != nil {
			return fmt.Errorf("resolve inventory %d for run %s: %w", req.JobManifest.InventoryID, req.ExecutionRunID, ierr)
		}
		req.JobManifest.Inventory = ini

		// Fact cache also travels by reference: fetch the inventory's stored facts
		// only when the job uses the cache, and fill them into this manifest copy
		// for the host-runner to preload (#48). Best-effort — a fact-cache miss
		// just means the play gathers facts fresh, so don't fail the run on it.
		if req.JobManifest.UseFactCache && len(req.JobManifest.CachedFacts) == 0 {
			if facts, ferr := r.ingest.ResolveFacts(context.Background(), req.JobManifest.InventoryID); ferr != nil {
				logger.Warn("resolve fact cache failed (play will gather facts)", "inventory_id", req.JobManifest.InventoryID, "run_id", req.ExecutionRunID, "err", ferr)
			} else if len(facts) > 0 {
				req.JobManifest.CachedFacts = facts
			}
		}
	}

	logger.Info("starting deployment", "run_id", req.ExecutionRunID)

	// Mint the per-run ingestion token the host-runner will present on its
	// events/logs/heartbeat/facts calls. It rides only in the 0600 manifest below;
	// ingestion recomputes and verifies it from the same shared secret + run id.
	req.JobManifest.IngestToken = runtoken.Mint(r.internalToken, req.ExecutionRunID.String())

	manifestBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	callbackURL := r.CallbackURL

	// A job with no inventory runs on the executor itself (localhost).
	if req.JobManifest.RunnerHost == "" {
		return r.localBootstrap(req, manifestBytes, callbackURL)
	}

	// Resolve how to reach the runner host from its inventory connection vars.
	vars := parseHostVars(req.JobManifest.Inventory, req.JobManifest.RunnerHost)
	addr := firstNonEmpty(vars["ansible_host"], req.JobManifest.RunnerHost)
	port := firstNonEmpty(vars["ansible_port"], "22")
	// Connection identity comes from the job's Machine credential (AAP-style): the
	// SSH user is the credential's username (ANSIBLE_REMOTE_USER) unless the host
	// pins its own ansible_user. Praetor ships no shared automation key or default
	// user — the operator owns the login account and its authorized_keys on the
	// host, and references the matching private key through a Machine credential.
	user := firstNonEmpty(vars["ansible_user"], req.JobManifest.CredentialEnv["ANSIBLE_REMOTE_USER"])
	if user == "" {
		return fmt.Errorf("no SSH user for runner host %s: assign a Machine credential (with a username) to the job template, or set ansible_user on the host", req.JobManifest.RunnerHost)
	}
	// When we log in as a non-root user, privileged bootstrap steps run via sudo.
	// This assumes the login user can escalate without a password; privilege
	// escalation for the play itself is configured on the credential (become).
	sudo := ""
	if user != "root" {
		sudo = "sudo "
	}

	// SSH key: strictly the Machine credential's key — no shared/platform fallback.
	keyContent := req.JobManifest.CredentialFiles["ANSIBLE_PRIVATE_KEY_FILE"]
	if keyContent == "" {
		return fmt.Errorf("no SSH key for runner host %s: assign a Machine credential with an SSH private key to the job template", req.JobManifest.RunnerHost)
	}
	if !strings.HasSuffix(keyContent, "\n") {
		keyContent += "\n" // SSH rejects a key file without a trailing newline
	}
	sshKeyPath := fmt.Sprintf("/tmp/cred-key-%s", req.ExecutionRunID)
	if err := os.WriteFile(sshKeyPath, []byte(keyContent), 0o600); err != nil {
		return fmt.Errorf("failed to write credential key: %w", err)
	}
	defer os.Remove(sshKeyPath)

	client, err := dialSSH(addr, port, user, sshKeyPath)
	if err != nil {
		return fmt.Errorf("ssh to runner host %s@%s:%s: %w", user, addr, port, err)
	}
	defer client.Close()
	logger.Info("connected to runner host", "user", user, "addr", addr, "port", port, "run_id", req.ExecutionRunID)

	runID := req.ExecutionRunID.String()
	jobDir := "/var/lib/praetor/jobs/" + runID

	// 1. Directories.
	if out, err := runSSH(client, fmt.Sprintf("%smkdir -p %s /opt/praetor /usr/local/share/praetor/plugins/callback", sudo, sshQuote(jobDir))); err != nil {
		return fmt.Errorf("mkdir on target: %w: %s", err, out)
	}

	// 2. The checkpoint callback plugin (task-level resume).
	if err := pushFile(client, "/tmp/plugins/callback/praetor_checkpoint.py", "/usr/local/share/praetor/plugins/callback/praetor_checkpoint.py", "0644", sudo); err != nil {
		logger.Warn("checkpoint plugin push failed (non-fatal)", "err", err)
	}

	// 3. The Execution Pack — the single self-contained bootstrapping unit
	// (host-runner daemon + Python + Ansible). Required now: the daemon ships
	// inside the pack, so a failed pack push is fatal rather than a soft fallback.
	pack := firstNonEmpty(req.JobManifest.ExecutionPack, "ansible-runtime")
	if err := r.pushRuntime(client, pack, sudo); err != nil {
		return fmt.Errorf("push Execution Pack %q (carries the host-runner daemon): %w", pack, err)
	}
	// Install the daemon from the pack to the stable path the launch command and
	// resume unit use — the pack is the source of the binary, not a separate push.
	hrSrc := fmt.Sprintf("/opt/praetor/packs/%s/bin/praetor-host-runner", pack)
	// Install atomically: copy to a temp path, chmod, then rename into place. A
	// plain `cp` truncates the destination in place, which fails with ETXTBSY if
	// another job's daemon is currently executing that inode; `mv` within the same
	// directory is a rename that swaps the directory entry without touching the
	// running process's inode.
	hrDst := "/usr/local/bin/praetor-host-runner"
	hrTmp := "/usr/local/bin/.praetor-host-runner.new"
	if out, err := runSSH(client, fmt.Sprintf("%scp %s %s && %schmod 0755 %s && %smv -f %s %s", sudo, hrSrc, hrTmp, sudo, hrTmp, sudo, hrTmp, hrDst)); err != nil {
		return fmt.Errorf("install host-runner from pack %q: %w: %s", pack, err, out)
	}

	// 4. The manifest.
	if err := pushBytes(client, manifestBytes, jobDir+"/manifest.json", "0600", sudo); err != nil {
		return fmt.Errorf("push manifest: %w", err)
	}

	// 5. The SSH key the host-runner uses for its downstream plays.
	if err := pushFile(client, sshKeyPath, jobDir+"/id_rsa", "0600", sudo); err != nil {
		return fmt.Errorf("push job key: %w", err)
	}

	// 6. The resume systemd unit (best-effort; skipped where systemd is absent).
	if out, err := runShellScript(client, sudo, resumeUnitScript); err != nil {
		logger.Warn("resume unit install skipped", "err", err, "output", out)
	}

	// 7. Launch the host-runner as root, detached so it outlives this SSH session.
	// Running the whole line under `sudo sh` keeps the log redirection root-owned.
	start := fmt.Sprintf(
		"setsid /usr/local/bin/praetor-host-runner --job-dir=%s --api-url=%s --run-id=%s >> %s/runner.log 2>&1 </dev/null &",
		jobDir, callbackURL, runID, jobDir,
	)
	if out, err := runShellScript(client, sudo, start); err != nil {
		return fmt.Errorf("start host-runner: %w: %s", err, out)
	}

	logger.Info("host-runner launched", "addr", addr, "run_id", req.ExecutionRunID)
	return nil
}

// pushRuntime streams the named Execution Pack onto the target and extracts it
// under /opt/praetor/packs/<pack>. It probes the host's CPU arch so it pushes the
// matching pack (<pack>-linux-<arch>.tar.gz); if it's already present it does
// nothing. Packs are name-scoped so several coexist on one host.
func (r *BootstrapRunner) pushRuntime(client *ssh.Client, pack, sudo string) error {
	detect, err := runSSH(client, fmt.Sprintf(`if [ -x /opt/praetor/packs/%s/bin/ansible-playbook ]; then echo present; else
  case "$(uname -m)" in aarch64|arm64) echo arm64 ;; x86_64|amd64) echo amd64 ;; *) echo unknown ;; esac
fi`, pack))
	if err != nil {
		return err
	}
	arch := strings.TrimSpace(detect)
	if arch == "present" {
		return nil
	}
	if arch == "unknown" || arch == "" {
		return fmt.Errorf("unsupported host CPU arch for Execution Pack")
	}
	tarball, cleanup, err := r.fetchPackTarball(pack, arch)
	if err != nil {
		return err
	}
	defer cleanup()
	f, err := os.Open(tarball)
	if err != nil {
		return fmt.Errorf("open Execution Pack %q for %s: %w", pack, arch, err)
	}
	defer f.Close()
	logger.Info("pushing execution pack", "pack", pack, "arch", arch)
	return pushStream(client, f, fmt.Sprintf("%smkdir -p /opt/praetor/packs && %star -xzf - -C /", sudo, sudo))
}

// localBootstrap runs the host-runner on the executor itself for jobs with no
// inventory (localhost execution).
func (r *BootstrapRunner) localBootstrap(req *events.ExecutionRequest, manifestBytes []byte, callbackURL string) error {
	runID := req.ExecutionRunID.String()
	jobDir := "/var/lib/praetor/jobs/" + runID
	if err := os.MkdirAll(jobDir, 0755); err != nil {
		return err
	}
	if err := os.WriteFile(jobDir+"/manifest.json", manifestBytes, 0600); err != nil {
		return err
	}
	logger.Info("no runner host - executing locally", "run_id", req.ExecutionRunID)
	// Localhost jobs run on the executor itself, but still source the daemon (and
	// Ansible runtime) from the pack — same as the remote path — so the daemon is
	// versioned via the pack rather than the executor image.
	pack := firstNonEmpty(req.JobManifest.ExecutionPack, "ansible-runtime")
	hostRunner, err := r.ensureLocalPack(pack)
	if err != nil {
		return fmt.Errorf("prepare Execution Pack %q locally: %w", pack, err)
	}
	cmd := exec.Command(hostRunner,
		"--job-dir="+jobDir, "--api-url="+callbackURL, "--run-id="+runID)
	logFile, _ := os.OpenFile(jobDir+"/runner.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if logFile != nil {
		cmd.Stdout, cmd.Stderr = logFile, logFile
	}
	return cmd.Start() // detached; do not wait
}

// ResumeLocalJobs recovers localhost runs after an executor restart. A local run
// executes the host-runner as a child of THIS container, writing its WAL to
// /var/lib/praetor/jobs (a persistent volume). On startup we relaunch the
// host-runner in --resume-root mode: it scans the root, skips already-terminal
// dirs, and for each interrupted job resumes from its on-disk WAL (checkpoint /
// task-level resume) while its syncers push the backlog. The resumed runner's
// first heartbeat revives a run the scheduler parked in 'reconciling' (see
// RecordHeartbeat), so a local job that outlived a control-plane blip completes
// normally instead of being falsely reported as an error (#45).
//
// Best-effort and detached: recovery must never block or fail executor startup.
func (r *BootstrapRunner) ResumeLocalJobs(root string) {
	if root == "" {
		root = "/var/lib/praetor/jobs"
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) == 0 {
		return // nothing to resume (fresh executor / no local runs)
	}
	// The resume needs a host-runner binary; localhost runs use the default pack.
	// Its own extracted pack (persisted) supplies ansible for any re-run.
	hostRunner, err := r.ensureLocalPack("ansible-runtime")
	if err != nil {
		logger.Warn("local job recovery skipped: host-runner unavailable", "err", err)
		return
	}
	cmd := exec.Command(hostRunner, "--resume-root="+root)
	if lf, ferr := os.OpenFile(filepath.Join(root, "resume.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); ferr == nil {
		cmd.Stdout, cmd.Stderr = lf, lf
	}
	if err := cmd.Start(); err != nil {
		logger.Warn("local job recovery: failed to start resume", "err", err)
		return
	}
	logger.Info("local job recovery started", "root", root, "dirs", len(entries))
	// Detached: the resume process outlives this call and runs alongside the agent.
}

// ensureLocalPack extracts the pack for the executor's own arch under
// /opt/praetor/packs/<pack> if absent, and returns the path to the bundled
// host-runner daemon. Mirrors the remote bootstrap: the daemon + runtime come
// from the pack, not a separately-shipped binary.
func (r *BootstrapRunner) ensureLocalPack(pack string) (string, error) {
	prefix := "/opt/praetor/packs/" + pack
	hostRunner := prefix + "/bin/praetor-host-runner"
	if _, err := os.Stat(prefix + "/bin/ansible-playbook"); err == nil {
		return hostRunner, nil // already extracted
	}
	tarball, cleanup, err := r.fetchPackTarball(pack, runtime.GOARCH)
	if err != nil {
		return "", err
	}
	defer cleanup()
	f, err := os.Open(tarball)
	if err != nil {
		return "", fmt.Errorf("open Execution Pack %q for %s: %w", pack, runtime.GOARCH, err)
	}
	defer f.Close()
	if err := os.MkdirAll("/opt/praetor/packs", 0o755); err != nil {
		return "", err
	}
	cmd := exec.Command("tar", "-xzf", "-", "-C", "/")
	cmd.Stdin = f
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("extract pack %q: %w: %s", pack, err, out)
	}
	return hostRunner, nil
}

// --- SSH helpers ---

func dialSSH(addr, port, user, keyPath string) (*ssh.Client, error) {
	keyBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read ssh key: %w", err)
	}
	return hostconn.Dial(addr, port, user, keyBytes)
}

// runSSH runs a command on the target and returns its combined output.
func runSSH(client *ssh.Client, cmd string) (string, error) { return hostconn.Run(client, cmd) }

// pushStream feeds r to a remote command's stdin (e.g. `cat > file` or
// `tar -xzf - -C /`), the primitive for copying files over SSH without Python.
func pushStream(client *ssh.Client, r io.Reader, remoteCmd string) error {
	sess, err := client.NewSession()
	if err != nil {
		return err
	}
	defer sess.Close()
	sess.Stdin = r
	var stderr bytes.Buffer
	sess.Stderr = &stderr
	if err := sess.Run(remoteCmd); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func pushFile(client *ssh.Client, local, remote, mode, sudo string) error {
	f, err := os.Open(local)
	if err != nil {
		return err
	}
	defer f.Close()
	return pushStream(client, f, writeCmd(remote, mode, sudo))
}

func pushBytes(client *ssh.Client, data []byte, remote, mode, sudo string) error {
	return pushStream(client, bytes.NewReader(data), writeCmd(remote, mode, sudo))
}

// writeCmd builds the remote command that receives a streamed file on stdin. As
// root it's `cat > file`; as a non-root user it uses `sudo tee` per step so the
// file is written with root privileges (no nested-quote gymnastics).
func writeCmd(remote, mode, sudo string) string {
	if sudo != "" {
		return fmt.Sprintf("%smkdir -p %s && %stee %s > /dev/null && %schmod %s %s",
			sudo, sshQuote(path.Dir(remote)), sudo, sshQuote(remote), sudo, mode, sshQuote(remote))
	}
	return fmt.Sprintf("mkdir -p %s && cat > %s && chmod %s %s",
		sshQuote(path.Dir(remote)), sshQuote(remote), mode, sshQuote(remote))
}

// runShellScript pipes a script to `sh` (or `sudo sh`) on stdin, avoiding quoting
// problems for multi-line scripts (systemd unit) and the detached launch line.
func runShellScript(client *ssh.Client, sudo, script string) (string, error) {
	sess, err := client.NewSession()
	if err != nil {
		return "", err
	}
	defer sess.Close()
	sess.Stdin = strings.NewReader(script)
	shell := "sh"
	if sudo != "" {
		shell = "sudo sh"
	}
	out, err := sess.CombinedOutput(shell)
	return string(out), err
}

// These delegate to pkg/hostconn so the executor and reconciler share one
// implementation (host-key policy, inventory parsing, quoting).
func sshQuote(s string) string            { return hostconn.Quote(s) }
func firstNonEmpty(vals ...string) string { return hostconn.FirstNonEmpty(vals...) }
func parseHostVars(inventory, host string) map[string]string {
	return hostconn.ParseHostVars(inventory, host)
}

const resumeUnitScript = `if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
  cat > /etc/systemd/system/praetor-resume.service <<'UNIT'
[Unit]
Description=Praetor host-runner - resume interrupted jobs after a restart
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/praetor-host-runner --resume-root=/var/lib/praetor/jobs
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
UNIT
  systemctl daemon-reload
  systemctl enable praetor-resume.service
fi`
