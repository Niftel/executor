package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"

	"github.com/praetordev/events"
)

const inventoryPreviewOutputLimit = 1 << 20

type boundedWriter struct {
	buf bytes.Buffer
	n   int64
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	if w.n+int64(len(p)) > inventoryPreviewOutputLimit {
		return 0, fmt.Errorf("inventory output exceeds %d bytes", inventoryPreviewOutputLimit)
	}
	w.n += int64(len(p))
	return w.buf.Write(p)
}

// syncInventory runs `ansible-inventory --list` against the source and POSTs the
// resulting JSON to ingestion, which upserts hosts/groups into the inventory.
// The executor emits the lifecycle events itself (there is no host-runner here).
func (r *BootstrapRunner) syncInventory(req *events.ExecutionRequest, eventChan chan<- events.JobEvent) error {
	m := req.JobManifest
	logger.Info("inventory sync starting", "run_id", req.ExecutionRunID, "inventory_id", m.SyncInventoryID)
	eventChan <- events.JobEvent{
		ExecutionRunID: req.ExecutionRunID, UnifiedJobID: req.UnifiedJobID,
		Seq: 1, EventType: "JOB_STARTED", Timestamp: time.Now(),
	}

	dir, err := os.MkdirTemp("", "praetor-sync-")
	if err != nil {
		return r.syncFail(req, eventChan, err)
	}
	defer os.RemoveAll(dir)

	// A plugin/static config is a .yml file; a script is an executable.
	name, mode := "source.yml", os.FileMode(0644)
	if m.InventorySourceKind == "script" {
		name, mode = "source", os.FileMode(0o755)
	}
	srcPath := filepath.Join(dir, name)
	if err := os.WriteFile(srcPath, []byte(m.InventorySource), mode); err != nil {
		return r.syncFail(req, eventChan, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ansible-inventory", "-i", srcPath, "--list")
	// Apply credential injectors so the inventory plugin can authenticate.
	env := os.Environ()
	for k, v := range m.CredentialEnv {
		env = append(env, k+"="+v)
	}
	for k, content := range m.CredentialFiles {
		// k is an env var name (alnum/underscore), safe to use as a filename.
		fp := filepath.Join(dir, "cred_"+k)
		if err := os.WriteFile(fp, []byte(content), 0o600); err != nil {
			return r.syncFail(req, eventChan, fmt.Errorf("writing credential file %s: %w", k, err))
		}
		env = append(env, k+"="+fp)
	}
	cmd.Env = env
	var stdout, stderr boundedWriter
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	out := stdout.buf.Bytes()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return r.syncFail(req, eventChan, fmt.Errorf("ansible-inventory timed out after 60 seconds"))
		}
		return r.syncFail(req, eventChan, fmt.Errorf("ansible-inventory failed: %v: %s", err, stderr.buf.String()))
	}

	if m.InventoryPreview {
		msg, err := summarizeInventoryPreview(out)
		if err != nil {
			return r.syncFail(req, eventChan, err)
		}
		eventChan <- events.JobEvent{ExecutionRunID: req.ExecutionRunID, UnifiedJobID: req.UnifiedJobID, Seq: 2, EventType: "JOB_COMPLETED", Timestamp: time.Now(), StdoutSnippet: &msg}
		logger.Info("inventory preview complete", "run_id", req.ExecutionRunID)
		return nil
	}

	if err := r.postInventorySync(m.SyncInventoryID, out); err != nil {
		return r.syncFail(req, eventChan, err)
	}

	msg := fmt.Sprintf("Inventory %d synced", m.SyncInventoryID)
	eventChan <- events.JobEvent{
		ExecutionRunID: req.ExecutionRunID, UnifiedJobID: req.UnifiedJobID,
		Seq: 2, EventType: "JOB_COMPLETED", Timestamp: time.Now(), StdoutSnippet: &msg,
	}
	logger.Info("inventory sync complete", "run_id", req.ExecutionRunID)
	return nil
}

func summarizeInventoryPreview(payload []byte) (string, error) {
	var inventory map[string]json.RawMessage
	if err := json.Unmarshal(payload, &inventory); err != nil {
		return "", fmt.Errorf("inventory preview returned invalid JSON: %w", err)
	}
	hostSet := map[string]struct{}{}
	if raw, ok := inventory["_meta"]; ok {
		var meta struct {
			HostVars map[string]json.RawMessage `json:"hostvars"`
		}
		_ = json.Unmarshal(raw, &meta)
		for host := range meta.HostVars {
			hostSet[host] = struct{}{}
		}
	}
	groups := 0
	for name, raw := range inventory {
		if name == "_meta" {
			continue
		}
		groups++
		var group struct {
			Hosts []string `json:"hosts"`
		}
		_ = json.Unmarshal(raw, &group)
		for _, host := range group.Hosts {
			hostSet[host] = struct{}{}
		}
	}
	hosts := make([]string, 0, len(hostSet))
	for host := range hostSet {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)
	if len(hosts) > 25 {
		hosts = hosts[:25]
	}
	summary, _ := json.Marshal(map[string]interface{}{"preview": true, "host_count": len(hostSet), "group_count": groups, "sample_hosts": hosts, "truncated": len(hostSet) > len(hosts)})
	return string(summary), nil
}

func (r *BootstrapRunner) postInventorySync(inventoryID int64, payload []byte) error {
	ingestion := r.IngestionURL
	if ingestion == "" {
		ingestion = "http://ingestion:8081"
	}
	url := fmt.Sprintf("%s/api/v1/inventories/%d/sync-data", ingestion, inventoryID)
	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("building sync-data request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	// sync-data is an in-cluster, authenticated ingestion endpoint; the executor
	// presents the shared internal token (the host-runner is not involved here).
	if r.internalToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+r.internalToken)
	}
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("posting sync data: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("ingestion upsert returned %d", resp.StatusCode)
	}
	return nil
}

func (r *BootstrapRunner) syncFail(req *events.ExecutionRequest, eventChan chan<- events.JobEvent, cause error) error {
	logger.Error("inventory sync failed", "run_id", req.ExecutionRunID, "err", cause)
	msg := cause.Error()
	eventChan <- events.JobEvent{
		ExecutionRunID: req.ExecutionRunID, UnifiedJobID: req.UnifiedJobID,
		Seq: 2, EventType: "JOB_FAILED", Timestamp: time.Now(), StdoutSnippet: &msg,
	}
	return cause
}
