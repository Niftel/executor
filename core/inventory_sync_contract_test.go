package core

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/praetordev/events"
)

func TestInventoryScriptExitStatusIsAuthoritative(t *testing.T) {
	script := filepath.Join(t.TempDir(), "inventory-script")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 23\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := inventoryAcquisitionCommand(context.Background(), "script", script).Run(); err == nil {
		t.Fatal("failing inventory script was treated as successful")
	}
}

func TestInventoryHeartbeatRunsUntilCanceled(t *testing.T) {
	var calls atomic.Int64
	runID := uuid.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/runs/"+runID.String()+"/heartbeat" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer internal-token" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	runner := NewBootstrapRunner("", "", "", server.URL, "", "internal-token", nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runner.runInventoryHeartbeat(ctx, runID, 10*time.Millisecond)
	}()

	deadline := time.Now().Add(time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := calls.Load(); got < 3 {
		t.Fatalf("heartbeat calls = %d, want at least 3", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat loop did not stop after cancellation")
	}
	stoppedAt := calls.Load()
	time.Sleep(30 * time.Millisecond)
	if got := calls.Load(); got != stoppedAt {
		t.Fatalf("heartbeat continued after cancellation: calls = %d, want %d", got, stoppedAt)
	}
}

func TestInventoryHeartbeatIntervalPrecedesValidationStaleGrace(t *testing.T) {
	const validationStaleGrace = 10 * time.Second
	if inventoryHeartbeatInterval >= validationStaleGrace {
		t.Fatalf("inventory heartbeat interval %s must be shorter than stale grace %s", inventoryHeartbeatInterval, validationStaleGrace)
	}
}

func TestPostInventorySyncMatchesV1Contract(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("testdata", "inventory_sync.v1.json"))
	if err != nil {
		t.Fatal(err)
	}

	var got []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/inventories/7/sync-data" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content type = %q", r.Header.Get("Content-Type"))
		}
		if r.Header.Get("Authorization") != "Bearer internal-token" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("X-Praetor-Unified-Job-ID") != "42" {
			t.Errorf("job id header = %q", r.Header.Get("X-Praetor-Unified-Job-ID"))
		}
		if r.Header.Get("X-Praetor-Execution-Run-ID") == "" {
			t.Error("execution run id header is missing")
		}
		got, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	runner := NewBootstrapRunner("", "", "", server.URL, "", "internal-token", nil)
	req := &events.ExecutionRequest{UnifiedJobID: 42, ExecutionRunID: uuid.New(), JobManifest: events.JobManifest{SyncInventoryID: 7}}
	if err := runner.postInventorySync(req, fixture); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(fixture) {
		t.Fatalf("inventory sync payload changed in transit:\ngot  %s\nwant %s", got, fixture)
	}
}
