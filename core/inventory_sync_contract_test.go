package core

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

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
