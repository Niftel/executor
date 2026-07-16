package core

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

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
		got, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	runner := NewBootstrapRunner("", "", "", server.URL, "", "internal-token", nil)
	if err := runner.postInventorySync(7, fixture); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(fixture) {
		t.Fatalf("inventory sync payload changed in transit:\ngot  %s\nwant %s", got, fixture)
	}
}
