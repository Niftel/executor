package ingestclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveInventoryConsumesV1(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("testdata", "inventory_rendered.v1.ini"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/inventories/7/rendered" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer internal-token" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write(fixture)
	}))
	defer server.Close()

	got, err := New(server.URL, "internal-token").ResolveInventory(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(fixture) {
		t.Fatalf("inventory changed during resolution:\ngot:\n%s\nwant:\n%s", got, fixture)
	}
}

func TestResolveFactsConsumesV1(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("testdata", "inventory_facts.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/inventories/7/facts" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(fixture)
	}))
	defer server.Close()

	got, err := New(server.URL, "").ResolveFacts(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("resolved %d fact sets, want 2", len(got))
	}
	var web map[string]json.RawMessage
	if err := json.Unmarshal(got["web-1"], &web); err != nil {
		t.Fatalf("web-1 facts are not an object: %v", err)
	}
}
