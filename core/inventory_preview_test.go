package core

import (
	"strings"
	"testing"
)

func TestSummarizeInventoryPreview(t *testing.T) {
	payload := []byte(`{"_meta":{"hostvars":{"web-2":{},"web-1":{}}},"web":{"hosts":["web-1","web-2"]}}`)
	got, err := summarizeInventoryPreview(payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"host_count":2`, `"group_count":1`, `"sample_hosts":["web-1","web-2"]`} {
		if !strings.Contains(got, want) {
			t.Fatalf("summary %s missing %s", got, want)
		}
	}
}

func TestSummarizeInventoryPreviewRejectsInvalidJSON(t *testing.T) {
	if _, err := summarizeInventoryPreview([]byte("not-json")); err == nil {
		t.Fatal("expected invalid JSON error")
	}
}

func TestBoundedWriterRejectsOversizedOutput(t *testing.T) {
	var writer boundedWriter
	if _, err := writer.Write(make([]byte, inventoryPreviewOutputLimit+1)); err == nil {
		t.Fatal("expected output limit error")
	}
}
