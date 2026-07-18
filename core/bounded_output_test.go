package core

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestBoundedErrorTextPreservesContextAndDiagnosticTail(t *testing.T) {
	value := "bootstrap failed: " + strings.Repeat("x", maxExecutorErrorBytes*2) + " শেষ failure"
	got := boundedErrorText(value)
	if len(got) > maxExecutorErrorBytes {
		t.Fatalf("bounded error is %d bytes, limit is %d", len(got), maxExecutorErrorBytes)
	}
	if !utf8.ValidString(got) {
		t.Fatal("bounded error is not valid UTF-8")
	}
	if !strings.HasPrefix(got, "bootstrap failed: ") || !strings.Contains(got, truncationMarker) || !strings.HasSuffix(got, "শেষ failure") {
		t.Fatalf("bounded error lost context or tail: %q", got)
	}
}

func TestBoundedTailBufferRetainsOnlyValidDiagnosticTail(t *testing.T) {
	buffer := newBoundedTailBuffer(12)
	_, _ = buffer.Write([]byte(strings.Repeat("x", 32)))
	_, _ = buffer.Write([]byte(" শেষ"))
	got := buffer.String()
	if len(got) > 12 || !utf8.ValidString(got) || !strings.HasSuffix(got, "শেষ") {
		t.Fatalf("unexpected bounded tail %q (%d bytes)", got, len(got))
	}
}
