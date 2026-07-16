package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestExecutorCollectionsAreVersionLocked(t *testing.T) {
	const requirementsPath = "deploy/collections-requirements.yml"

	requirements, err := os.ReadFile(requirementsPath)
	if err != nil {
		t.Fatalf("read collection lock file: %v", err)
	}

	names := regexp.MustCompile(`(?m)^\s+- name:\s+(\S+)\s*$`).FindAllSubmatch(requirements, -1)
	versions := regexp.MustCompile(`(?m)^\s+version:\s+([0-9]+\.[0-9]+\.[0-9]+)\s*$`).FindAllSubmatch(requirements, -1)
	if len(names) == 0 || len(names) != len(versions) {
		t.Fatalf("every Ansible collection must have an exact semantic version: names=%d versions=%d", len(names), len(versions))
	}

	dockerfile, err := os.ReadFile("Dockerfile")
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	text := string(dockerfile)
	if !strings.Contains(text, "COPY "+requirementsPath+" /tmp/build/collections-requirements.yml") {
		t.Fatalf("Dockerfile does not copy the locked collection requirements")
	}
	if !strings.Contains(text, "ansible-galaxy collection install --no-deps -r /tmp/build/collections-requirements.yml") {
		t.Fatalf("Dockerfile does not install only the explicitly locked collections")
	}
	lockIndex := strings.Index(text, "ansible-galaxy collection install --no-deps")
	binaryIndex := strings.Index(text, "COPY --from=builder /praetor-executor")
	if lockIndex < 0 || binaryIndex < 0 || lockIndex > binaryIndex {
		t.Fatalf("locked runtime dependencies must be installed before the changing executor binary is copied")
	}
}
