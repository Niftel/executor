package core

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidatePackTarballRejectsEntryOutsidePack(t *testing.T) {
	archive := writePackArchive(t, "ansible-runtime", map[string]string{
		"bin/ansible-playbook":       "ansible",
		"bin/praetor-host-runner":    "runner",
		"../../../../etc/unexpected": "escape",
	})
	if _, err := validatePackTarball(archive, "ansible-runtime"); err == nil {
		t.Fatal("expected an out-of-pack archive entry to be rejected")
	}
}

func TestPackInstallReplacesDigestWithoutRemoteArchive(t *testing.T) {
	root := t.TempDir()
	first := writePackArchive(t, "ansible-runtime", map[string]string{
		"bin/ansible-playbook":    "ansible-v1",
		"bin/praetor-host-runner": "runner-v1",
	})
	firstDigest, err := validatePackTarball(first, "ansible-runtime")
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	wantFirstDigest := fmt.Sprintf("%x", sha256.Sum256(firstBytes))
	if firstDigest != wantFirstDigest {
		t.Fatalf("pack digest = %s, want %s", firstDigest, wantFirstDigest)
	}
	if command := packInstallCommand("ansible-runtime", firstDigest, ""); strings.Contains(command, "/tmp") {
		t.Fatalf("target install command relies on /tmp: %s", command)
	}
	runPackInstall(t, root, first, firstDigest, true)

	second := writePackArchive(t, "ansible-runtime", map[string]string{
		"bin/ansible-playbook":    "ansible-v2",
		"bin/praetor-host-runner": "runner-v2",
	})
	secondDigest, err := validatePackTarball(second, "ansible-runtime")
	if err != nil {
		t.Fatal(err)
	}
	runPackInstall(t, root, second, secondDigest, true)

	active := filepath.Join(root, "ansible-runtime")
	if info, err := os.Lstat(active); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("active pack is not a symlink: info=%v err=%v", info, err)
	}
	content, err := os.ReadFile(filepath.Join(active, "bin", "praetor-host-runner"))
	if err != nil || string(content) != "runner-v2" {
		t.Fatalf("active pack was not replaced: content=%q err=%v", content, err)
	}
	marker, err := os.ReadFile(filepath.Join(active, ".praetor-pack-digest"))
	if err != nil || strings.TrimSpace(string(marker)) != secondDigest {
		t.Fatalf("digest marker mismatch: marker=%q err=%v", marker, err)
	}
	if matches, _ := filepath.Glob(filepath.Join(root, "*.tar.gz")); len(matches) != 0 {
		t.Fatalf("compressed archive was written to target: %v", matches)
	}
}

func TestInterruptedPackInstallKeepsActivePackAndCleansPartial(t *testing.T) {
	root := t.TempDir()
	good := writePackArchive(t, "ansible-runtime", map[string]string{
		"bin/ansible-playbook":    "ansible-good",
		"bin/praetor-host-runner": "runner-good",
	})
	goodDigest, err := validatePackTarball(good, "ansible-runtime")
	if err != nil {
		t.Fatal(err)
	}
	runPackInstall(t, root, good, goodDigest, true)

	badBytes, err := os.ReadFile(good)
	if err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(t.TempDir(), "truncated.tar.gz")
	if err := os.WriteFile(bad, badBytes[:len(badBytes)/2], 0o600); err != nil {
		t.Fatal(err)
	}
	runPackInstall(t, root, bad, strings.Repeat("f", 64), false)

	content, err := os.ReadFile(filepath.Join(root, "ansible-runtime", "bin", "praetor-host-runner"))
	if err != nil || string(content) != "runner-good" {
		t.Fatalf("failed install changed the active pack: content=%q err=%v", content, err)
	}
	partials, _ := filepath.Glob(filepath.Join(root, ".*.partial.*"))
	if len(partials) != 0 {
		t.Fatalf("failed install left partial trees: %v", partials)
	}
}

func runPackInstall(t *testing.T, root, archive, digest string, wantSuccess bool) {
	t.Helper()
	f, err := os.Open(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	cmd := exec.Command("sh", "-c", packInstallCommandAt("ansible-runtime", digest, "", root))
	cmd.Stdin = f
	out, err := cmd.CombinedOutput()
	if wantSuccess && err != nil {
		t.Fatalf("pack install failed: %v: %s", err, out)
	}
	if !wantSuccess && err == nil {
		t.Fatalf("pack install unexpectedly succeeded: %s", out)
	}
}

func writePackArchive(t *testing.T, pack string, files map[string]string) string {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, content := range files {
		fullName := "opt/praetor/packs/" + pack + "/" + name
		hdr := &tar.Header{Name: fullName, Mode: 0o755, Size: int64(len(content)), Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(buf.Bytes())
	filename := filepath.Join(t.TempDir(), fmt.Sprintf("pack-%x.tar.gz", sum[:4]))
	if err := os.WriteFile(filename, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return filename
}
