package output

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteNewFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.csv")
	if err := WriteNewFile(path, []byte("a,b\n")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("%v %v", info, err)
	}
	if err := WriteNewFile(path, []byte("other")); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("overwrite: %v", err)
	}
	if data, _ := os.ReadFile(path); string(data) != "a,b\n" {
		t.Fatalf("replaced: %q", data)
	}
}
