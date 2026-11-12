package terminal

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestReadHiddenPasswordRefusesANonTerminal(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	var prompt bytes.Buffer
	if _, err := NewReadHiddenPassword(reader, &prompt).Execute("Password: "); !errors.Is(err, ErrNoTerminal) || prompt.Len() != 0 {
		t.Fatalf("err %v, prompt %q", err, prompt.String())
	}
}
