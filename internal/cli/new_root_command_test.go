package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestNewRootCommandListsItsSubcommands(t *testing.T) {
	var out bytes.Buffer
	root := NewRootCommand(&cobra.Command{Use: "example", Short: "An example", RunE: func(*cobra.Command, []string) error { return nil }})
	root.SetOut(&out)
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "kafka3o") || !strings.Contains(out.String(), "example") {
		t.Fatalf("help = %s", out.String())
	}
}

func TestNewRootCommandRefusesUnknownCommands(t *testing.T) {
	root := NewRootCommand(&cobra.Command{Use: "example", RunE: func(*cobra.Command, []string) error { return nil }})
	root.SetOut(&bytes.Buffer{})
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"nope"})
	if err := root.Execute(); err == nil || ExitCode(err) != ExitUsage {
		t.Fatalf("err = %v", err)
	}
}
