package cli

import (
	"bytes"
	"testing"

	"github.com/spf13/cobra"
)

func TestOutputFormat(t *testing.T) {
	for args, want := range map[string]string{"": "table", "--output=json": "json", "-o=table": "table", "--output=yaml": "error"} {
		var got string
		child := &cobra.Command{Use: "child", RunE: func(command *cobra.Command, _ []string) error {
			format, err := OutputFormat(command)
			got = format
			if err != nil {
				got = "error"
			}
			return nil
		}}
		root := NewRootCommand(child)
		root.SetOut(&bytes.Buffer{})
		arguments := []string{"child"}
		if args != "" {
			arguments = append(arguments, args)
		}
		root.SetArgs(arguments)
		_ = root.Execute()
		if got != want {
			t.Errorf("%q: %q, want %q", args, got, want)
		}
	}
}

func TestOutputFormatExtraFormats(t *testing.T) {
	var formats []string
	var failures []error
	child := &cobra.Command{Use: "child", RunE: func(command *cobra.Command, _ []string) error {
		format, err := OutputFormat(command, "csv")
		formats, failures = append(formats, format), append(failures, err)
		return nil
	}}
	plain := &cobra.Command{Use: "plain", RunE: func(command *cobra.Command, _ []string) error {
		_, err := OutputFormat(command)
		failures = append(failures, err)
		return nil
	}}
	for _, args := range [][]string{{"child", "-o", "csv"}, {"plain", "-o", "csv"}} {
		root := NewRootCommand(child, plain)
		root.SetOut(&bytes.Buffer{})
		root.SetArgs(args)
		_ = root.Execute()
	}
	if formats[0] != "csv" || failures[0] != nil || failures[1] == nil || failures[1].Error() != `--output must be table or json, not "csv"` {
		t.Fatalf("%v %v", formats, failures)
	}
}
