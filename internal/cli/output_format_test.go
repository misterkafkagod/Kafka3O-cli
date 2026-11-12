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
