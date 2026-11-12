package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// OutputFormat reads the root's --output flag: "table" (the default) or "json".
func OutputFormat(command *cobra.Command) (string, error) {
	format, err := command.Flags().GetString("output")
	if err != nil || format == "" {
		return "table", nil
	}
	if format != "table" && format != "json" {
		return "", fmt.Errorf("--output must be table or json, not %q", format)
	}
	return format, nil
}
