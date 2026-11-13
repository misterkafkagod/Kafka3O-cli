package cli

import (
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/cobra"
)

// OutputFormat reads the root's --output flag: "table" (the default), "json", or one of the extra formats a
// command also accepts (for example "csv" for metrics).
func OutputFormat(command *cobra.Command, extra ...string) (string, error) {
	format, err := command.Flags().GetString("output")
	if err != nil || format == "" {
		return "table", nil
	}
	allowed := append([]string{"table", "json"}, extra...)
	if !slices.Contains(allowed, format) {
		return "", fmt.Errorf("--output must be %s, not %q", strings.Join(allowed, " or "), format)
	}
	return format, nil
}
