package cli

import (
	"bufio"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
)

// confirmAccessChange shows the subject, scope and old → new role and asks; it reports false (nothing to send)
// when the role already matches.
func confirmAccessChange(command *cobra.Command, subject, scope, expected, role string, yes bool) (bool, error) {
	old, next := expected, role
	if old == "" {
		old = "none"
	}
	if next == "" {
		next = "none"
	}
	where := "all clusters"
	if scope != "" {
		where = "cluster " + scope
	}
	if expected == role {
		_, err := fmt.Fprintf(command.OutOrStdout(), "Nothing to change: %s already has %s on %s.\n", messages.EscapeTerminalText(subject), old, where)
		return false, err
	}
	summary := []string{
		fmt.Sprintf("%s on %s: %s → %s", messages.EscapeTerminalText(subject), where, old, next),
		"roles add up: the highest of the global grant and the cluster grant applies",
	}
	return true, ConfirmChange(bufio.NewReader(command.InOrStdin()), command.ErrOrStderr(), summary, yes)
}
