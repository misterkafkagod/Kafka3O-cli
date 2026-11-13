package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/accounts"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// NewAccountsCommand lists the Console's local accounts (local global ADMIN only).
func NewAccountsCommand(deps SessionDependencies, list accounts.ListAccounts) *cobra.Command {
	return &cobra.Command{
		Use:   "accounts",
		Short: "List the Console's local accounts (local global ADMIN only)",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			format, err := OutputFormat(command)
			if err != nil {
				return err
			}
			session, err := deps.LoadSession()
			if err != nil {
				return err
			}
			all, err := list.Execute(command.Context(), session)
			if err != nil {
				return adminError(err)
			}
			if format == "json" {
				return output.WriteJSON(command.OutOrStdout(), all)
			}
			if len(all) == 0 {
				_, err := fmt.Fprintln(command.OutOrStdout(), "No local accounts.")
				return err
			}
			rows := [][]string{}
			for _, account := range all {
				enabled, password := "yes", "set"
				if !account.Enabled {
					enabled = "no"
				}
				if account.MustChangePassword {
					password = "must change"
				}
				rows = append(rows, []string{messages.EscapeTerminalText(account.Username), enabled, password, messages.EscapeTerminalText(account.ID)})
			}
			return output.WriteTable(command.OutOrStdout(), []string{"USERNAME", "ENABLED", "PASSWORD", "ID"}, rows)
		},
	}
}
