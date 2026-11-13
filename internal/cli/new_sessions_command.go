package cli

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
	"github.com/misterkafkagod/Kafka3O-cli/internal/sessions"
)

// NewSessionsCommand lists every active Console session (local global ADMIN only); the current one is marked.
func NewSessionsCommand(deps SessionDependencies, list sessions.ListSessions) *cobra.Command {
	return &cobra.Command{
		Use:   "sessions",
		Short: "List active Console sessions (local global ADMIN only)",
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
				_, err := fmt.Fprintln(command.OutOrStdout(), "No active sessions.")
				return err
			}
			rows := [][]string{}
			for _, active := range all {
				times := []string{active.CreatedAt, active.IdleExpiresAt, active.AbsoluteExpiresAt}
				for i, text := range times {
					if at, err := time.Parse(time.RFC3339Nano, text); err == nil {
						times[i] = at.In(time.Local).Format("2006-01-02 15:04")
					}
				}
				current := ""
				if active.IsCurrent {
					current = "yes"
				}
				row := []string{active.Username, active.AuthSource, times[0], times[1], times[2], current, active.ID}
				for i := range row {
					row[i] = messages.EscapeTerminalText(row[i])
				}
				rows = append(rows, row)
			}
			return output.WriteTable(command.OutOrStdout(), []string{"USER", "SOURCE", "CREATED", "IDLE EXPIRES", "EXPIRES", "CURRENT", "ID"}, rows)
		},
	}
}
