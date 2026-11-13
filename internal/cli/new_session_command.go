package cli

import (
	"bufio"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/sessions"
)

// SessionAdminDependencies are what `kafka3o session revoke` needs.
type SessionAdminDependencies struct {
	Session SessionDependencies
	List    sessions.ListSessions
	Revoke  sessions.RevokeSession
}

// NewSessionCommand revokes another Console session; the current one is ended with `kafka3o logout` instead.
func NewSessionCommand(deps SessionAdminDependencies) *cobra.Command {
	var yes bool
	session := &cobra.Command{Use: "session", Short: "Revoke a Console session (local global ADMIN only)"}
	revoke := &cobra.Command{
		Use:   "revoke <id>",
		Short: "Revoke another session; it must sign in again",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			signedIn, err := deps.Session.LoadSession()
			if err != nil {
				return err
			}
			all, err := deps.List.Execute(command.Context(), signedIn)
			if err != nil {
				return adminError(err)
			}
			var target *sessions.Session
			for i := range all {
				if all[i].ID == args[0] {
					target = &all[i]
				}
			}
			switch {
			case target == nil:
				return fmt.Errorf("no active session has the id %s", args[0])
			case target.IsCurrent:
				return errors.New("this is your current session; use kafka3o logout")
			}
			created := target.CreatedAt
			if at, err := time.Parse(time.RFC3339Nano, created); err == nil {
				created = at.In(time.Local).Format("2006-01-02 15:04")
			}
			summary := []string{fmt.Sprintf("Revoke the %s session of %s created %s (it must sign in again)",
				messages.EscapeTerminalText(target.AuthSource), messages.EscapeTerminalText(target.Username), messages.EscapeTerminalText(created))}
			if err := ConfirmChange(bufio.NewReader(command.InOrStdin()), command.ErrOrStderr(), summary, yes); err != nil {
				return err
			}
			if err := deps.Revoke.Execute(command.Context(), signedIn, target.ID); err != nil {
				return adminError(err)
			}
			_, err = fmt.Fprintln(command.OutOrStdout(), "Revoked; that session must sign in again.")
			return err
		},
	}
	revoke.Flags().BoolVarP(&yes, "yes", "y", false, "do not ask for confirmation")
	session.AddCommand(revoke)
	return session
}
