package cli

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// NewLogoutCommand ends the Console session (so no copy of the session file works any more) and deletes the file.
// Not being signed in is not an error.
func NewLogoutCommand(loadSession func() (console.Session, error), signOut console.SignOut, deleteSession func() error) *cobra.Command {
	return &cobra.Command{
		Use:   "logout",
		Short: "Sign out of the Kafka3O Console",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			session, err := loadSession()
			if errors.Is(err, console.ErrSignedOut) {
				fmt.Fprintln(command.OutOrStdout(), "Not signed in.")
				return deleteSession()
			}
			if err != nil {
				return err
			}
			if err := signOut.Execute(command.Context(), session); err != nil {
				return fmt.Errorf("sign-out failed, the session file was kept: %w", err)
			}
			if err := deleteSession(); err != nil {
				return err
			}
			fmt.Fprintln(command.OutOrStdout(), "Signed out.")
			return nil
		},
	}
}
