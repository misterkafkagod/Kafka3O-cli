package cli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// LoginDependencies are what `kafka3o login` needs; production wiring is in main.
type LoginDependencies struct {
	SignIn       console.SignIn
	SaveSession  func(path string, session console.Session) error
	SessionPath  string
	ReadPassword ReadPassword
}

// NewLoginCommand signs in to the Console with a local account and keeps the session for the next commands.
// The password is asked for hidden, or read from standard input with --password-stdin; it is never printed.
func NewLoginCommand(deps LoginDependencies) *cobra.Command {
	var consoleURL, username string
	var passwordStdin bool
	command := &cobra.Command{
		Use:   "login",
		Short: "Sign in to the Kafka3O Console with a local account",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if consoleURL == "" {
				return errors.New("--console is required, for example --console http://localhost:5173")
			}
			input := bufio.NewReader(command.InOrStdin())
			if username == "" {
				if passwordStdin {
					return errors.New("--username is required with --password-stdin")
				}
				fmt.Fprint(command.ErrOrStderr(), "Username: ")
				line, err := input.ReadString('\n')
				if err != nil && (err != io.EOF || line == "") {
					return fmt.Errorf("read the username: %w", err)
				}
				username = strings.TrimSpace(line)
			}
			var password string
			if passwordStdin {
				line, err := input.ReadString('\n')
				if err != nil && (err != io.EOF || line == "") {
					return errors.New("--password-stdin: no password on standard input")
				}
				password = strings.TrimRight(line, "\r\n")
			} else {
				read, err := deps.ReadPassword.Execute("Password: ")
				if err != nil {
					return err
				}
				password = read
			}
			if username == "" || password == "" {
				return errors.New("a username and a password are required")
			}
			session, err := deps.SignIn.Execute(command.Context(), consoleURL, username, password)
			if err != nil {
				return fmt.Errorf("sign-in failed: %w", err)
			}
			if err := deps.SaveSession(deps.SessionPath, session); err != nil {
				return err
			}
			fmt.Fprintf(command.OutOrStdout(), "Signed in to %s as %s until %s\n", session.ConsoleURL, session.Username,
				session.ExpiresAt.In(time.Local).Format("2006-01-02 15:04 MST"))
			return nil
		},
	}
	command.Flags().StringVar(&consoleURL, "console", "", "Console URL, as opened in the browser (for example http://localhost:5173)")
	command.Flags().StringVar(&username, "username", "", "local account name (asked for when missing)")
	command.Flags().BoolVar(&passwordStdin, "password-stdin", false, "read the password from standard input")
	return command
}
