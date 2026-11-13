package cli

import (
	"bufio"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/accounts"
)

// AccountDependencies are what `kafka3o account …` needs.
type AccountDependencies struct {
	Session      SessionDependencies
	ReadPassword ReadPassword
	Find         accounts.FindAccount
	Create       accounts.CreateAccount
	Reset        accounts.ResetPassword
	SetEnabled   accounts.SetAccountEnabled
}

// NewAccountCommand creates local accounts, resets their password, and enables or disables them. Temporary passwords
// are read hidden (twice) or from standard input, and are never printed.
func NewAccountCommand(deps AccountDependencies) *cobra.Command {
	var yes, passwordStdin bool
	account := &cobra.Command{Use: "account", Short: "Create, reset, enable or disable a local account (local global ADMIN only)"}
	create := &cobra.Command{
		Use:   "create <username>",
		Short: "Create a local account with a temporary password it must change at first sign-in",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if err := accounts.ValidateUsername(args[0]); err != nil {
				return err
			}
			session, err := deps.Session.LoadSession()
			if err != nil {
				return err
			}
			input := bufio.NewReader(command.InOrStdin())
			password, err := readNewPassword(input, passwordStdin, deps.ReadPassword)
			if err != nil {
				return err
			}
			if err := ConfirmChange(input, command.ErrOrStderr(), []string{fmt.Sprintf("Create local account \"%s\" with the temporary password you entered (it must be changed at first sign-in)", args[0])}, yes); err != nil {
				return err
			}
			created, err := deps.Create.Execute(command.Context(), session, args[0], password)
			if err != nil {
				return adminError(err)
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "Created local account %s; it must change its password at first sign-in.\n", created.Username)
			return err
		},
	}
	reset := &cobra.Command{
		Use:   "reset-password <username>",
		Short: "Give a local account a new temporary password; its sessions end",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			session, err := deps.Session.LoadSession()
			if err != nil {
				return err
			}
			target, err := deps.Find.Execute(command.Context(), session, args[0])
			if err != nil {
				return adminError(err)
			}
			input := bufio.NewReader(command.InOrStdin())
			password, err := readNewPassword(input, passwordStdin, deps.ReadPassword)
			if err != nil {
				return err
			}
			if err := ConfirmChange(input, command.ErrOrStderr(), []string{fmt.Sprintf("Give local account \"%s\" the temporary password you entered (it must be changed at next sign-in; its sessions end)", target.Username)}, yes); err != nil {
				return err
			}
			if _, err := deps.Reset.Execute(command.Context(), session, target, password); err != nil {
				return adminError(err)
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "Reset the password of %s; it must change it at next sign-in.\n", target.Username)
			return err
		},
	}
	create.Flags().BoolVar(&passwordStdin, "password-stdin", false, "read the temporary password from the first line of standard input")
	reset.Flags().BoolVar(&passwordStdin, "password-stdin", false, "read the temporary password from the first line of standard input")
	account.AddCommand(create, reset)
	for _, state := range []struct {
		name, short, summary, done string
		enabled                    bool
	}{
		{"enable", "Enable a local account", "Enable local account \"%s\"", "Enabled %s.", true},
		{"disable", "Disable a local account; its sessions end", "Disable local account \"%s\" (it can no longer sign in; its sessions end)", "Disabled %s; its sessions ended.", false},
	} {
		action := &cobra.Command{
			Use:   state.name + " <username>",
			Short: state.short,
			Args:  cobra.ExactArgs(1),
			RunE: func(command *cobra.Command, args []string) error {
				session, err := deps.Session.LoadSession()
				if err != nil {
					return err
				}
				target, err := deps.Find.Execute(command.Context(), session, args[0])
				if err != nil {
					return adminError(err)
				}
				if err := ConfirmChange(bufio.NewReader(command.InOrStdin()), command.ErrOrStderr(), []string{fmt.Sprintf(state.summary, target.Username)}, yes); err != nil {
					return err
				}
				if _, err := deps.SetEnabled.Execute(command.Context(), session, target, state.enabled); err != nil {
					return adminError(err)
				}
				_, err = fmt.Fprintf(command.OutOrStdout(), state.done+"\n", target.Username)
				return err
			},
		}
		account.AddCommand(action)
	}
	for _, sub := range account.Commands() {
		sub.Flags().BoolVarP(&yes, "yes", "y", false, "do not ask for confirmation")
	}
	return account
}
