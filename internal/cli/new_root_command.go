package cli

import "github.com/spf13/cobra"

// NewRootCommand is `kafka3o`; subcommands are added by the caller so each one gets its own dependencies.
func NewRootCommand(subcommands ...*cobra.Command) *cobra.Command {
	root := &cobra.Command{
		Use:           "kafka3o",
		Short:         "Kafka3O from the terminal: every cluster you may access, through the Kafka3O Console",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(subcommands...)
	return root
}
