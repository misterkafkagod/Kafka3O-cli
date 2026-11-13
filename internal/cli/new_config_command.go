package cli

import (
	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
)

// NewConfigCommand groups `config set` and `config reset` for topic and broker settings.
func NewConfigCommand(deps ConfigurationDependencies) *cobra.Command {
	var yes bool
	var reason string
	set := &cobra.Command{
		Use:   "set <cluster> topic|broker <name|id> <setting> <value>",
		Short: "Change a topic or broker setting (put -- before a negative value)",
		Args:  cobra.ExactArgs(5),
		RunE: func(command *cobra.Command, args []string) error {
			value := args[4]
			return runConfigurationChange(command, deps, args[0], args[1], args[2], clusters.ConfigurationChange{Name: args[3], Operation: "set", Value: &value}, yes, reason)
		},
	}
	reset := &cobra.Command{
		Use:   "reset <cluster> topic|broker <name|id> <setting>",
		Short: "Reset a topic or broker setting to its default",
		Args:  cobra.ExactArgs(4),
		RunE: func(command *cobra.Command, args []string) error {
			return runConfigurationChange(command, deps, args[0], args[1], args[2], clusters.ConfigurationChange{Name: args[3], Operation: "reset"}, yes, reason)
		},
	}
	addChangeFlags(set, &yes, &reason)
	addChangeFlags(reset, &yes, &reason)
	config := &cobra.Command{Use: "config", Short: "Change topic and broker configuration"}
	config.AddCommand(set, reset)
	return config
}
