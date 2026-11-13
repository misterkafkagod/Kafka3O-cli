package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/environments"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// NewEnvironmentsCommand lists the registered clusters with their environment (local global ADMIN only).
func NewEnvironmentsCommand(deps SessionDependencies, list environments.ListRegisteredClusters) *cobra.Command {
	return &cobra.Command{
		Use:   "environments",
		Short: "List registered clusters with their environment (local global ADMIN only)",
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
			listing, err := list.Execute(command.Context(), session)
			if err != nil {
				return adminError(err)
			}
			if format == "json" {
				return output.WriteJSON(command.OutOrStdout(), listing)
			}
			if len(listing.Clusters) == 0 {
				_, err := fmt.Fprintln(command.OutOrStdout(), "No registered clusters.")
				return err
			}
			rows := [][]string{}
			for _, cluster := range listing.Clusters {
				active := "yes"
				if !cluster.Active {
					active = "no"
				}
				environment := describeEnvironment(cluster.Environment)
				if environment == "" {
					environment = "Unclassified"
				}
				rows = append(rows, []string{messages.EscapeTerminalText(cluster.ID), messages.EscapeTerminalText(cluster.Name), environment,
					active, messages.EscapeTerminalText(cluster.GatewayURL)})
			}
			return output.WriteTable(command.OutOrStdout(), []string{"CLUSTER", "NAME", "ENVIRONMENT", "ACTIVE", "GATEWAY"}, rows)
		},
	}
}
