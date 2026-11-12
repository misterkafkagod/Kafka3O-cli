package cli

import (
	"strconv"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// NewClustersCommand lists every cluster the signed-in user may access with its health, read in parallel.
func NewClustersCommand(deps SessionDependencies, list clusters.ListClusterHealth) *cobra.Command {
	return &cobra.Command{
		Use:   "clusters",
		Short: "List every cluster you may access with its health",
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
			health, err := list.Execute(command.Context(), session)
			if err != nil {
				return err
			}
			if format == "json" {
				return output.WriteJSON(command.OutOrStdout(), health)
			}
			rows := make([][]string, 0, len(health))
			for _, cluster := range health {
				brokers, controller := "", ""
				if cluster.Brokers != nil {
					brokers = strconv.Itoa(*cluster.Brokers)
				}
				if cluster.ControllerID != nil {
					controller = strconv.Itoa(int(*cluster.ControllerID))
				}
				rows = append(rows, []string{cluster.ID, cluster.Name, describeEnvironment(cluster.Environment), cluster.EffectiveRole,
					describeClusterStatus(cluster), brokers, controller})
			}
			return output.WriteTable(command.OutOrStdout(), []string{"CLUSTER", "NAME", "ENVIRONMENT", "ROLE", "STATUS", "BROKERS", "CONTROLLER"}, rows)
		},
	}
}
