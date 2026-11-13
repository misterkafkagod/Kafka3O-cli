package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/connect"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

type connectorList struct {
	Clusters []connect.ClusterConnectors `json:"clusters"`
	Failures []clusters.ClusterFailure   `json:"failures"`
}

// NewConnectorsCommand lists Kafka Connect connectors and their tasks across every accessible cluster (or one with
// --cluster). Everything Connect reports is terminal-escaped.
func NewConnectorsCommand(deps SessionDependencies, list connect.ListConnectors) *cobra.Command {
	var cluster string
	command := &cobra.Command{
		Use:   "connectors",
		Short: "List Kafka Connect connectors and their tasks across every cluster you may access",
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
			found, failures, err := list.Execute(command.Context(), session, cluster)
			if err != nil {
				return err
			}
			result := error(nil)
			if len(failures) > 0 {
				result = fmt.Errorf("%d cluster(s) could not list connectors: %w", len(failures), ErrClusterFailed)
			}
			if format == "json" {
				if err := output.WriteJSON(command.OutOrStdout(), connectorList{Clusters: found, Failures: failures}); err != nil {
					return err
				}
				return result
			}
			for _, failure := range failures {
				fmt.Fprintf(command.ErrOrStderr(), "%s: %s\n", failure.Cluster, describeClusterStatus(clustersHealthOf(failure)))
			}
			rows, problems, notConfigured := [][]string{}, []string{}, []string{}
			for _, answered := range found {
				if answered.NotConfigured {
					notConfigured = append(notConfigured, answered.Cluster+": Kafka Connect is not configured for this cluster.")
				}
				for _, connector := range answered.Connectors {
					name := messages.EscapeTerminalText(connector.Name)
					tasks := []string{}
					if connector.Error != "" {
						problems = append(problems, fmt.Sprintf("%s %s: %s", answered.Cluster, name, messages.EscapeTerminalText(connector.Error)))
					}
					for _, task := range connector.Tasks {
						tasks = append(tasks, fmt.Sprintf("%d:%s", task.ID, messages.EscapeTerminalText(task.State)))
						if task.Error != "" {
							problems = append(problems, fmt.Sprintf("%s %s task %d: %s", answered.Cluster, name, task.ID, messages.EscapeTerminalText(task.Error)))
						}
					}
					if len(tasks) == 0 {
						tasks = append(tasks, "none")
					}
					rows = append(rows, []string{answered.Cluster, name, messages.EscapeTerminalText(connector.Type), messages.EscapeTerminalText(connector.State),
						messages.EscapeTerminalText(connector.WorkerID), strings.Join(tasks, " ")})
				}
			}
			if len(rows) == 0 {
				fmt.Fprintln(command.OutOrStdout(), "No connectors.")
			} else if err := output.WriteTable(command.OutOrStdout(), []string{"CLUSTER", "CONNECTOR", "TYPE", "STATE", "WORKER", "TASKS"}, rows); err != nil {
				return err
			}
			if len(problems) > 0 {
				fmt.Fprintf(command.OutOrStdout(), "\nErrors\n%s\n", strings.Join(problems, "\n"))
			}
			if len(notConfigured) > 0 {
				fmt.Fprintf(command.OutOrStdout(), "\n%s\n", strings.Join(notConfigured, "\n"))
			}
			return result
		},
	}
	command.Flags().StringVar(&cluster, "cluster", "", "only this cluster (default: every cluster you may access)")
	return command
}
