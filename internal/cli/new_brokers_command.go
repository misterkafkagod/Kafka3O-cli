package cli

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// brokerList is the JSON shape of `kafka3o brokers --output json`.
type brokerList struct {
	Brokers  []clusters.Broker         `json:"brokers"`
	Failures []clusters.ClusterFailure `json:"failures"`
}

// NewBrokersCommand lists the brokers of every accessible cluster (or one with --cluster).
func NewBrokersCommand(deps SessionDependencies, list clusters.ListBrokers) *cobra.Command {
	var cluster string
	command := &cobra.Command{
		Use:   "brokers",
		Short: "List the brokers of every cluster you may access",
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
			brokers, failures, err := list.Execute(command.Context(), session, cluster)
			if err != nil {
				return err
			}
			result := error(nil)
			if len(failures) > 0 {
				result = fmt.Errorf("%d cluster(s) could not list brokers: %w", len(failures), ErrClusterFailed)
			}
			if format == "json" {
				if err := output.WriteJSON(command.OutOrStdout(), brokerList{Brokers: brokers, Failures: failures}); err != nil {
					return err
				}
				return result
			}
			for _, failure := range failures {
				fmt.Fprintf(command.ErrOrStderr(), "%s: %s\n", failure.Cluster, describeClusterStatus(clustersHealthOf(failure)))
			}
			rows := make([][]string, 0, len(brokers))
			for _, broker := range brokers {
				role := ""
				if broker.Controller {
					role = "controller"
				}
				rows = append(rows, []string{broker.Cluster, strconv.Itoa(int(broker.ID)), broker.Host, strconv.Itoa(int(broker.Port)), role})
			}
			if err := output.WriteTable(command.OutOrStdout(), []string{"CLUSTER", "BROKER", "HOST", "PORT", "ROLE"}, rows); err != nil {
				return err
			}
			return result
		},
	}
	command.Flags().StringVar(&cluster, "cluster", "", "only this cluster (default: every cluster you may access)")
	return command
}
