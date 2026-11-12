package cli

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// NewTopicCommand shows one topic: partitions, configuration, the consumer groups reading it and its backup.
func NewTopicCommand(deps SessionDependencies, describe topics.DescribeTopic) *cobra.Command {
	return &cobra.Command{
		Use:   "topic <cluster> <topic>",
		Short: "Show one topic's partitions, configuration, consumers and backup",
		Args:  cobra.ExactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			format, err := OutputFormat(command)
			if err != nil {
				return err
			}
			session, err := deps.LoadSession()
			if err != nil {
				return err
			}
			view, err := describe.Execute(command.Context(), session, args[0], args[1])
			var clusterRead topics.ErrClusterRead
			if errors.As(err, &clusterRead) {
				return fmt.Errorf("%s: %s: %w", clusterRead.Cluster, describeClusterStatus(clustersHealthOf(topics.ClusterFailure{
					Cluster: clusterRead.Cluster, Status: clusterRead.Status, Error: clusterRead.Reason})), ErrClusterFailed)
			}
			if err != nil {
				return err
			}
			if format == "json" {
				return output.WriteJSON(command.OutOrStdout(), view)
			}
			return WriteTopicView(command.OutOrStdout(), view)
		},
	}
}
