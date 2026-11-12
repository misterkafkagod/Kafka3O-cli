package cli

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// topicList is the JSON shape of `kafka3o topics --output json`.
type topicList struct {
	Topics   []topics.Topic          `json:"topics"`
	Failures []topics.ClusterFailure `json:"failures"`
}

// NewTopicsCommand lists topics across every accessible cluster (or one with --cluster), filtered by name.
func NewTopicsCommand(deps SessionDependencies, list topics.ListTopicsAcrossClusters) *cobra.Command {
	var cluster, filter string
	var internal bool
	command := &cobra.Command{
		Use:   "topics",
		Short: "List topics across every cluster you may access",
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
			shown := topics.FilterTopics(found, filter, internal)
			result := error(nil)
			if len(failures) > 0 {
				result = fmt.Errorf("%d cluster(s) could not list topics: %w", len(failures), ErrClusterFailed)
			}
			if format == "json" {
				if err := output.WriteJSON(command.OutOrStdout(), topicList{Topics: shown, Failures: failures}); err != nil {
					return err
				}
				return result
			}
			for _, failure := range failures {
				fmt.Fprintf(command.ErrOrStderr(), "%s: %s\n", failure.Cluster, describeClusterStatus(clustersHealthOf(failure)))
			}
			if len(shown) == 0 {
				fmt.Fprintln(command.OutOrStdout(), "No topics.")
				return result
			}
			rows := make([][]string, 0, len(shown))
			for _, topic := range shown {
				rows = append(rows, []string{topic.Cluster, topic.Name, strconv.Itoa(topic.Partitions), strconv.Itoa(topic.ReplicationFactor),
					strconv.FormatInt(topic.ApproxMessages, 10)})
			}
			if err := output.WriteTable(command.OutOrStdout(), []string{"CLUSTER", "TOPIC", "PARTITIONS", "REPLICATION", "MESSAGES"}, rows); err != nil {
				return err
			}
			return result
		},
	}
	command.Flags().StringVar(&cluster, "cluster", "", "only this cluster (default: every cluster you may access)")
	command.Flags().StringVar(&filter, "filter", "", "only topics whose name contains this text (case-insensitive)")
	command.Flags().BoolVar(&internal, "internal", false, "include internal topics such as __consumer_offsets")
	return command
}
