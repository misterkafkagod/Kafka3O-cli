package cli

import (
	"bufio"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// NewCreateTopicCommand creates a topic after confirmation.
func NewCreateTopicCommand(deps SessionDependencies, accessible clusters.ListAccessibleClusters, create topics.CreateTopic) *cobra.Command {
	var partitions, replication int
	var yes bool
	var reason string
	command := &cobra.Command{
		Use:   "create-topic <cluster> <topic>",
		Short: "Create a topic",
		Args:  cobra.ExactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			topic := topics.NewTopic{Name: args[1], Partitions: partitions, ReplicationFactor: replication}
			if err := topics.ValidateNewTopic(topic); err != nil {
				return err
			}
			session, err := deps.LoadSession()
			if err != nil {
				return err
			}
			sentReason, err := PrepareChange(command.Context(), session, accessible, bufio.NewReader(command.InOrStdin()), command.ErrOrStderr(), ChangeRequest{
				Cluster: args[0], Yes: yes, Reason: reason,
				Summary: []string{fmt.Sprintf("Create topic %s with %d partition(s) and replication factor %d", topic.Name, topic.Partitions, topic.ReplicationFactor)},
			})
			if err != nil {
				return err
			}
			if err := create.Execute(command.Context(), session, args[0], topic, sentReason); err != nil {
				return writeError(args[0], err)
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "Created topic %s on %s.\n", topic.Name, args[0])
			return err
		},
	}
	command.Flags().IntVar(&partitions, "partitions", 1, "number of partitions")
	command.Flags().IntVar(&replication, "replication-factor", 1, "replicas per partition")
	addChangeFlags(command, &yes, &reason)
	return command
}
