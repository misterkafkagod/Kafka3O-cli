package cli

import (
	"bufio"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// PartitionDependencies are what `kafka3o partitions add|reassign` need.
type PartitionDependencies struct {
	Session    SessionDependencies
	Accessible clusters.ListAccessibleClusters
	Describe   topics.DescribeTopic
	Increase   topics.IncreasePartitions
	Reassign   topics.ReassignPartition
}

// NewPartitionsCommand groups adding partitions and moving a partition's replicas.
func NewPartitionsCommand(deps PartitionDependencies) *cobra.Command {
	var yes bool
	var reason string
	add := &cobra.Command{
		Use:   "add <cluster> <topic> <total>",
		Short: "Raise a topic's partition count to a new total (partitions cannot be removed)",
		Args:  cobra.ExactArgs(3),
		RunE: func(command *cobra.Command, args []string) error {
			total, err := strconv.Atoi(args[2])
			if err != nil || total < 1 {
				return fmt.Errorf("the new total must be a number of partitions, not %q", args[2])
			}
			session, err := deps.Session.LoadSession()
			if err != nil {
				return err
			}
			view, err := deps.Describe.Execute(command.Context(), session, args[0], args[1])
			if err != nil {
				return describeError(err)
			}
			if total <= len(view.Partitions) {
				return fmt.Errorf("%s already has %d partition(s); the new total must be higher", args[1], len(view.Partitions))
			}
			sentReason, err := PrepareChange(command.Context(), session, deps.Accessible, bufio.NewReader(command.InOrStdin()), command.ErrOrStderr(), ChangeRequest{
				Cluster: args[0], Yes: yes, Reason: reason,
				Summary: []string{fmt.Sprintf("Topic %s: %d → %d partitions (cannot be undone; keyed messages may map to new partitions)", args[1], len(view.Partitions), total)},
			})
			if err != nil {
				return err
			}
			if err := deps.Increase.Execute(command.Context(), session, args[0], args[1], total, sentReason); err != nil {
				return writeError(args[0], err)
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "Topic %s now has %d partitions.\n", args[1], total)
			return err
		},
	}
	reassign := &cobra.Command{
		Use:   "reassign <cluster> <topic> <partition> <broker,…>",
		Short: "Move a partition's replicas to the given brokers (the first is the preferred leader)",
		Args:  cobra.ExactArgs(4),
		RunE: func(command *cobra.Command, args []string) error {
			partition, err := strconv.ParseInt(args[2], 10, 32)
			if err != nil || partition < 0 {
				return fmt.Errorf("partition must be a partition number, not %q", args[2])
			}
			replicas, err := topics.ParseBrokerIDs(args[3])
			if err != nil {
				return err
			}
			session, err := deps.Session.LoadSession()
			if err != nil {
				return err
			}
			sentReason, err := PrepareChange(command.Context(), session, deps.Accessible, bufio.NewReader(command.InOrStdin()), command.ErrOrStderr(), ChangeRequest{
				Cluster: args[0], Yes: yes, Reason: reason,
				Summary: []string{fmt.Sprintf("Topic %s partition %d: move replicas to brokers %s", args[1], partition, joinIDs(replicas))},
			})
			if err != nil {
				return err
			}
			if err := deps.Reassign.Execute(command.Context(), session, args[0], args[1], int32(partition), replicas, sentReason); err != nil {
				return writeError(args[0], err)
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "Kafka accepted moving %s partition %d to brokers %s; follow it with kafka3o reassignments.\n", args[1], partition, joinIDs(replicas))
			return err
		},
	}
	addChangeFlags(add, &yes, &reason)
	addChangeFlags(reassign, &yes, &reason)
	partitions := &cobra.Command{Use: "partitions", Short: "Add partitions or move a partition's replicas"}
	partitions.AddCommand(add, reassign)
	return partitions
}
