package cli

import (
	"bufio"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/groups"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// ResetDependencies are what `kafka3o reset-offsets` needs.
type ResetDependencies struct {
	Session    SessionDependencies
	Accessible clusters.ListAccessibleClusters
	Preview    groups.PreviewOffsetReset
	Apply      groups.ApplyOffsetReset
}

// NewResetOffsetsCommand previews a consumer group's offset reset for one topic, then applies exactly that preview
// after confirmation (the group must be inactive).
func NewResetOffsetsCommand(deps ResetDependencies) *cobra.Command {
	var to, reason string
	var yes bool
	command := &cobra.Command{
		Use:   "reset-offsets <cluster> <group> <topic>",
		Short: "Reset an inactive consumer group's offsets on a topic, with a preview",
		Args:  cobra.ExactArgs(3),
		RunE: func(command *cobra.Command, args []string) error {
			target, err := groups.ParseResetTarget(to)
			if err != nil {
				return err
			}
			session, err := deps.Session.LoadSession()
			if err != nil {
				return err
			}
			if err := clusters.RequireAccessibleCluster(command.Context(), session, deps.Accessible, args[0]); err != nil {
				return err
			}
			preview, err := deps.Preview.Execute(command.Context(), session, args[0], args[1], args[2], target)
			if err != nil {
				return writeError(args[0], err)
			}
			rows := [][]string{}
			for _, partition := range preview.Partitions {
				current := "none"
				if partition.CurrentOffset != nil {
					current = *partition.CurrentOffset
				}
				rows = append(rows, []string{strconv.Itoa(int(partition.Partition)), current, partition.TargetOffset})
			}
			fmt.Fprintf(command.ErrOrStderr(), "Preview for group %s on topic %s (to %s):\n", args[1], args[2], to)
			if err := output.WriteTable(command.ErrOrStderr(), []string{"PARTITION", "CURRENT", "NEW"}, rows); err != nil {
				return err
			}
			sentReason, err := PrepareChange(command.Context(), session, deps.Accessible, bufio.NewReader(command.InOrStdin()), command.ErrOrStderr(), ChangeRequest{
				Cluster: args[0], Yes: yes, Reason: reason,
				Summary: []string{fmt.Sprintf("Reset group %s on topic %s to %s (%d partition(s), as previewed)", args[1], args[2], to, len(preview.Partitions))},
			})
			if err != nil {
				return err
			}
			applied, err := deps.Apply.Execute(command.Context(), session, args[0], preview, sentReason)
			if err != nil {
				return writeError(args[0], err)
			}
			committed := [][]string{}
			for _, partition := range applied.Partitions {
				committed = append(committed, []string{strconv.Itoa(int(partition.Partition)), partition.CommittedOffset})
			}
			fmt.Fprintf(command.OutOrStdout(), "Committed offsets for group %s on topic %s:\n", applied.Group, applied.Topic)
			return output.WriteTable(command.OutOrStdout(), []string{"PARTITION", "COMMITTED"}, committed)
		},
	}
	command.Flags().StringVar(&to, "to", "", "earliest, latest, or a time (RFC 3339 or Unix milliseconds)")
	addChangeFlags(command, &yes, &reason)
	return command
}
