package cli

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/backups"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// NewRestoreCommand appends a topic's backed-up messages (all, or a time range and/or partitions) to the topic,
// like the topic's Backup tab, and with --wait follows the restore to its end.
func NewRestoreCommand(deps BackupDependencies) *cobra.Command {
	var from, to, partitions, reason string
	var yes, wait bool
	command := &cobra.Command{
		Use:   "restore <cluster> <topic>",
		Short: "Append a topic's backed-up messages to the topic (all, or a time range and partitions)",
		Args:  cobra.ExactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			cluster, topic := args[0], args[1]
			if err := topics.ValidateTopicName(topic); err != nil {
				return err
			}
			chosen := command.Flags().Changed("from") || command.Flags().Changed("to") || command.Flags().Changed("partitions")
			selection, err := backups.ParseRestoreSelection(from, to, partitions, chosen)
			if err != nil {
				return err
			}
			scope := fmt.Sprintf("Restore the backup of \"%s\" into \"%s\"", topic, topic)
			if selection != nil {
				parts := []string{fmt.Sprintf("Restore the backed-up messages of \"%s\"", topic)}
				if selection.Partitions != nil {
					numbers := []string{}
					for _, partition := range selection.Partitions {
						numbers = append(numbers, strconv.Itoa(partition))
					}
					label := "in partitions "
					if len(numbers) == 1 {
						label = "in partition "
					}
					parts = append(parts, label+strings.Join(numbers, ", "))
				}
				if selection.From != nil {
					parts = append(parts, "from "+selection.From.In(time.Local).Format("2006-01-02 15:04:05 MST"))
				}
				if selection.To != nil {
					parts = append(parts, "before "+selection.To.In(time.Local).Format("2006-01-02 15:04:05 MST"))
				}
				scope = strings.Join(parts, " ") + fmt.Sprintf(" into \"%s\"", topic)
			}
			session, err := deps.Session.LoadSession()
			if err != nil {
				return err
			}
			sentReason, err := PrepareChange(command.Context(), session, deps.Accessible, bufio.NewReader(command.InOrStdin()), command.ErrOrStderr(), ChangeRequest{
				Cluster: cluster, Yes: yes, Reason: reason,
				Summary: []string{scope, "the saved messages are appended to the topic, so messages still in it may be duplicated",
					"the topic's backup pauses until the restore finishes"},
			})
			if err != nil {
				return err
			}
			started, err := deps.Restore.Execute(command.Context(), session, cluster, topic, selection, sentReason)
			if err != nil {
				return backupError(cluster, topic, err)
			}
			out := command.OutOrStdout()
			fmt.Fprintf(out, "Restore started for %s from %d stored object(s).\n", topic, started.Objects)
			if started.Skipped > 0 {
				fmt.Fprintf(out, "%d older object(s) written with another compression will not be restored.\n", started.Skipped)
			}
			if !wait {
				_, err := fmt.Fprintf(out, "Follow it with: kafka3o backups --cluster %s (listing backups also finishes the restore)\n", cluster)
				return err
			}
			outcome, err := deps.Wait.Execute(command.Context(), session, cluster, topic, started.RestoreID, out)
			switch {
			case err != nil:
				return describeError(err)
			case outcome.Error != "":
				return fmt.Errorf("restore failed after %d of %d records: %s: %w", outcome.Restored, outcome.Total, messages.EscapeTerminalText(outcome.Error), ErrClusterFailed)
			case outcome.Gone:
				_, err = fmt.Fprintln(out, "Restore finished.")
			default:
				_, err = fmt.Fprintf(out, "Restore finished: %d records restored.\n", outcome.Total)
			}
			return err
		},
	}
	command.Flags().StringVar(&from, "from", "", "only messages from this time (RFC 3339 or Unix milliseconds, inclusive)")
	command.Flags().StringVar(&to, "to", "", "only messages before this time (RFC 3339 or Unix milliseconds, exclusive)")
	command.Flags().StringVar(&partitions, "partitions", "", "only these partitions, for example 0,2")
	command.Flags().BoolVar(&wait, "wait", false, "follow the restore until it finishes or fails")
	addChangeFlags(command, &yes, &reason)
	return command
}
