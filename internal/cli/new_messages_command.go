package cli

import (
	"fmt"
	"slices"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// NewMessagesCommand reads a topic's latest messages, or the messages from a partition's offset or timestamp.
func NewMessagesCommand(deps SessionDependencies, accessible clusters.ListAccessibleClusters, read messages.ReadMessages) *cobra.Command {
	var partition, offset, timestamp, valueFormat string
	command := &cobra.Command{
		Use:   "messages <cluster> <topic>",
		Short: "Read a topic's latest messages, or from an offset or a timestamp (up to 50)",
		Args:  cobra.ExactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			format, err := OutputFormat(command)
			if err != nil {
				return err
			}
			if !slices.Contains([]string{"text", "json", "base64"}, valueFormat) {
				return fmt.Errorf("--value must be text, json or base64, not %q", valueFormat)
			}
			position, err := messages.ParsePosition(partition, offset, timestamp)
			if err != nil {
				return err
			}
			session, err := deps.LoadSession()
			if err != nil {
				return err
			}
			if err := clusters.RequireAccessibleCluster(command.Context(), session, accessible, args[0]); err != nil {
				return err
			}
			found, err := read.Execute(command.Context(), session, args[0], args[1], position)
			if err != nil {
				return topicReadError(args[0], args[1], err)
			}
			if format == "json" {
				return output.WriteJSON(command.OutOrStdout(), found)
			}
			return messages.WriteMessages(command.OutOrStdout(), found, valueFormat)
		},
	}
	command.Flags().StringVar(&partition, "partition", "", "partition to read from (with --offset or --timestamp)")
	command.Flags().StringVar(&offset, "offset", "", "first offset to read")
	command.Flags().StringVar(&timestamp, "timestamp", "", "read from this time: RFC 3339 (2026-11-12T04:08:00Z) or Unix milliseconds")
	command.Flags().StringVar(&valueFormat, "value", "text", "show values as text, json or base64")
	return command
}
