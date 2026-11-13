package cli

import (
	"errors"
	"fmt"
	"slices"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// NewSearchCommand searches a topic's most recent messages (the Gateway scans at most 500) for a text in the key
// or value, case-sensitive.
func NewSearchCommand(deps SessionDependencies, accessible clusters.ListAccessibleClusters, search messages.SearchMessages) *cobra.Command {
	var valueFormat string
	command := &cobra.Command{
		Use:   "search <cluster> <topic> <text>",
		Short: "Search a topic's recent messages for a text in the key or value",
		Args:  cobra.ExactArgs(3),
		RunE: func(command *cobra.Command, args []string) error {
			format, err := OutputFormat(command)
			if err != nil {
				return err
			}
			if !slices.Contains([]string{"text", "json", "base64"}, valueFormat) {
				return fmt.Errorf("--value must be text, json or base64, not %q", valueFormat)
			}
			if args[2] == "" {
				return errors.New("give a text to search for")
			}
			session, err := deps.LoadSession()
			if err != nil {
				return err
			}
			if err := clusters.RequireAccessibleCluster(command.Context(), session, accessible, args[0]); err != nil {
				return err
			}
			result, err := search.Execute(command.Context(), session, args[0], args[1], args[2])
			if err != nil {
				return topicReadError(args[0], args[1], err)
			}
			out := command.OutOrStdout()
			if format == "json" {
				return output.WriteJSON(out, result)
			}
			if len(result.Messages) == 0 {
				fmt.Fprintf(out, "No messages contain %s.\n", messages.EscapeTerminalText(args[2]))
			} else if err := messages.WriteMessages(out, result.Messages, valueFormat); err != nil {
				return err
			} else {
				fmt.Fprintln(out)
			}
			fmt.Fprintf(out, "Scanned %d messages.\n", result.Scanned)
			if result.Truncated {
				fmt.Fprintln(out, "Only the most recent messages were searched; older ones were not.")
			}
			return nil
		},
	}
	command.Flags().StringVar(&valueFormat, "value", "text", "show values as text, json or base64")
	return command
}
