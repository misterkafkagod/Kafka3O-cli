package cli

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// NewProduceCommand writes one test message (text key, value and headers) to a topic, after confirmation.
func NewProduceCommand(deps SessionDependencies, accessible clusters.ListAccessibleClusters, produce messages.ProduceMessage) *cobra.Command {
	var value, key, reason string
	var headers []string
	var yes bool
	command := &cobra.Command{
		Use:   "produce <cluster> <topic>",
		Short: "Write a test message to a topic",
		Args:  cobra.ExactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			format, err := OutputFormat(command)
			if err != nil {
				return err
			}
			if !command.Flags().Changed("value") {
				return fmt.Errorf("--value is required")
			}
			message := messages.NewMessage{Value: value, Headers: []messages.TextHeader{}}
			if command.Flags().Changed("key") {
				message.Key = &key
			}
			summary := []string{"Produce to topic " + args[1]}
			for _, header := range headers {
				name, headerValue, found := strings.Cut(header, "=")
				if !found || name == "" {
					return fmt.Errorf("--header must be key=value, not %q", header)
				}
				message.Headers = append(message.Headers, messages.TextHeader{Key: name, Value: headerValue})
				summary = append(summary, "header "+messages.EscapeTerminalText(name)+": "+messages.EscapeTerminalText(headerValue))
			}
			if message.Key != nil {
				summary = append(summary, "key: "+messages.EscapeTerminalText(*message.Key))
			}
			summary = append(summary, "value: "+messages.EscapeTerminalText(value))
			session, err := deps.LoadSession()
			if err != nil {
				return err
			}
			sentReason, err := PrepareChange(command.Context(), session, accessible, bufio.NewReader(command.InOrStdin()), command.ErrOrStderr(),
				ChangeRequest{Cluster: args[0], Summary: summary, Yes: yes, Reason: reason})
			if err != nil {
				return err
			}
			written, err := produce.Execute(command.Context(), session, args[0], args[1], message, sentReason)
			if err != nil {
				return writeError(args[0], err)
			}
			if format == "json" {
				return output.WriteJSON(command.OutOrStdout(), written)
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "Written to partition %d at offset %s.\n", written.Partition, written.Offset)
			return err
		},
	}
	command.Flags().StringVar(&value, "value", "", "message value (text)")
	command.Flags().StringVar(&key, "key", "", "message key (text); no key when omitted")
	command.Flags().StringArrayVar(&headers, "header", nil, "header as key=value; repeat for more, kept in order")
	addChangeFlags(command, &yes, &reason)
	return command
}
