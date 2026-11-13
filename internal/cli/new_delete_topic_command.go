package cli

import (
	"bufio"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// NewDeleteTopicCommand deletes a topic only after its name is typed again (or given with --confirm), then the
// usual confirmation and Production reason.
func NewDeleteTopicCommand(deps SessionDependencies, accessible clusters.ListAccessibleClusters, remove topics.DeleteTopic) *cobra.Command {
	var confirm, reason string
	var yes bool
	command := &cobra.Command{
		Use:   "delete-topic <cluster> <topic>",
		Short: "Delete a topic and all its messages",
		Args:  cobra.ExactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			if err := topics.ValidateTopicName(args[1]); err != nil {
				return err
			}
			input := bufio.NewReader(command.InOrStdin())
			if !command.Flags().Changed("confirm") {
				if yes {
					return errors.New("--yes alone cannot delete a topic: add --confirm <topic>")
				}
				fmt.Fprintf(command.ErrOrStderr(), "Deleting %s on %s removes all its messages. Type the topic name to delete it: ", args[1], args[0])
				line, _ := input.ReadString('\n')
				confirm = strings.TrimSpace(line)
			}
			if confirm != args[1] {
				return fmt.Errorf("the typed name does not match %s: %w", args[1], console.ErrCanceled)
			}
			session, err := deps.LoadSession()
			if err != nil {
				return err
			}
			sentReason, err := PrepareChange(command.Context(), session, accessible, input, command.ErrOrStderr(), ChangeRequest{
				Cluster: args[0], Yes: yes, Reason: reason, Summary: []string{"Delete topic " + args[1] + " and all its messages"},
			})
			if err != nil {
				return err
			}
			if err := remove.Execute(command.Context(), session, args[0], args[1], sentReason); err != nil {
				return writeError(args[0], err)
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "Deleted topic %s on %s.\n", args[1], args[0])
			return err
		},
	}
	command.Flags().StringVar(&confirm, "confirm", "", "the topic name again, to delete without typing it")
	addChangeFlags(command, &yes, &reason)
	return command
}
