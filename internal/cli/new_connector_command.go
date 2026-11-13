package cli

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/connect"
)

// NewConnectorCommand groups the connector lifecycle actions; Connect applies each one asynchronously.
func NewConnectorCommand(deps ConnectorDependencies) *cobra.Command {
	var yes bool
	var reason string
	connector := &cobra.Command{Use: "connector", Short: "Pause, resume or restart a Kafka Connect connector or one of its tasks"}
	for _, kind := range []struct{ name, short string }{
		{connect.ActionPause, "Pause a connector"},
		{connect.ActionResume, "Resume a paused connector"},
		{connect.ActionRestart, "Restart a connector and its tasks"},
	} {
		action := &cobra.Command{
			Use:   kind.name + " <cluster> <connector>",
			Short: kind.short,
			Args:  cobra.ExactArgs(2),
			RunE: func(command *cobra.Command, args []string) error {
				return runConnectorChange(command, deps, args[0], connect.ConnectorAction{Kind: kind.name, Connector: args[1]}, yes, reason)
			},
		}
		addChangeFlags(action, &yes, &reason)
		connector.AddCommand(action)
	}
	restartTask := &cobra.Command{
		Use:   "restart-task <cluster> <connector> <task>",
		Short: "Restart one task of a connector",
		Args:  cobra.ExactArgs(3),
		RunE: func(command *cobra.Command, args []string) error {
			task, err := strconv.ParseInt(args[2], 10, 32)
			if err != nil || task < 0 {
				return fmt.Errorf("task must be a task id such as 0, not %q", args[2])
			}
			return runConnectorChange(command, deps, args[0], connect.ConnectorAction{Kind: connect.ActionRestartTask, Connector: args[1], Task: int32(task)}, yes, reason)
		},
	}
	addChangeFlags(restartTask, &yes, &reason)
	connector.AddCommand(restartTask)
	return connector
}
