package cli

import (
	"bufio"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/connect"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
)

// ConnectorDependencies are what `kafka3o connector …` needs.
type ConnectorDependencies struct {
	Session    SessionDependencies
	Accessible clusters.ListAccessibleClusters
	Run        connect.RunConnectorAction
}

// runConnectorChange confirms a connector action with the UI's wording, sends it and prints what was requested.
func runConnectorChange(command *cobra.Command, deps ConnectorDependencies, cluster string, action connect.ConnectorAction, yes bool, reason string) error {
	session, err := deps.Session.LoadSession()
	if err != nil {
		return err
	}
	name := messages.EscapeTerminalText(action.Connector)
	summary := map[string]string{
		connect.ActionPause:       fmt.Sprintf("Pause connector \"%s\"", name),
		connect.ActionResume:      fmt.Sprintf("Resume connector \"%s\"", name),
		connect.ActionRestart:     fmt.Sprintf("Restart connector \"%s\" and its tasks", name),
		connect.ActionRestartTask: fmt.Sprintf("Restart task %d of connector \"%s\"", action.Task, name),
	}[action.Kind]
	sentReason, err := PrepareChange(command.Context(), session, deps.Accessible, bufio.NewReader(command.InOrStdin()), command.ErrOrStderr(), ChangeRequest{
		Cluster: cluster, Yes: yes, Reason: reason, Summary: []string{summary},
	})
	if err != nil {
		return err
	}
	if err := deps.Run.Execute(command.Context(), session, cluster, action, sentReason); err != nil {
		return writeError(cluster, err)
	}
	requested := map[string]string{
		connect.ActionPause:       fmt.Sprintf("Pause requested for %s.", name),
		connect.ActionResume:      fmt.Sprintf("Resume requested for %s.", name),
		connect.ActionRestart:     fmt.Sprintf("Restart requested for %s.", name),
		connect.ActionRestartTask: fmt.Sprintf("Restart requested for task %d of %s.", action.Task, name),
	}[action.Kind]
	_, err = fmt.Fprintln(command.OutOrStdout(), requested)
	return err
}
