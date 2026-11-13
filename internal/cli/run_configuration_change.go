package cli

import (
	"bufio"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
)

// ConfigurationDependencies are what `kafka3o config set|reset` needs.
type ConfigurationDependencies struct {
	Session    SessionDependencies
	Accessible clusters.ListAccessibleClusters
	Read       clusters.ReadConfiguration
	Change     clusters.ChangeConfiguration
}

// runConfigurationChange reads the setting first (so an unknown, sensitive or read-only one is refused before
// sending and the confirmation shows old → new), then sends the change.
func runConfigurationChange(command *cobra.Command, deps ConfigurationDependencies, cluster, kind, name string, change clusters.ConfigurationChange, yes bool, reason string) error {
	if kind != "topic" && kind != "broker" {
		return fmt.Errorf("the resource must be topic or broker, not %q", kind)
	}
	if id, err := strconv.Atoi(name); kind == "broker" && (err != nil || id < 0) {
		return fmt.Errorf("broker must be a broker id such as 1, not %q", name)
	}
	resource := clusters.ConfigurationResource{Kind: kind, Name: name}
	session, err := deps.Session.LoadSession()
	if err != nil {
		return err
	}
	if err := clusters.RequireAccessibleCluster(command.Context(), session, deps.Accessible, cluster); err != nil {
		return err
	}
	settings, err := deps.Read.Execute(command.Context(), session, cluster, resource)
	if err != nil {
		return topicReadError(cluster, name, err)
	}
	var current *clusters.Setting
	for i := range settings {
		if settings[i].Name == change.Name {
			current = &settings[i]
		}
	}
	switch {
	case current == nil:
		return fmt.Errorf("%s %s has no setting %q", kind, name, change.Name)
	case current.Sensitive:
		return fmt.Errorf("%s is sensitive and cannot be changed here", change.Name)
	case current.ReadOnly:
		return fmt.Errorf("%s is read-only (it needs a broker restart or cannot be changed dynamically)", change.Name)
	}
	before := "(not set)"
	if current.Value != nil {
		before = *current.Value
	}
	after := "the default"
	if change.Value != nil {
		after = *change.Value
	}
	sentReason, err := PrepareChange(command.Context(), session, deps.Accessible, bufio.NewReader(command.InOrStdin()), command.ErrOrStderr(), ChangeRequest{
		Cluster: cluster, Yes: yes, Reason: reason,
		Summary: []string{fmt.Sprintf("%s %s: %s: %s → %s", kind, name, change.Name, before, after)},
	})
	if err != nil {
		return err
	}
	if err := deps.Change.Execute(command.Context(), session, cluster, resource, change, sentReason); err != nil {
		return writeError(cluster, err)
	}
	_, err = fmt.Fprintf(command.OutOrStdout(), "%s of %s %s is now %s.\n", change.Name, kind, name, after)
	return err
}
