package cli

import (
	"bufio"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/environments"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
)

// EnvironmentDependencies are what `kafka3o environment set` needs.
type EnvironmentDependencies struct {
	Session SessionDependencies
	List    environments.ListRegisteredClusters
	Change  environments.ChangeClusterEnvironment
}

// NewEnvironmentCommand reclassifies a cluster; a reason is always required and the environment read just before
// is sent as the expected one.
func NewEnvironmentCommand(deps EnvironmentDependencies) *cobra.Command {
	var yes bool
	var reason string
	environment := &cobra.Command{Use: "environment", Short: "Classify a cluster as Production or Non-production (local global ADMIN only)"}
	set := &cobra.Command{
		Use:   "set <cluster> <production|non-production>",
		Short: "Reclassify a cluster; a change reason is required",
		Args:  cobra.ExactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			target, err := environments.ParseEnvironment(args[1])
			if err != nil {
				return err
			}
			session, err := deps.Session.LoadSession()
			if err != nil {
				return err
			}
			listing, err := deps.List.Execute(command.Context(), session)
			if err != nil {
				return adminError(err)
			}
			var cluster *environments.RegisteredCluster
			for i := range listing.Clusters {
				if listing.Clusters[i].ID == args[0] {
					cluster = &listing.Clusters[i]
				}
			}
			if cluster == nil {
				return fmt.Errorf("cluster %s is not registered", args[0])
			}
			name := fmt.Sprintf("%s (%s)", messages.EscapeTerminalText(cluster.Name), cluster.ID)
			if cluster.Environment == target {
				_, err := fmt.Fprintf(command.OutOrStdout(), "Nothing to change: %s is already %s.\n", name, describeEnvironment(target))
				return err
			}
			input := bufio.NewReader(command.InOrStdin())
			given := strings.TrimSpace(reason)
			if given == "" && yes {
				return errors.New("a change reason is required to reclassify a cluster: add --reason")
			}
			if given == "" {
				fmt.Fprint(command.ErrOrStderr(), "Change reason (required): ")
				line, _ := input.ReadString('\n')
				if given = strings.TrimSpace(line); given == "" {
					return console.ErrCanceled
				}
			}
			old := describeEnvironment(cluster.Environment)
			if old == "" {
				old = "Unclassified"
			}
			summary := []string{
				fmt.Sprintf("Classify %s: %s → %s", name, old, describeEnvironment(target)),
				"reason: " + messages.EscapeTerminalText(given),
			}
			if err := ConfirmChange(input, command.ErrOrStderr(), summary, yes); err != nil {
				return err
			}
			result, err := deps.Change.Execute(command.Context(), session, cluster.ID, cluster.Environment, target, given)
			if err != nil {
				return adminError(err)
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "Classified %s as %s; policy revision %d (operation %s).\n", cluster.ID, describeEnvironment(target), result.PolicyRevision, result.OperationID)
			return err
		},
	}
	set.Flags().BoolVarP(&yes, "yes", "y", false, "do not ask for confirmation")
	set.Flags().StringVar(&reason, "reason", "", "change reason (always required; asked for when missing)")
	environment.AddCommand(set)
	return environment
}
