package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/groups"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// NewGroupCommand shows one consumer group's members and its offsets per topic and partition.
func NewGroupCommand(deps SessionDependencies, list groups.ListGroups) *cobra.Command {
	return &cobra.Command{
		Use:   "group <cluster> <group>",
		Short: "Show a consumer group's members and offsets per topic and partition",
		Args:  cobra.ExactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			format, err := OutputFormat(command)
			if err != nil {
				return err
			}
			session, err := deps.LoadSession()
			if err != nil {
				return err
			}
			found, failures, err := list.Execute(command.Context(), session, args[0])
			if err != nil {
				return err
			}
			if len(failures) > 0 {
				return fmt.Errorf("%s: %s: %w", failures[0].Cluster, describeClusterStatus(clustersHealthOf(failures[0])), ErrClusterFailed)
			}
			for _, group := range found {
				if group.ID != args[1] {
					continue
				}
				if format == "json" {
					return output.WriteJSON(command.OutOrStdout(), group)
				}
				return groups.WriteGroup(command.OutOrStdout(), group)
			}
			return groups.ErrGroupNotFound{Cluster: args[0], Group: args[1]}
		},
	}
}
