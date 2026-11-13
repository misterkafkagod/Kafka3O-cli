package cli

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// NewReassignmentsCommand lists a topic's partition moves in progress.
func NewReassignmentsCommand(deps SessionDependencies, accessible clusters.ListAccessibleClusters, list topics.ListReassignments) *cobra.Command {
	return &cobra.Command{
		Use:   "reassignments <cluster> <topic>",
		Short: "List a topic's partition moves in progress",
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
			if err := clusters.RequireAccessibleCluster(command.Context(), session, accessible, args[0]); err != nil {
				return err
			}
			moves, err := list.Execute(command.Context(), session, args[0], args[1])
			if err != nil {
				return topicReadError(args[0], args[1], err)
			}
			if format == "json" {
				return output.WriteJSON(command.OutOrStdout(), moves)
			}
			if len(moves) == 0 {
				_, err := fmt.Fprintln(command.OutOrStdout(), "No reassignments in progress.")
				return err
			}
			rows := make([][]string, 0, len(moves))
			for _, move := range moves {
				rows = append(rows, []string{strconv.Itoa(int(move.Partition)), joinIDs(move.Replicas), joinIDs(move.AddingReplicas), joinIDs(move.RemovingReplicas)})
			}
			return output.WriteTable(command.OutOrStdout(), []string{"PARTITION", "REPLICAS", "ADDING", "REMOVING"}, rows)
		},
	}
}
