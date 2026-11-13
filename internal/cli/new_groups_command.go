package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/groups"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// groupSummary is one row of `kafka3o groups --output json`.
type groupSummary struct {
	Cluster  string     `json:"cluster"`
	Group    string     `json:"group"`
	State    string     `json:"state"`
	Members  int        `json:"members"`
	Topics   []string   `json:"topics"`
	TotalLag groups.Lag `json:"totalLag"`
	Error    string     `json:"error,omitempty"`
}

type groupList struct {
	Groups   []groupSummary            `json:"groups"`
	Failures []clusters.ClusterFailure `json:"failures"`
}

// NewGroupsCommand lists consumer groups across every accessible cluster (or one with --cluster) with exact lag.
func NewGroupsCommand(deps SessionDependencies, list groups.ListGroups) *cobra.Command {
	var cluster, filter string
	command := &cobra.Command{
		Use:   "groups",
		Short: "List consumer groups across every cluster you may access, with their total lag",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			format, err := OutputFormat(command)
			if err != nil {
				return err
			}
			session, err := deps.LoadSession()
			if err != nil {
				return err
			}
			found, failures, err := list.Execute(command.Context(), session, cluster)
			if err != nil {
				return err
			}
			summaries := []groupSummary{}
			for _, group := range found {
				if !strings.Contains(strings.ToLower(group.ID), strings.ToLower(filter)) {
					continue
				}
				summaries = append(summaries, groupSummary{Cluster: group.Cluster, Group: group.ID, State: group.State, Members: len(group.Members),
					Topics: group.Topics, TotalLag: groups.TotalLag(group), Error: group.Error})
			}
			result := error(nil)
			if len(failures) > 0 {
				result = fmt.Errorf("%d cluster(s) could not list consumer groups: %w", len(failures), ErrClusterFailed)
			}
			if format == "json" {
				if err := output.WriteJSON(command.OutOrStdout(), groupList{Groups: summaries, Failures: failures}); err != nil {
					return err
				}
				return result
			}
			for _, failure := range failures {
				fmt.Fprintf(command.ErrOrStderr(), "%s: %s\n", failure.Cluster, describeClusterStatus(clustersHealthOf(failure)))
			}
			if len(summaries) == 0 {
				fmt.Fprintln(command.OutOrStdout(), "No consumer groups.")
				return result
			}
			rows := make([][]string, 0, len(summaries))
			for _, group := range summaries {
				lag := describeTotalLag(group.TotalLag)
				if group.Error != "" {
					lag = "unavailable: " + group.Error
				}
				rows = append(rows, []string{group.Cluster, group.Group, group.State, strconv.Itoa(group.Members), strconv.Itoa(len(group.Topics)), lag})
			}
			if err := output.WriteTable(command.OutOrStdout(), []string{"CLUSTER", "GROUP", "STATE", "MEMBERS", "TOPICS", "TOTAL LAG"}, rows); err != nil {
				return err
			}
			return result
		},
	}
	command.Flags().StringVar(&cluster, "cluster", "", "only this cluster (default: every cluster you may access)")
	command.Flags().StringVar(&filter, "filter", "", "only groups whose id contains this text (case-insensitive)")
	return command
}
