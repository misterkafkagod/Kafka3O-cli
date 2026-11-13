package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// NewBrokerConfigCommand shows one broker's configuration; sensitive values are never printed.
func NewBrokerConfigCommand(deps SessionDependencies, accessible clusters.ListAccessibleClusters, read clusters.ReadBrokerConfig) *cobra.Command {
	var filter string
	command := &cobra.Command{
		Use:   "broker-config <cluster> <broker>",
		Short: "Show a broker's configuration",
		Args:  cobra.ExactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			format, err := OutputFormat(command)
			if err != nil {
				return err
			}
			broker, err := strconv.ParseInt(args[1], 10, 32)
			if err != nil || broker < 0 {
				return fmt.Errorf("broker must be a broker id such as 1, not %q", args[1])
			}
			session, err := deps.LoadSession()
			if err != nil {
				return err
			}
			settings, failures, err := clusters.ReadAcrossClusters(command.Context(), session, accessible, args[0],
				func(ctx context.Context, cluster clusters.AccessibleCluster) ([]clusters.BrokerSetting, error) {
					return read.Execute(ctx, session, cluster.ID, int32(broker))
				})
			if err != nil {
				return err
			}
			if len(failures) > 0 {
				return fmt.Errorf("%s: %s: %w", failures[0].Cluster, describeClusterStatus(clustersHealthOf(failures[0])), ErrClusterFailed)
			}
			shown := []clusters.BrokerSetting{}
			for _, setting := range settings[0] {
				if strings.Contains(strings.ToLower(setting.Name), strings.ToLower(filter)) {
					shown = append(shown, setting)
				}
			}
			if format == "json" {
				return output.WriteJSON(command.OutOrStdout(), shown)
			}
			rows := make([][]string, 0, len(shown))
			for _, setting := range shown {
				value := orUnknown(setting.Value)
				if setting.Value == nil {
					value = "(not set)"
				}
				if setting.Sensitive {
					value = "(sensitive, hidden)"
				}
				readOnly := "no"
				if setting.ReadOnly {
					readOnly = "yes"
				}
				rows = append(rows, []string{setting.Name, value, strings.ToLower(strings.ReplaceAll(setting.Source, "_", " ")), readOnly})
			}
			return output.WriteTable(command.OutOrStdout(), []string{"NAME", "VALUE", "SOURCE", "READ-ONLY"}, rows)
		},
	}
	command.Flags().StringVar(&filter, "filter", "", "only settings whose name contains this text (case-insensitive)")
	return command
}
