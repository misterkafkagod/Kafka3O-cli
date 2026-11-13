package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/metrics"
)

// metricsRead is one cluster's metrics document, raw and typed.
type metricsRead struct {
	raw   json.RawMessage
	typed metrics.ClusterMetrics
}

// NewMetricsCommand shows one cluster's Metrics tab; -o json writes the Gateway's document unchanged and -o csv the
// rows of the UI's CSV download.
func NewMetricsCommand(deps SessionDependencies, accessible clusters.ListAccessibleClusters, read metrics.ReadMetrics) *cobra.Command {
	return &cobra.Command{
		Use:   "metrics <cluster>",
		Short: "Show a cluster's Kafka metrics (-o json or -o csv to export them)",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			format, err := OutputFormat(command, "csv")
			if err != nil {
				return err
			}
			session, err := deps.LoadSession()
			if err != nil {
				return err
			}
			reads, failures, err := clusters.ReadAcrossClusters(command.Context(), session, accessible, args[0],
				func(ctx context.Context, cluster clusters.AccessibleCluster) (metricsRead, error) {
					raw, typed, err := read.Execute(ctx, session, cluster.ID)
					return metricsRead{raw: raw, typed: typed}, err
				})
			if err != nil {
				return err
			}
			if len(failures) > 0 {
				return fmt.Errorf("%s: %s: %w", failures[0].Cluster, describeClusterStatus(clustersHealthOf(failures[0])), ErrClusterFailed)
			}
			out := command.OutOrStdout()
			switch format {
			case "json":
				_, err := fmt.Fprintf(out, "%s\n", reads[0].raw)
				return err
			case "csv":
				_, err := io.WriteString(out, metrics.MetricsToCSV(reads[0].typed))
				return err
			default:
				return metrics.WriteMetricsText(out, args[0], reads[0].typed)
			}
		},
	}
}
