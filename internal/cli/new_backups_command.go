package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/backups"
	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

type backupList struct {
	Clusters []backups.ClusterBackups  `json:"clusters"`
	Failures []clusters.ClusterFailure `json:"failures"`
}

// NewBackupsCommand lists topic backups across every accessible cluster (or one with --cluster), like the
// cluster's Backups tab.
func NewBackupsCommand(deps SessionDependencies, list backups.ListBackups) *cobra.Command {
	var cluster string
	command := &cobra.Command{
		Use:   "backups",
		Short: "List topic backups across every cluster you may access",
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
			result := error(nil)
			if len(failures) > 0 {
				result = fmt.Errorf("%d cluster(s) could not list backups: %w", len(failures), ErrClusterFailed)
			}
			if format == "json" {
				if err := output.WriteJSON(command.OutOrStdout(), backupList{Clusters: found, Failures: failures}); err != nil {
					return err
				}
				return result
			}
			for _, failure := range failures {
				fmt.Fprintf(command.ErrOrStderr(), "%s: %s\n", failure.Cluster, describeClusterStatus(clustersHealthOf(failure)))
			}
			rows, problems, notConfigured := [][]string{}, []string{}, []string{}
			for _, answered := range found {
				if answered.NotConfigured {
					notConfigured = append(notConfigured, answered.Cluster+": Backups are not configured for this cluster.")
				}
				for _, backup := range answered.Backups {
					topic := messages.EscapeTerminalText(backup.Topic)
					if backup.Error != "" {
						problems = append(problems, fmt.Sprintf("%s %s: %s", answered.Cluster, topic, messages.EscapeTerminalText(backup.Error)))
					}
					lastWrite, stored := "unavailable", "unavailable"
					if backup.Storage != nil {
						lastWrite = describeLastWrite(backup.Storage.LastWrite)
						stored = fmt.Sprintf("%d bytes in %d objects", backup.Storage.StoredBytes, backup.Storage.Objects)
					}
					settings := describeBackupSettings(backup.Settings)
					if settings == "" {
						settings = "unknown"
					}
					restore := describeBackupRestore(backup.Restore)
					switch {
					case restore == "" && backup.Restoring:
						restore = "restoring (progress not checked)"
					case restore == "":
						restore = "none"
					}
					state := messages.EscapeTerminalText(backup.State)
					if state == "" {
						state = "unknown"
					}
					rows = append(rows, []string{answered.Cluster, topic, state, lastWrite, stored, settings, restore})
				}
			}
			if len(rows) == 0 {
				fmt.Fprintln(command.OutOrStdout(), "No backups.")
			} else if err := output.WriteTable(command.OutOrStdout(), []string{"CLUSTER", "TOPIC", "STATE", "LAST WRITE", "STORED", "SETTINGS", "RESTORE"}, rows); err != nil {
				return err
			}
			if len(problems) > 0 {
				fmt.Fprintf(command.OutOrStdout(), "\nErrors\n%s\n", strings.Join(problems, "\n"))
			}
			if len(notConfigured) > 0 {
				fmt.Fprintf(command.OutOrStdout(), "\n%s\n", strings.Join(notConfigured, "\n"))
			}
			return result
		},
	}
	command.Flags().StringVar(&cluster, "cluster", "", "only this cluster (default: every cluster you may access)")
	return command
}
