package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/audit"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// NewAuditExportCommand writes the Console's CSV export of the matching audit events to standard output or a new file.
func NewAuditExportCommand(deps SessionDependencies, export audit.ExportAuditEvents) *cobra.Command {
	var filters audit.Filters
	var file string
	command := &cobra.Command{
		Use:   "export",
		Short: "Export the matching audit events as the Console's CSV",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			query, err := audit.BuildAuditQuery(filters)
			if err != nil {
				return err
			}
			session, err := deps.LoadSession()
			if err != nil {
				return err
			}
			data, events, err := export.Execute(command.Context(), session, query)
			if err != nil {
				return adminError(err)
			}
			if file == "" {
				_, err := command.OutOrStdout().Write(data)
				return err
			}
			if err := output.WriteNewFile(file, data); err != nil {
				return err
			}
			_, err = fmt.Fprintf(command.ErrOrStderr(), "Wrote %d events to %s.\n", events, file)
			return err
		},
	}
	command.Flags().StringVar(&filters.Cluster, "cluster", "", "only this cluster's events (default: all clusters, which needs global ADMIN)")
	command.Flags().StringVar(&filters.Actor, "actor", "", "only events by this actor id")
	command.Flags().StringVar(&filters.Action, "action", "", "only this action, for example topic.delete")
	command.Flags().StringVar(&filters.Outcome, "outcome", "", "only this outcome, as in `kafka3o audit`")
	command.Flags().StringVar(&filters.From, "from", "", "only events at or after this time (RFC 3339 or Unix milliseconds)")
	command.Flags().StringVar(&filters.Until, "until", "", "only events before this time (RFC 3339 or Unix milliseconds)")
	command.Flags().StringVar(&file, "file", "", "write to this new file (readable only by you) instead of standard output")
	return command
}
