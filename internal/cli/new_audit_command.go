package cli

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/audit"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// NewAuditCommand queries the audit trail the signed-in user may see, one page at a time, oldest first.
func NewAuditCommand(deps SessionDependencies, list audit.ListAuditEvents) *cobra.Command {
	var filters audit.Filters
	command := &cobra.Command{
		Use:   "audit",
		Short: "Query the audit trail (all clusters, or one with --cluster)",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			format, err := OutputFormat(command)
			if err != nil {
				return err
			}
			query, err := audit.BuildAuditQuery(filters)
			if err != nil {
				return err
			}
			session, err := deps.LoadSession()
			if err != nil {
				return err
			}
			page, err := list.Execute(command.Context(), session, query)
			if err != nil {
				return adminError(err)
			}
			if format == "json" {
				return output.WriteJSON(command.OutOrStdout(), page)
			}
			if len(page.Events) == 0 {
				_, err := fmt.Fprintln(command.OutOrStdout(), "No audit events.")
				return err
			}
			rows := [][]string{}
			for _, event := range page.Events {
				when := event.OccurredAt
				if at, err := time.Parse(time.RFC3339Nano, event.OccurredAt); err == nil {
					when = at.In(time.Local).Format("2006-01-02 15:04:05")
				}
				cluster, resource := event.ClusterID, event.ResourceKind+":"+event.ResourceID
				if cluster == "" {
					cluster = "-"
				}
				if event.ResourceKind == "" && event.ResourceID == "" {
					resource = "-"
				}
				row := []string{when, event.OperationID, event.ActorID, event.Source, cluster, event.Action, resource, event.Decision, event.Outcome, event.Reason}
				for i := range row {
					row[i] = messages.EscapeTerminalText(row[i])
				}
				rows = append(rows, row)
			}
			if err := output.WriteTable(command.OutOrStdout(), []string{"TIME", "OPERATION", "ACTOR", "SOURCE", "CLUSTER", "ACTION", "RESOURCE", "DECISION", "OUTCOME", "REASON"}, rows); err != nil {
				return err
			}
			if page.NextCursor != "" {
				_, err := fmt.Fprintf(command.OutOrStdout(), "More events: --after %s\n", messages.EscapeTerminalText(page.NextCursor))
				return err
			}
			return nil
		},
	}
	command.Flags().StringVar(&filters.Cluster, "cluster", "", "only this cluster's events (default: all clusters, which needs global ADMIN)")
	command.Flags().StringVar(&filters.Actor, "actor", "", "only events by this actor id, for example local:admin")
	command.Flags().StringVar(&filters.Action, "action", "", "only this action, for example topic.delete")
	command.Flags().StringVar(&filters.Outcome, "outcome", "", "only this outcome (attempted, denied, validation_rejected, accepted, confirmed, failed_before_send, unknown, succeeded, failed)")
	command.Flags().StringVar(&filters.From, "from", "", "only events at or after this time (RFC 3339 or Unix milliseconds)")
	command.Flags().StringVar(&filters.Until, "until", "", "only events before this time (RFC 3339 or Unix milliseconds)")
	command.Flags().IntVar(&filters.Limit, "limit", 0, "events per page, 1 to 500 (default: the Console's 100)")
	command.Flags().StringVar(&filters.After, "after", "", "continue after this cursor, printed with the previous page")
	return command
}
