package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// SessionDependencies are what every signed-in command needs: the saved session and Console reads.
type SessionDependencies struct {
	LoadSession func() (console.Session, error)
	GetJSON     console.GetJSON
}

// whoami is the JSON shape of `kafka3o whoami --output json`.
type whoami struct {
	Username       string                       `json:"username"`
	AccountID      string                       `json:"accountId"`
	AuthSource     string                       `json:"authSource"`
	Console        string                       `json:"console"`
	ExpiresAt      time.Time                    `json:"expiresAt"`
	Administration map[string]bool              `json:"administration"`
	Clusters       []clusters.AccessibleCluster `json:"clusters"`
}

// NewWhoamiCommand shows who is signed in, until when, and the role on every accessible cluster.
func NewWhoamiCommand(deps SessionDependencies, list clusters.ListAccessibleClusters) *cobra.Command {
	return &cobra.Command{
		Use:   "whoami",
		Short: "Show the signed-in account and its role on every cluster",
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
			var account struct {
				AccountID         string          `json:"accountId"`
				Username          string          `json:"username"`
				AuthSource        string          `json:"authSource"`
				AbsoluteExpiresAt time.Time       `json:"absoluteExpiresAt"`
				Administration    map[string]bool `json:"administration"`
			}
			if err := deps.GetJSON.Execute(command.Context(), session, "/api/session", &account); err != nil {
				return err
			}
			accessible, err := list.Execute(command.Context(), session)
			if err != nil {
				return err
			}
			result := whoami{Username: account.Username, AccountID: account.AccountID, AuthSource: account.AuthSource, Console: session.ConsoleURL,
				ExpiresAt: account.AbsoluteExpiresAt, Administration: account.Administration, Clusters: accessible}
			if format == "json" {
				return output.WriteJSON(command.OutOrStdout(), result)
			}
			rights := []string{}
			for _, right := range []string{"access", "audit"} {
				if result.Administration[right] {
					rights = append(rights, right)
				}
			}
			if len(rights) == 0 {
				rights = append(rights, "none")
			}
			out := command.OutOrStdout()
			fmt.Fprintf(out, "Account:         %s (%s)\nConsole:         %s\nSession until:   %s\nAdministration:  %s\n\n",
				result.Username, result.AuthSource, result.Console, result.ExpiresAt.In(time.Local).Format("2006-01-02 15:04 MST"), strings.Join(rights, ", "))
			rows := make([][]string, 0, len(accessible))
			for _, cluster := range accessible {
				rows = append(rows, []string{cluster.ID, cluster.Name, describeEnvironment(cluster.Environment), cluster.EffectiveRole})
			}
			return output.WriteTable(out, []string{"CLUSTER", "NAME", "ENVIRONMENT", "ROLE"}, rows)
		},
	}
}
