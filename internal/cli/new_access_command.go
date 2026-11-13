package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/access"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// AccessDependencies are what `kafka3o access …` needs.
type AccessDependencies struct {
	Session SessionDependencies
	List    access.ListAccess
	Grant   access.ChangeRoleGrant
	Map     access.ChangeGroupMapping
}

// NewAccessCommand lists identities with their grants and the Cognito group mappings, and changes one grant or
// mapping at a time with the role read just before as the expected role.
func NewAccessCommand(deps AccessDependencies) *cobra.Command {
	var yes bool
	command := &cobra.Command{
		Use:   "access",
		Short: "List identities, their grants and the Cognito group mappings (local global ADMIN only)",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			format, err := OutputFormat(command)
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
			if format == "json" {
				return output.WriteJSON(command.OutOrStdout(), listing)
			}
			rows := [][]string{}
			for _, identity := range listing.Identities {
				grants := []string{}
				for _, grant := range identity.Grants {
					scope := grant.ClusterID
					if scope == "" {
						scope = "all"
					}
					grants = append(grants, messages.EscapeTerminalText(scope+":"+grant.Role))
				}
				if len(grants) == 0 {
					grants = append(grants, "none")
				}
				enabled := "yes"
				if !identity.Enabled {
					enabled = "no"
				}
				rows = append(rows, []string{messages.EscapeTerminalText(identity.Label), identity.Source, enabled, strings.Join(grants, " "), messages.EscapeTerminalText(identity.ID)})
			}
			out := command.OutOrStdout()
			fmt.Fprintf(out, "Policy revision %d\n", listing.PolicyRevision)
			if err := output.WriteTable(out, []string{"IDENTITY", "SOURCE", "ENABLED", "GRANTS", "ID"}, rows); err != nil {
				return err
			}
			if len(listing.GroupGrants) == 0 {
				_, err := fmt.Fprintln(out, "\nNo Cognito group mappings.")
				return err
			}
			mappings := [][]string{}
			for _, mapping := range listing.GroupGrants {
				scope := mapping.ClusterID
				if scope == "" {
					scope = "all"
				}
				mappings = append(mappings, []string{messages.EscapeTerminalText(mapping.Issuer), messages.EscapeTerminalText(mapping.Group), messages.EscapeTerminalText(scope), mapping.Role})
			}
			fmt.Fprintln(out)
			if err := output.WriteTable(out, []string{"ISSUER", "GROUP", "SCOPE", "ROLE"}, mappings); err != nil {
				return err
			}
			if listing.GroupGrantsTruncated {
				_, err := fmt.Fprintln(out, "More group mappings exist than the Console listed.")
				return err
			}
			return nil
		},
	}
	grant := &cobra.Command{
		Use:   "grant <identity> <cluster|all> <VIEWER|OPERATOR|ADMIN|none>",
		Short: "Set or remove an identity's role on one cluster or on all clusters",
		Args:  cobra.ExactArgs(3),
		RunE: func(command *cobra.Command, args []string) error {
			scope, err := access.ParseScope(args[1])
			if err != nil {
				return err
			}
			role, err := access.ParseRole(args[2])
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
			identity, err := access.FindIdentity(listing.Identities, args[0])
			if err != nil {
				return err
			}
			expected := ""
			for _, current := range identity.Grants {
				if current.ClusterID == scope {
					expected = current.Role
				}
			}
			needed, err := confirmAccessChange(command, fmt.Sprintf("%s (%s)", identity.Label, identity.Source), scope, expected, role, yes)
			if err != nil || !needed {
				return err
			}
			result, err := deps.Grant.Execute(command.Context(), session, identity.ID, scope, expected, role)
			if err != nil {
				return adminError(err)
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "Changed; policy revision %d (operation %s).\n", result.PolicyRevision, result.OperationID)
			return err
		},
	}
	mapGroup := &cobra.Command{
		Use:   "map-group <issuer> <group> <cluster|all> <VIEWER|OPERATOR|ADMIN|none>",
		Short: "Set or remove a Cognito group's role on one cluster or on all clusters",
		Args:  cobra.ExactArgs(4),
		RunE: func(command *cobra.Command, args []string) error {
			scope, err := access.ParseScope(args[2])
			if err != nil {
				return err
			}
			role, err := access.ParseRole(args[3])
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
			expected := ""
			for _, mapping := range listing.GroupGrants {
				if mapping.Issuer == args[0] && mapping.Group == args[1] && mapping.ClusterID == scope {
					expected = mapping.Role
				}
			}
			needed, err := confirmAccessChange(command, fmt.Sprintf("group %s of %s", args[1], args[0]), scope, expected, role, yes)
			if err != nil || !needed {
				return err
			}
			result, err := deps.Map.Execute(command.Context(), session, args[0], args[1], scope, expected, role)
			if err != nil {
				return adminError(err)
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "Changed; policy revision %d (operation %s).\n", result.PolicyRevision, result.OperationID)
			return err
		},
	}
	for _, change := range []*cobra.Command{grant, mapGroup} {
		change.Flags().BoolVarP(&yes, "yes", "y", false, "do not ask for confirmation")
	}
	command.AddCommand(grant, mapGroup)
	return command
}
