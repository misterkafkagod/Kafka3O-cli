package access

import (
	"context"
	"fmt"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ChangeRoleGrant sets (or with role "" removes) an identity's role on a scope, if it is still expectedRole.
type ChangeRoleGrant interface {
	Execute(ctx context.Context, session console.Session, identityID, clusterID, expectedRole, role string) (ChangeResult, error)
}

type changeRoleGrant struct {
	send console.SendJSON
}

func NewChangeRoleGrant(send console.SendJSON) ChangeRoleGrant {
	return changeRoleGrant{send: send}
}

func (c changeRoleGrant) Execute(ctx context.Context, session console.Session, identityID, clusterID, expectedRole, role string) (ChangeResult, error) {
	body := struct {
		IdentityID   string `json:"identityId"`
		ClusterID    string `json:"clusterId"`
		ExpectedRole string `json:"expectedRole"`
		Role         string `json:"role"`
	}{identityID, clusterID, expectedRole, role}
	var result ChangeResult
	if err := c.send.Execute(ctx, session, http.MethodPut, "/api/admin/role-grants", body, "", &result); err != nil {
		return ChangeResult{}, err
	}
	if result.OperationID == "" || result.PolicyRevision == 0 {
		return ChangeResult{}, fmt.Errorf("%w: the Console did not confirm the role change", console.ErrOutcomeUnknown)
	}
	return result, nil
}
