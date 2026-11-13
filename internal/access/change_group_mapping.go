package access

import (
	"context"
	"fmt"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ChangeGroupMapping sets (or with role "" removes) a Cognito group's role on a scope, if it is still expectedRole.
type ChangeGroupMapping interface {
	Execute(ctx context.Context, session console.Session, issuer, group, clusterID, expectedRole, role string) (ChangeResult, error)
}

type changeGroupMapping struct {
	send console.SendJSON
}

func NewChangeGroupMapping(send console.SendJSON) ChangeGroupMapping {
	return changeGroupMapping{send: send}
}

func (c changeGroupMapping) Execute(ctx context.Context, session console.Session, issuer, group, clusterID, expectedRole, role string) (ChangeResult, error) {
	body := struct {
		Issuer       string `json:"issuer"`
		Group        string `json:"group"`
		ClusterID    string `json:"clusterId"`
		ExpectedRole string `json:"expectedRole"`
		Role         string `json:"role"`
	}{issuer, group, clusterID, expectedRole, role}
	var result ChangeResult
	if err := c.send.Execute(ctx, session, http.MethodPut, "/api/admin/group-mappings", body, "", &result); err != nil {
		return ChangeResult{}, err
	}
	if result.OperationID == "" || result.PolicyRevision == 0 {
		return ChangeResult{}, fmt.Errorf("%w: the Console did not confirm the group mapping change", console.ErrOutcomeUnknown)
	}
	return result, nil
}
