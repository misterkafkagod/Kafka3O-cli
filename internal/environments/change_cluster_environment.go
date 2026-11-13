package environments

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ChangeClusterEnvironment reclassifies a cluster, if it is still classified as expected.
type ChangeClusterEnvironment interface {
	Execute(ctx context.Context, session console.Session, cluster, expected, environment, reason string) (ChangeResult, error)
}

type changeClusterEnvironment struct {
	send console.SendJSON
}

func NewChangeClusterEnvironment(send console.SendJSON) ChangeClusterEnvironment {
	return changeClusterEnvironment{send: send}
}

func (c changeClusterEnvironment) Execute(ctx context.Context, session console.Session, cluster, expected, environment, reason string) (ChangeResult, error) {
	body := struct {
		ExpectedEnvironment string `json:"expectedEnvironment"`
		Environment         string `json:"environment"`
		Reason              string `json:"reason"`
	}{expected, environment, reason}
	var result ChangeResult
	if err := c.send.Execute(ctx, session, http.MethodPut, "/api/admin/clusters/"+url.PathEscape(cluster)+"/environment", body, "", &result); err != nil {
		return ChangeResult{}, err
	}
	if result.OperationID == "" || result.PolicyRevision == 0 {
		return ChangeResult{}, fmt.Errorf("%w: the Console did not confirm the classification of %s", console.ErrOutcomeUnknown, cluster)
	}
	return result, nil
}
