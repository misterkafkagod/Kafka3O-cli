package access

import "fmt"

// Grant is one role binding; an empty ClusterID is the global scope.
type Grant struct {
	ClusterID string `json:"clusterId"`
	Role      string `json:"role"`
}

// Identity is a local or Cognito identity with its direct grants.
type Identity struct {
	ID      string  `json:"id"`
	Source  string  `json:"source"`
	Label   string  `json:"label"`
	Enabled bool    `json:"enabled"`
	Grants  []Grant `json:"grants"`
}

// GroupGrant maps a Cognito group to a role on a scope.
type GroupGrant struct {
	Issuer    string `json:"issuer"`
	Group     string `json:"group"`
	ClusterID string `json:"clusterId"`
	Role      string `json:"role"`
}

// Listing is the whole access policy as the Console reports it.
type Listing struct {
	PolicyRevision       int64        `json:"policyRevision"`
	Identities           []Identity   `json:"identities"`
	GroupGrants          []GroupGrant `json:"groupGrants"`
	GroupGrantsTruncated bool         `json:"groupGrantsTruncated"`
}

// ChangeResult is the Console's answer to an access change.
type ChangeResult struct {
	OperationID    string `json:"operationId"`
	PolicyRevision int64  `json:"policyRevision"`
}

// ErrIdentityNotFound means no identity has this id or label, or the label is ambiguous.
type ErrIdentityNotFound struct {
	Identity  string
	Ambiguous bool
}

func (e ErrIdentityNotFound) Error() string {
	if e.Ambiguous {
		return fmt.Sprintf("more than one identity is labeled %s; use its id from `kafka3o access`", e.Identity)
	}
	return fmt.Sprintf("no identity has the id or label %s", e.Identity)
}
