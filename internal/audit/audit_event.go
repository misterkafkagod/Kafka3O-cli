package audit

// Event is one audit record as the Console reports it.
type Event struct {
	Sequence       int64  `json:"sequence"`
	OccurredAt     string `json:"occurredAt"`
	OperationID    string `json:"operationId"`
	ActorID        string `json:"actorId"`
	Source         string `json:"source"`
	ClusterID      string `json:"clusterId"`
	Environment    string `json:"environment"`
	Action         string `json:"action"`
	ResourceKind   string `json:"resourceKind"`
	ResourceID     string `json:"resourceId"`
	Decision       string `json:"decision"`
	Outcome        string `json:"outcome"`
	Reason         string `json:"reason"`
	BeforeState    string `json:"beforeState"`
	AfterState     string `json:"afterState"`
	PolicyRevision int64  `json:"policyRevision"`
}

// Page is one page of audit events; NextCursor is the --after value for the next page, empty on the last.
type Page struct {
	OperationID string  `json:"operationId"`
	Events      []Event `json:"events"`
	NextCursor  string  `json:"nextCursor"`
}

// Filters are the audit filters as typed; empty fields are left out. An empty Cluster is the all-cluster scope.
type Filters struct {
	Cluster, Actor, Action, Outcome, From, Until, After string
	Limit                                               int
}

// MaxLimit is the Console's largest audit page.
const MaxLimit = 500
