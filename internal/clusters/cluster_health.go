package clusters

// Statuses a cluster can have, as the UI's cluster cards show them.
const (
	StatusHealthy                  = "healthy"
	StatusGatewayUnreachable       = "gateway_unreachable"
	StatusClusterUnreachable       = "cluster_unreachable"
	StatusAccessDenied             = "access_denied"
	StatusAuthorizationUnavailable = "authorization_unavailable"
	StatusUnavailable              = "unavailable"
)

// ClusterHealth is one accessible cluster with what its Gateway reports; a cluster that could not answer keeps its
// status and Error instead of values.
type ClusterHealth struct {
	AccessibleCluster
	Status       string `json:"status"`
	Error        string `json:"error,omitempty"`
	ClusterID    string `json:"clusterId,omitempty"`
	ControllerID *int32 `json:"controllerId,omitempty"`
	Brokers      *int   `json:"brokers,omitempty"`
}
