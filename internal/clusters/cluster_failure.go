package clusters

// ClusterFailure is a cluster that could not answer a read, with its status and reason as the cluster list shows them.
type ClusterFailure struct {
	Cluster string `json:"cluster"`
	Status  string `json:"status"`
	Error   string `json:"error"`
}

// ErrUnknownCluster means the requested cluster does not exist or the user may not access it; the Console does
// not tell them apart.
type ErrUnknownCluster struct {
	Cluster string
}

func (e ErrUnknownCluster) Error() string {
	return "cluster " + e.Cluster + " not found or not accessible"
}
