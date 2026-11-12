// Package topics reads topics from every accessible cluster through the Console.
package topics

// Topic is a topic as a Gateway lists it, with the cluster it belongs to.
type Topic struct {
	Cluster           string `json:"cluster"`
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replicationFactor"`
	Internal          bool   `json:"internal"`
	ApproxMessages    int64  `json:"approxMessages"`
}

// ClusterFailure is a cluster that could not answer, with its status and reason as the cluster list shows them.
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
