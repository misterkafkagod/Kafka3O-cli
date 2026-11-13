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
