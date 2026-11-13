package connect

// Connector is one connector's status as the Gateway reports it; its configuration is never part of it.
type Connector struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	State    string `json:"state"`
	WorkerID string `json:"workerId"`
	Error    string `json:"error,omitempty"`
	Tasks    []Task `json:"tasks"`
}

type Task struct {
	ID       int32  `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"workerId"`
	Error    string `json:"error,omitempty"`
}

// ClusterConnectors is one cluster's connectors. NotConfigured means the cluster has no Kafka Connect.
type ClusterConnectors struct {
	Cluster       string      `json:"cluster"`
	NotConfigured bool        `json:"notConfigured"`
	Connectors    []Connector `json:"connectors"`
}
