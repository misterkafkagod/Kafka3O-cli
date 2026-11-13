package environments

// RegisteredCluster is a cluster the Console knows, with its classification.
type RegisteredCluster struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	GatewayURL  string `json:"gatewayUrl"`
	Environment string `json:"environment"`
	Active      bool   `json:"active"`
}

// Listing is the Console's registered clusters at a policy revision.
type Listing struct {
	PolicyRevision int64               `json:"policyRevision"`
	Clusters       []RegisteredCluster `json:"clusters"`
}

// ChangeResult is the Console's answer to a reclassification.
type ChangeResult struct {
	OperationID    string `json:"operationId"`
	PolicyRevision int64  `json:"policyRevision"`
}
