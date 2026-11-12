package cli

import "github.com/misterkafkagod/Kafka3O-cli/internal/clusters"

// describeClusterStatus is the status text the UI's cluster card shows, with the reason when there is one.
func describeClusterStatus(cluster clusters.ClusterHealth) string {
	text := map[string]string{
		clusters.StatusHealthy:                  "Healthy",
		clusters.StatusGatewayUnreachable:       "Gateway unreachable",
		clusters.StatusClusterUnreachable:       "Cluster unreachable",
		clusters.StatusAccessDenied:             "Access denied",
		clusters.StatusAuthorizationUnavailable: "Authorization unavailable",
		clusters.StatusUnavailable:              "Status unavailable",
	}[cluster.Status]
	if text == "" {
		text = cluster.Status
	}
	if cluster.Error != "" {
		return text + ": " + cluster.Error
	}
	return text
}
