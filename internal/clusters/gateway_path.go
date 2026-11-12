package clusters

import (
	"net/url"
	"strings"
)

// GatewayPath is the Console path that forwards a Gateway API path to the cluster's Gateway.
func GatewayPath(clusterID, gatewayPath string) string {
	return "/api/clusters/" + url.PathEscape(clusterID) + "/gateway/" + strings.TrimLeft(gatewayPath, "/")
}
