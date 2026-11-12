// Package clusters reads the clusters the signed-in user may access and their health.
package clusters

// AccessibleCluster is a cluster as GET /api/clusters lists it for the signed-in user.
type AccessibleCluster struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Environment   string `json:"environment"`
	EffectiveRole string `json:"effectiveRole"`
}
