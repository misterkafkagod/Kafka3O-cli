package environments

import (
	"context"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestListRegisteredClusters(t *testing.T) {
	get := &fakeGetJSON{answers: map[string]string{"/api/admin/clusters": `{"operationId":"o","policyRevision":5,"clusters":[
		{"id":"kafka-a","name":"Kafka A","gatewayUrl":"http://gateway-a:8080","environment":"NON_PRODUCTION","active":true},
		{"id":"kafka-b","name":"Kafka B","gatewayUrl":"http://gateway-b:8080","environment":"PRODUCTION","active":true}]}`}}
	listing, err := NewListRegisteredClusters(get).Execute(context.Background(), console.Session{})
	if err != nil || listing.PolicyRevision != 5 || len(listing.Clusters) != 2 || listing.Clusters[1].Environment != "PRODUCTION" || listing.Clusters[0].GatewayURL != "http://gateway-a:8080" {
		t.Fatalf("%+v %v", listing, err)
	}
}
