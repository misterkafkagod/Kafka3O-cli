package clusters

import "testing"

func TestGatewayPath(t *testing.T) {
	if got := GatewayPath("kafka-a", "/api/v1/cluster"); got != "/api/clusters/kafka-a/gateway/api/v1/cluster" {
		t.Fatal(got)
	}
	if got := GatewayPath("dev west/1", "api/v1/topics"); got != "/api/clusters/dev%20west%2F1/gateway/api/v1/topics" {
		t.Fatal(got)
	}
}
