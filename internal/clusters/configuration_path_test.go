package clusters

import "testing"

func TestConfigurationPath(t *testing.T) {
	if got := ConfigurationPath(ConfigurationResource{Kind: "topic", Name: "a b"}); got != "/api/v1/topics/a%20b/config" {
		t.Fatal(got)
	}
	if got := ConfigurationPath(ConfigurationResource{Kind: "broker", Name: "1"}); got != "/api/v1/brokers/1/config" {
		t.Fatal(got)
	}
}
