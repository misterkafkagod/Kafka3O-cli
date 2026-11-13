package clusters

import (
	"context"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestReadBrokerConfigDropsSensitiveValues(t *testing.T) {
	get := &fakeGetJSON{answers: map[string]string{"/api/clusters/kafka-a/gateway/api/v1/brokers/1/config": `[
		{"name":"log.retention.hours","value":"168","sensitive":false,"readOnly":false,"source":"DEFAULT_CONFIG","type":"INT"},
		{"name":"ssl.keystore.password","value":"leaked?","sensitive":true,"readOnly":true,"source":"STATIC_BROKER_CONFIG","type":"PASSWORD"}]`}}
	settings, err := NewReadBrokerConfig(get).Execute(context.Background(), console.Session{}, "kafka-a", 1)
	if err != nil || len(settings) != 2 || *settings[0].Value != "168" || settings[1].Value != nil {
		t.Fatalf("%+v %v", settings, err)
	}
}
