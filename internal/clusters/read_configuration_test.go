package clusters

import (
	"context"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestReadConfiguration(t *testing.T) {
	get := &fakeGetJSON{answers: map[string]string{
		"/api/clusters/kafka-a/gateway/api/v1/topics/t/config":  `[{"name":"retention.ms","value":"604800000","sensitive":false,"readOnly":false,"source":"DEFAULT_CONFIG","type":"LONG"},{"name":"secret","value":"x","sensitive":true,"readOnly":true}]`,
		"/api/clusters/kafka-a/gateway/api/v1/brokers/1/config": `[{"name":"log.retention.hours","value":"168","readOnly":true}]`,
	}}
	settings, err := NewReadConfiguration(get).Execute(context.Background(), console.Session{}, "kafka-a", ConfigurationResource{Kind: "topic", Name: "t"})
	if err != nil || len(settings) != 2 || *settings[0].Value != "604800000" || settings[1].Value != nil {
		t.Fatalf("%+v %v", settings, err)
	}
	if settings, err := NewReadConfiguration(get).Execute(context.Background(), console.Session{}, "kafka-a", ConfigurationResource{Kind: "broker", Name: "1"}); err != nil || !settings[0].ReadOnly {
		t.Fatalf("%+v %v", settings, err)
	}
}
