package topics

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

var describeReads = map[string]string{
	"/api/clusters": topicReads["/api/clusters"],
	"/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-m10-verify": `{"name":"kafka3o-m10-verify","partitions":[
		{"id":0,"leader":1,"replicas":[1],"isr":[1],"startOffset":0,"endOffset":67},
		{"id":1,"leader":1,"replicas":[1],"isr":[1],"startOffset":0,"endOffset":3}],
		"config":[{"name":"cleanup.policy","value":"delete","sensitive":false},{"name":"sasl.jaas.config","value":"","sensitive":true}]}`,
	"/api/clusters/kafka-a/gateway/api/v1/consumer-groups": `[
		{"id":"connect-kafka3o-backup-kafka3o-m10-verify","protocol":"classic","state":"Stable","members":[{"id":"m"}],"topics":["kafka3o-m10-verify"],
		 "partitions":[{"topic":"kafka3o-m10-verify","partition":0,"committedOffset":"67","startOffset":"0","endOffset":"67","lag":"0","status":"ok"}]},
		{"id":"elsewhere","protocol":"classic","state":"Empty","members":[],"topics":["tert"],"partitions":[]}]`,
	"/api/clusters/kafka-a/gateway/api/v1/backups": `{"backups":[{"topic":"kafka3o-m10-verify","connector":"kafka3o-backup-kafka3o-m10-verify","location":"s3://kafka3o-backups/kafka-a/kafka3o-m10-verify/",
		"state":"RUNNING","storage":{"objects":12,"storedBytes":2400,"lastWrite":"2026-11-12T04:10:00Z"},"settings":{"startFrom":"earliest","compression":"gzip","recordsPerFile":0}},
		{"topic":"tert","connector":"kafka3o-backup-tert","location":"s3://x/","state":"RUNNING"}]}`,
}

func describe(get *fakeGetJSON) DescribeTopic {
	return NewDescribeTopic(clusters.NewListAccessibleClusters(get), get)
}

func TestDescribeTopicReadsEverySection(t *testing.T) {
	view, err := describe(&fakeGetJSON{answers: describeReads}).Execute(context.Background(), console.Session{}, "kafka-a", "kafka3o-m10-verify")
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Partitions) != 2 || view.Partitions[0].EndOffset != 67 || len(view.Config) != 2 || !view.Config[1].Sensitive {
		t.Fatalf("detail %+v", view)
	}
	if len(view.Groups.Groups) != 1 || *view.Groups.Groups[0].Partitions[0].Lag != "0" || view.Groups.Error != "" {
		t.Fatalf("groups %+v", view.Groups)
	}
	if view.Backup.Backup == nil || view.Backup.Backup.State != "RUNNING" || view.Backup.Backup.Settings.Compression != "gzip" || view.Backup.Backup.Storage.Objects != 12 {
		t.Fatalf("backup %+v", view.Backup)
	}
}

func TestDescribeTopicKeepsSectionErrors(t *testing.T) {
	get := &fakeGetJSON{answers: describeReads, errors: map[string]error{
		"/api/clusters/kafka-a/gateway/api/v1/consumer-groups": console.Problem{Status: 503, Code: "cluster_unreachable", Title: "Cluster unreachable", Detail: "list groups: timeout"},
		"/api/clusters/kafka-a/gateway/api/v1/backups":         console.Problem{Status: 404, Code: "backups_not_configured", Title: "Backups not configured"},
	}}
	view, err := describe(get).Execute(context.Background(), console.Session{}, "kafka-a", "kafka3o-m10-verify")
	if err != nil || view.Groups.Error != "list groups: timeout" || view.Groups.Groups == nil || !view.Backup.NotConfigured || view.Backup.Backup != nil {
		t.Fatalf("%+v %v", view, err)
	}
	get.errors["/api/clusters/kafka-a/gateway/api/v1/backups"] = console.Problem{Status: 503, Code: "connect_unreachable", Title: "Kafka Connect is unreachable"}
	view, _ = describe(get).Execute(context.Background(), console.Session{}, "kafka-a", "kafka3o-m10-verify")
	if view.Backup.Error != "Kafka Connect is unreachable" || view.Backup.NotConfigured {
		t.Fatalf("backup error %+v", view.Backup)
	}
}

func TestDescribeTopicFailures(t *testing.T) {
	get := &fakeGetJSON{answers: describeReads, errors: map[string]error{
		"/api/clusters/kafka-a/gateway/api/v1/topics/nope": console.Problem{Status: 404, Code: "not_found", Title: "Not Found", Detail: `Topic "nope" was not found`},
		"/api/clusters/kafka-b/gateway/api/v1/topics/x":    console.Problem{Status: 503, Code: "gateway_unavailable", Title: "The Gateway could not be reached"},
	}}
	var notFound ErrTopicNotFound
	if _, err := describe(get).Execute(context.Background(), console.Session{}, "kafka-a", "nope"); !errors.As(err, &notFound) || err.Error() != "topic nope was not found on kafka-a" {
		t.Errorf("not found: %v", err)
	}
	var unknown ErrUnknownCluster
	if _, err := describe(get).Execute(context.Background(), console.Session{}, "nope", "x"); !errors.As(err, &unknown) {
		t.Errorf("unknown cluster: %v", err)
	}
	var clusterRead ErrClusterRead
	if _, err := describe(get).Execute(context.Background(), console.Session{}, "kafka-b", "x"); !errors.As(err, &clusterRead) || clusterRead.Status != "gateway_unreachable" {
		t.Errorf("unreachable: %v", err)
	}
	get.errors["/api/clusters/kafka-a/gateway/api/v1/backups"] = console.ErrSignedOut
	if _, err := describe(get).Execute(context.Background(), console.Session{}, "kafka-a", "kafka3o-m10-verify"); !errors.Is(err, console.ErrSignedOut) {
		t.Errorf("signed out: %v", err)
	}
}

func TestDescribeTopicEscapesTheName(t *testing.T) {
	get := &fakeGetJSON{answers: map[string]string{
		"/api/clusters": topicReads["/api/clusters"],
		"/api/clusters/kafka-a/gateway/api/v1/topics/a%2Fb":    `{"partitions":[],"config":[]}`,
		"/api/clusters/kafka-a/gateway/api/v1/consumer-groups": `[]`,
		"/api/clusters/kafka-a/gateway/api/v1/backups":         `{"backups":[]}`,
	}}
	if view, err := describe(get).Execute(context.Background(), console.Session{}, "kafka-a", "a/b"); err != nil || view.Backup.Backup != nil {
		t.Fatalf("%+v %v", view, err)
	}
}
