package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/metrics"
)

const metricsDocument = `{"readAt":"2026-11-13T10:00:00Z","logDirs":{"dirs":[{"broker":1,"dir":"/var/lib/kafka/data","totalBytes":100,"usableBytes":60,"kafkaBytes":7}]},"partitions":{"topics":6,"partitions":39,"underReplicated":0,"offline":0,"belowMinIsr":0,"unreadableTopics":[]},"largestTopics":{"topics":[{"topic":"orders","bytes":7}]},"endOffsets":{"topics":[]}}`

func runMetricsCommand(reads *fakeConsoleReads, args ...string) (string, error) {
	var out bytes.Buffer
	root := NewRootCommand(NewMetricsCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads},
		clusters.NewListAccessibleClusters(reads), metrics.NewReadMetrics(reads)))
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs(append([]string{"metrics"}, args...))
	err := root.Execute()
	return out.String(), err
}

func TestMetricsCommandFormats(t *testing.T) {
	reads := &fakeConsoleReads{answers: map[string]string{"/api/clusters": whoamiReads["/api/clusters"], "/api/clusters/kafka-a/gateway/api/v1/metrics": metricsDocument}}
	out, err := runMetricsCommand(reads, "kafka-a")
	if err != nil || !strings.HasPrefix(out, "Metrics for kafka-a, read at 2026-11-13T10:00:00Z") || !strings.Contains(out, "Topics 6, partitions 39") {
		t.Fatalf("text: %v\n%s", err, out)
	}
	out, err = runMetricsCommand(reads, "kafka-a", "-o", "json")
	if err != nil || out != metricsDocument+"\n" {
		t.Fatalf("json changed: %v\n%s", err, out)
	}
	out, err = runMetricsCommand(reads, "kafka-a", "-o", "csv")
	if err != nil || !strings.HasPrefix(out, "metric,broker,logDir,topic,name,value\r\nread_at,,,,,2026-11-13T10:00:00Z\r\nlog_dir_total_bytes,1,/var/lib/kafka/data,,,100\r\n") {
		t.Fatalf("csv: %v\n%q", err, out)
	}
}

func TestMetricsCommandFailures(t *testing.T) {
	reads := &fakeConsoleReads{answers: map[string]string{"/api/clusters": whoamiReads["/api/clusters"]}, errors: map[string]error{
		"/api/clusters/kafka-b/gateway/api/v1/metrics": console.Problem{Status: 503, Code: "gateway_unavailable", Title: "The Gateway could not be reached"}}}
	if _, err := runMetricsCommand(reads, "kafka-b"); ExitCode(err) != ExitClusterFail {
		t.Errorf("unreachable: %v", err)
	}
	if _, err := runMetricsCommand(reads, "nope"); ExitCode(err) != ExitUsage {
		t.Errorf("unknown: %v", err)
	}
	if _, err := runMetricsCommand(reads, "kafka-a", "-o", "yaml"); ExitCode(err) != ExitUsage || !strings.Contains(err.Error(), "table or json or csv") {
		t.Errorf("format: %v", err)
	}
}
