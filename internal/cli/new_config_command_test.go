package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

var configReads = map[string]string{
	"/api/clusters": whoamiReads["/api/clusters"],
	"/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-cli-p113/config": `[{"name":"retention.ms","value":"604800000","readOnly":false,"source":"DEFAULT_CONFIG"},{"name":"secret.thing","value":null,"sensitive":true}]`,
	"/api/clusters/kafka-a/gateway/api/v1/brokers/1/config":               `[{"name":"log.retention.hours","value":"168","readOnly":true},{"name":"log.cleaner.threads","value":"1","readOnly":false}]`,
	"/api/clusters/kafka-b/gateway/api/v1/topics/orders/config":           `[{"name":"retention.ms","value":"604800000"}]`,
}

func runConfig(writes *fakeConsoleWrites, stdin string, args ...string) (string, string, error) {
	reads := &fakeConsoleReads{answers: configReads}
	deps := ConfigurationDependencies{Session: SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, Accessible: clusters.NewListAccessibleClusters(reads),
		Read: clusters.NewReadConfiguration(reads), Change: clusters.NewChangeConfiguration(writes)}
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewConfigCommand(deps))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(append([]string{"config"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestConfigSetAndReset(t *testing.T) {
	topicPath := "/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-cli-p113/config"
	writes := &fakeConsoleWrites{answers: map[string]string{topicPath: `{"name":"retention.ms","status":"accepted"}`}}
	out, prompt, err := runConfig(writes, "y\n", "set", "kafka-a", "topic", "kafka3o-cli-p113", "retention.ms", "3600000")
	if err != nil || out != "retention.ms of topic kafka3o-cli-p113 is now 3600000.\n" || !strings.Contains(prompt, "topic kafka3o-cli-p113: retention.ms: 604800000 → 3600000") ||
		writes.sent[0].Body != `{"name":"retention.ms","operation":"set","value":"3600000"}` {
		t.Fatalf("%v %q %q %+v", err, out, prompt, writes.sent)
	}
	out, prompt, err = runConfig(writes, "", "reset", "kafka-a", "topic", "kafka3o-cli-p113", "retention.ms", "--yes")
	if err != nil || out != "retention.ms of topic kafka3o-cli-p113 is now the default.\n" || !strings.Contains(prompt, "604800000 → the default") ||
		writes.sent[1].Body != `{"name":"retention.ms","operation":"reset","value":null}` {
		t.Fatalf("reset: %v %q %q %+v", err, out, prompt, writes.sent)
	}
	brokerPath := "/api/clusters/kafka-a/gateway/api/v1/brokers/1/config"
	broker := &fakeConsoleWrites{answers: map[string]string{brokerPath: `{"name":"log.cleaner.threads","status":"accepted"}`}}
	if _, _, err := runConfig(broker, "", "set", "kafka-a", "broker", "1", "log.cleaner.threads", "2", "--yes"); err != nil || broker.sent[0].Path != brokerPath {
		t.Fatalf("broker: %v %+v", err, broker.sent)
	}
}

func TestConfigRefusals(t *testing.T) {
	writes := &fakeConsoleWrites{}
	for name, args := range map[string][]string{
		"read-only":  {"set", "kafka-a", "broker", "1", "log.retention.hours", "1", "--yes"},
		"sensitive":  {"set", "kafka-a", "topic", "kafka3o-cli-p113", "secret.thing", "x", "--yes"},
		"unknown":    {"set", "kafka-a", "topic", "kafka3o-cli-p113", "nope.setting", "x", "--yes"},
		"kind":       {"set", "kafka-a", "group", "g", "x", "y", "--yes"},
		"broker id":  {"reset", "kafka-a", "broker", "one", "x", "--yes"},
		"production": {"set", "kafka-b", "topic", "orders", "retention.ms", "1", "--yes"},
	} {
		if _, _, err := runConfig(writes, "", args...); err == nil || ExitCode(err) != ExitUsage {
			t.Errorf("%s: %v", name, err)
		}
	}
	if len(writes.sent) != 0 {
		t.Fatalf("a refused change was sent: %+v", writes.sent)
	}
	rejected := &fakeConsoleWrites{errors: map[string]error{"/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-cli-p113/config": console.Problem{Status: 422, Code: "configuration_rejected",
		Detail: "Kafka rejected this configuration change. Check the value and Kafka constraints; no change was submitted."}}}
	if _, _, err := runConfig(rejected, "", "set", "--yes", "kafka-a", "topic", "kafka3o-cli-p113", "retention.ms", "--", "-5"); err == nil || !strings.HasPrefix(err.Error(), "refused: Kafka rejected") {
		t.Fatalf("rejected: %v", err)
	}
}
