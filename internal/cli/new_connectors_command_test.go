package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/connect"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

const connectorsPathA = "/api/clusters/kafka-a/gateway/api/v1/connect/connectors"

func connectorsReads() *fakeConsoleReads {
	return &fakeConsoleReads{
		answers: map[string]string{
			"/api/clusters": whoamiReads["/api/clusters"],
			connectorsPathA: `[
				{"name":"kafka3o-verify-running","type":"sink","state":"RUNNING","workerId":"connect:8083","tasks":[{"id":1,"state":"FAILED","workerId":"connect:8083","error":"java.lang.RuntimeException: \u001b[31mboom"},{"id":0,"state":"RUNNING","workerId":"connect:8083"}]},
				{"name":"kafka3o-verify-paused","type":"source","state":"PAUSED","workerId":"connect:8083","tasks":[]},
				{"name":"kafka3o-verify-failed","type":"sink","state":"FAILED","workerId":"connect:8083","error":"org.apache.kafka.connect.errors.ConnectException: no topic","tasks":[]}]`,
		},
		errors: map[string]error{"/api/clusters/kafka-b/gateway/api/v1/connect/connectors": console.Problem{Status: 404, Code: "connect_not_configured", Detail: "Kafka Connect is not configured for this cluster"}},
	}
}

func runConnectorsCommand(reads *fakeConsoleReads, args ...string) (string, string, error) {
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewConnectorsCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads},
		connect.NewListConnectors(clusters.NewListAccessibleClusters(reads), reads)))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"connectors"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestConnectorsCommand(t *testing.T) {
	out, _, err := runConnectorsCommand(connectorsReads())
	want := "CLUSTER  CONNECTOR               TYPE    STATE    WORKER        TASKS\n" +
		"kafka-a  kafka3o-verify-failed   sink    FAILED   connect:8083  none\n" +
		"kafka-a  kafka3o-verify-paused   source  PAUSED   connect:8083  none\n" +
		"kafka-a  kafka3o-verify-running  sink    RUNNING  connect:8083  0:RUNNING 1:FAILED\n" +
		"\nErrors\n" +
		"kafka-a kafka3o-verify-failed: org.apache.kafka.connect.errors.ConnectException: no topic\n" +
		"kafka-a kafka3o-verify-running task 1: java.lang.RuntimeException: \\x1b[31mboom\n" +
		"\nkafka-b: Kafka Connect is not configured for this cluster.\n"
	if err != nil || out != want {
		t.Fatalf("%v\n%s", err, out)
	}
}

func TestConnectorsCommandJSONAndFailures(t *testing.T) {
	out, _, err := runConnectorsCommand(connectorsReads(), "-o", "json")
	var list struct {
		Clusters []connect.ClusterConnectors `json:"clusters"`
		Failures []clusters.ClusterFailure   `json:"failures"`
	}
	if err != nil || json.Unmarshal([]byte(out), &list) != nil || len(list.Clusters) != 2 || len(list.Clusters[0].Connectors) != 3 || !list.Clusters[1].NotConfigured ||
		list.Failures == nil || !strings.Contains(out, `"notConfigured": true`) {
		t.Fatalf("%v\n%s", err, out)
	}
	failing := connectorsReads()
	failing.errors[connectorsPathA] = console.Problem{Status: 503, Code: "connect_unreachable", Title: "Kafka Connect unreachable", Detail: "Kafka Connect is unreachable"}
	out, errOut, err := runConnectorsCommand(failing)
	if ExitCode(err) != ExitClusterFail || errOut != "kafka-a: Status unavailable: Kafka Connect is unreachable (503 connect_unreachable)\n" ||
		out != "No connectors.\n\nkafka-b: Kafka Connect is not configured for this cluster.\n" {
		t.Fatalf("%v\n%q\n%q", err, out, errOut)
	}
}
