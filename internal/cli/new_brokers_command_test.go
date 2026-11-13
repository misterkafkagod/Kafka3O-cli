package cli

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

var brokerReads = map[string]string{
	"/api/clusters": whoamiReads["/api/clusters"],
	"/api/clusters/kafka-a/gateway/api/v1/cluster": `{"clusterId":"a","controllerId":1,"brokers":[{"id":1,"host":"kafka-a.svc","port":9092}]}`,
	"/api/clusters/kafka-b/gateway/api/v1/cluster": `{"clusterId":"b","controllerId":1,"brokers":[{"id":1,"host":"kafka-b.svc","port":9092}]}`,
	"/api/clusters/kafka-a/gateway/api/v1/brokers/1/config": `[
		{"name":"log.retention.hours","value":"168","sensitive":false,"readOnly":false,"source":"DEFAULT_CONFIG","type":"INT"},
		{"name":"log.retention.bytes","value":null,"sensitive":false,"readOnly":false,"source":"DEFAULT_CONFIG","type":"LONG"},
		{"name":"ssl.keystore.password","value":"secret-keystore-password","sensitive":true,"readOnly":true,"source":"STATIC_BROKER_CONFIG","type":"PASSWORD"}]`,
}

func runBrokerCommand(reads *fakeConsoleReads, args ...string) (string, string, error) {
	var out, errOut bytes.Buffer
	accessible := clusters.NewListAccessibleClusters(reads)
	deps := SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}
	root := NewRootCommand(NewBrokersCommand(deps, clusters.NewListBrokers(accessible, reads)),
		NewBrokerConfigCommand(deps, accessible, clusters.NewReadBrokerConfig(reads)))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestBrokersTableJSONAndFailure(t *testing.T) {
	out, _, err := runBrokerCommand(&fakeConsoleReads{answers: brokerReads}, "brokers")
	want := "CLUSTER  BROKER  HOST         PORT  ROLE\nkafka-a  1       kafka-a.svc  9092  controller\nkafka-b  1       kafka-b.svc  9092  controller\n"
	if err != nil || out != want {
		t.Fatalf("%v\n%s", err, out)
	}
	out, _, _ = runBrokerCommand(&fakeConsoleReads{answers: brokerReads}, "brokers", "-o", "json")
	var list brokerList
	if json.Unmarshal([]byte(out), &list) != nil || len(list.Brokers) != 2 || !list.Brokers[1].Controller || list.Failures == nil {
		t.Fatalf("json:\n%s", out)
	}
	failing := &fakeConsoleReads{answers: brokerReads, errors: map[string]error{
		"/api/clusters/kafka-b/gateway/api/v1/cluster": console.Problem{Status: 503, Code: "gateway_unavailable", Title: "The Gateway could not be reached"}}}
	out, errOut, err := runBrokerCommand(failing, "brokers")
	if ExitCode(err) != ExitClusterFail || errOut != "kafka-b: Gateway unreachable: The Gateway could not be reached\n" || out != "CLUSTER  BROKER  HOST         PORT  ROLE\nkafka-a  1       kafka-a.svc  9092  controller\n" {
		t.Fatalf("%v %q\n%s", err, errOut, out)
	}
}

func TestBrokerConfig(t *testing.T) {
	out, _, err := runBrokerCommand(&fakeConsoleReads{answers: brokerReads}, "broker-config", "kafka-a", "1", "--filter", "RETENTION")
	want := "NAME                 VALUE      SOURCE          READ-ONLY\nlog.retention.hours  168        default config  no\nlog.retention.bytes  (not set)  default config  no\n"
	if err != nil || out != want {
		t.Fatalf("%v\n%s\nwant\n%s", err, out, want)
	}
	for _, format := range []string{"table", "json"} {
		out, _, _ = runBrokerCommand(&fakeConsoleReads{answers: brokerReads}, "broker-config", "kafka-a", "1", "-o", format)
		if bytes.Contains([]byte(out), []byte("secret-keystore-password")) {
			t.Fatalf("%s printed a sensitive value:\n%s", format, out)
		}
	}
	if _, _, err := runBrokerCommand(&fakeConsoleReads{answers: brokerReads}, "broker-config", "kafka-a", "one"); ExitCode(err) != ExitUsage {
		t.Fatalf("bad id: %v", err)
	}
	failing := &fakeConsoleReads{answers: brokerReads, errors: map[string]error{
		"/api/clusters/kafka-a/gateway/api/v1/brokers/7/config": console.Problem{Status: 503, Code: "cluster_unreachable", Title: "Cluster unreachable", Detail: "describe configs: broker 7 unknown"}}}
	if _, _, err := runBrokerCommand(failing, "broker-config", "kafka-a", "7"); ExitCode(err) != ExitClusterFail || err.Error() != "kafka-a: Cluster unreachable: describe configs: broker 7 unknown: at least one cluster could not answer" {
		t.Fatalf("failing: %v", err)
	}
	if _, _, err := runBrokerCommand(&fakeConsoleReads{answers: brokerReads}, "broker-config", "nope", "1"); ExitCode(err) != ExitUsage {
		t.Fatalf("unknown cluster: %v", err)
	}
}
