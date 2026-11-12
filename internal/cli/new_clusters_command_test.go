package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

var clusterReads = map[string]string{
	"/api/clusters": whoamiReads["/api/clusters"],
	"/api/clusters/kafka-a/gateway/api/v1/cluster": `{"clusterId":"lhGFvWSZTF66EQfx-IW6jg","controllerId":1,"brokers":[{"id":1}]}`,
	"/api/clusters/kafka-b/gateway/api/v1/cluster": `{"clusterId":"DCApYVmJTdSeBtIO3L0ZPw","controllerId":1,"brokers":[{"id":1}]}`,
}

func runClusters(reads *fakeConsoleReads, load func() (console.Session, error), args ...string) (string, error) {
	var out bytes.Buffer
	list := clusters.NewListClusterHealth(clusters.NewListAccessibleClusters(reads), clusters.NewReadClusterHealth(reads))
	root := NewRootCommand(NewClustersCommand(SessionDependencies{LoadSession: load, GetJSON: reads}, list))
	root.SetOut(&out)
	root.SetArgs(append([]string{"clusters"}, args...))
	err := root.Execute()
	return out.String(), err
}

func TestClustersTable(t *testing.T) {
	out, err := runClusters(&fakeConsoleReads{answers: clusterReads}, loaded(signedIn, nil))
	if err != nil {
		t.Fatal(err)
	}
	want := "CLUSTER  NAME     ENVIRONMENT     ROLE   STATUS   BROKERS  CONTROLLER\n" +
		"kafka-a  Kafka A  Non-production  ADMIN  Healthy  1        1\n" +
		"kafka-b  Kafka B  Production      ADMIN  Healthy  1        1\n"
	if out != want {
		t.Fatalf("%q\nwant %q", out, want)
	}
}

func TestClustersJSON(t *testing.T) {
	out, err := runClusters(&fakeConsoleReads{answers: clusterReads}, loaded(signedIn, nil), "-o", "json")
	var got []clusters.ClusterHealth
	if err != nil || json.Unmarshal([]byte(out), &got) != nil || len(got) != 2 || got[1].Environment != "PRODUCTION" ||
		got[0].ClusterID != "lhGFvWSZTF66EQfx-IW6jg" || *got[0].Brokers != 1 || got[1].Status != "healthy" {
		t.Fatalf("%v\n%s", err, out)
	}
	if !strings.Contains(out, `"effectiveRole": "ADMIN"`) {
		t.Fatal("the role is missing from the JSON")
	}
}

func TestClustersNotSignedIn(t *testing.T) {
	if _, err := runClusters(&fakeConsoleReads{}, loaded(console.Session{}, console.ErrSignedOut)); ExitCode(err) != ExitNotSignedIn {
		t.Fatalf("%v", err)
	}
}

func TestClustersWithAFailingGateway(t *testing.T) {
	reads := &fakeConsoleReads{answers: clusterReads, errors: map[string]error{
		"/api/clusters/kafka-b/gateway/api/v1/cluster": console.Problem{Status: 503, Code: "gateway_unavailable", Title: "The Gateway could not be reached"},
	}}
	out, err := runClusters(reads, loaded(signedIn, nil))
	if ExitCode(err) != ExitClusterFail || err.Error() != "1 of 2 clusters: at least one cluster could not answer" {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(out, "kafka-a  Kafka A  Non-production  ADMIN  Healthy") ||
		!strings.Contains(out, "kafka-b  Kafka B  Production      ADMIN  Gateway unreachable: The Gateway could not be reached") {
		t.Fatalf("table:\n%s", out)
	}
	out, err = runClusters(reads, loaded(signedIn, nil), "-o", "json")
	var got []clusters.ClusterHealth
	if ExitCode(err) != ExitClusterFail || json.Unmarshal([]byte(out), &got) != nil || len(got) != 2 || got[1].Status != "gateway_unreachable" ||
		got[1].Error != "The Gateway could not be reached" || *got[0].Brokers != 1 {
		t.Fatalf("json: %v\n%s", err, out)
	}
}
