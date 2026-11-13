package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/environments"
)

var registeredReads = map[string]string{"/api/admin/clusters": `{"operationId":"o","policyRevision":5,"clusters":[
	{"id":"kafka-a","name":"Kafka A","gatewayUrl":"http://gateway-a:8080","environment":"NON_PRODUCTION","active":true},
	{"id":"kafka-b","name":"Kafka B","gatewayUrl":"http://gateway-b:8080","environment":"PRODUCTION","active":true},
	{"id":"kafka-c","name":"Kafka C","gatewayUrl":"http://gateway-c:8080","environment":"","active":false}]}`}

func runEnvironmentsCommand(reads *fakeConsoleReads, args ...string) (string, error) {
	var out bytes.Buffer
	root := NewRootCommand(NewEnvironmentsCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, environments.NewListRegisteredClusters(reads)))
	root.SetOut(&out)
	root.SetArgs(append([]string{"environments"}, args...))
	err := root.Execute()
	return out.String(), err
}

func TestEnvironmentsCommand(t *testing.T) {
	out, err := runEnvironmentsCommand(&fakeConsoleReads{answers: registeredReads})
	want := "CLUSTER  NAME     ENVIRONMENT     ACTIVE  GATEWAY\n" +
		"kafka-a  Kafka A  Non-production  yes     http://gateway-a:8080\n" +
		"kafka-b  Kafka B  Production      yes     http://gateway-b:8080\n" +
		"kafka-c  Kafka C  Unclassified    no      http://gateway-c:8080\n"
	if err != nil || out != want {
		t.Fatalf("%v\n%s", err, out)
	}
	if out, err := runEnvironmentsCommand(&fakeConsoleReads{answers: registeredReads}, "-o", "json"); err != nil || !strings.Contains(out, `"gatewayUrl": "http://gateway-b:8080"`) {
		t.Fatalf("json: %v\n%s", err, out)
	}
	denied := &fakeConsoleReads{errors: map[string]error{"/api/admin/clusters": console.Problem{Status: 403, Code: "global_admin_required"}}}
	if _, err := runEnvironmentsCommand(denied); err == nil || !strings.HasPrefix(err.Error(), "denied: Only a local global administrator") {
		t.Fatalf("denied: %v", err)
	}
}
