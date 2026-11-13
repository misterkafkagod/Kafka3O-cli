package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
)

func runSearchCommand(reads *fakeConsoleReads, args ...string) (string, error) {
	var out bytes.Buffer
	root := NewRootCommand(NewSearchCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads},
		clusters.NewListAccessibleClusters(reads), messages.NewSearchMessages(reads)))
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs(append([]string{"search"}, args...))
	err := root.Execute()
	return out.String(), err
}

const searchPath = "/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-m10-verify/messages/search?query="

func TestSearchCommand(t *testing.T) {
	reads := &fakeConsoleReads{answers: map[string]string{
		"/api/clusters":          whoamiReads["/api/clusters"],
		searchPath + "check+two": `{"messages":[{"partition":0,"offset":61,"offsetExact":"61","timestamp":"2026-11-12T04:08:52.928Z","key":"ci1i","value":"Y2hlY2sgdHdv","headers":[]}],"scanned":141,"truncated":false}`,
		searchPath + "nothing":   `{"messages":[],"scanned":141,"truncated":false}`,
		searchPath + "old":       `{"messages":[],"scanned":500,"truncated":true}`,
	}}
	out, err := runSearchCommand(reads, "kafka-a", "kafka3o-m10-verify", "check two")
	if err != nil || out != "partition 0  offset 61  2026-11-12T04:08:52.928Z\nkey: r-b\nvalue:\n  check two\n\nScanned 141 messages.\n" {
		t.Fatalf("%v\n%q", err, out)
	}
	out, _ = runSearchCommand(reads, "kafka-a", "kafka3o-m10-verify", "nothing")
	if out != "No messages contain nothing.\nScanned 141 messages.\n" {
		t.Fatalf("%q", out)
	}
	out, _ = runSearchCommand(reads, "kafka-a", "kafka3o-m10-verify", "old")
	if !strings.HasSuffix(out, "Scanned 500 messages.\nOnly the most recent messages were searched; older ones were not.\n") {
		t.Fatalf("%q", out)
	}
	out, _ = runSearchCommand(reads, "kafka-a", "kafka3o-m10-verify", "check two", "-o", "json")
	var result messages.SearchResult
	if json.Unmarshal([]byte(out), &result) != nil || result.Scanned != 141 || len(result.Messages) != 1 {
		t.Fatalf("json:\n%s", out)
	}
}

func TestSearchCommandRefusals(t *testing.T) {
	reads := &fakeConsoleReads{answers: map[string]string{"/api/clusters": whoamiReads["/api/clusters"]}, errors: map[string]error{
		"/api/clusters/kafka-a/gateway/api/v1/topics/nope/messages/search?query=x": console.Problem{Status: 404, Code: "not_found"},
		"/api/clusters/kafka-b/gateway/api/v1/topics/t/messages/search?query=x":    console.Problem{Status: 503, Code: "gateway_unavailable", Title: "The Gateway could not be reached"},
	}}
	if _, err := runSearchCommand(reads, "kafka-a", "t", ""); ExitCode(err) != ExitUsage {
		t.Errorf("empty: %v", err)
	}
	if _, err := runSearchCommand(reads, "kafka-a", "nope", "x"); ExitCode(err) != ExitUsage || !strings.Contains(err.Error(), "not found") {
		t.Errorf("missing topic: %v", err)
	}
	if _, err := runSearchCommand(reads, "kafka-b", "t", "x"); ExitCode(err) != ExitClusterFail {
		t.Errorf("unreachable: %v", err)
	}
}
