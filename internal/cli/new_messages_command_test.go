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

var messageReads = map[string]string{
	"/api/clusters": whoamiReads["/api/clusters"],
	"/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-m10-verify/messages":                       `[{"partition":0,"offset":61,"offsetExact":"61","timestamp":"2026-11-12T04:08:52.928Z","key":"ci1i","value":"eyJjaGVjayI6Mn0=","headers":[]}]`,
	"/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-m10-verify/messages?offset=61&partition=0": `[]`,
}

func runMessagesCommand(reads *fakeConsoleReads, args ...string) (string, error) {
	var out bytes.Buffer
	root := NewRootCommand(NewMessagesCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads},
		clusters.NewListAccessibleClusters(reads), messages.NewReadMessages(reads)))
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs(append([]string{"messages"}, args...))
	err := root.Execute()
	return out.String(), err
}

func TestMessagesCommand(t *testing.T) {
	reads := &fakeConsoleReads{answers: messageReads}
	out, err := runMessagesCommand(reads, "kafka-a", "kafka3o-m10-verify", "--value", "json")
	if err != nil || out != "partition 0  offset 61  2026-11-12T04:08:52.928Z\nkey: r-b\nvalue:\n  {\n    \"check\": 2\n  }\n" {
		t.Fatalf("%v\n%q", err, out)
	}
	out, err = runMessagesCommand(reads, "kafka-a", "kafka3o-m10-verify", "--partition", "0", "--offset", "61")
	if err != nil || out != "No messages.\n" {
		t.Fatalf("position: %v %q", err, out)
	}
	out, _ = runMessagesCommand(reads, "kafka-a", "kafka3o-m10-verify", "-o", "json")
	var got []messages.Message
	if json.Unmarshal([]byte(out), &got) != nil || len(got) != 1 || string(got[0].Value) != `{"check":2}` {
		t.Fatalf("json:\n%s", out)
	}
}

func TestMessagesCommandRefusals(t *testing.T) {
	reads := &fakeConsoleReads{answers: messageReads, errors: map[string]error{
		"/api/clusters/kafka-a/gateway/api/v1/topics/nope/messages": console.Problem{Status: 404, Code: "not_found"},
		"/api/clusters/kafka-b/gateway/api/v1/topics/t/messages":    console.Problem{Status: 503, Code: "cluster_unreachable", Title: "Cluster unreachable", Detail: "timeout"},
	}}
	for name, tt := range map[string]struct {
		args []string
		exit int
		text string
	}{
		"offset alone":  {[]string{"kafka-a", "t", "--offset", "5"}, ExitUsage, "give --partition with either --offset or --timestamp"},
		"bad format":    {[]string{"kafka-a", "t", "--value", "hex"}, ExitUsage, "--value must be text, json or base64"},
		"missing topic": {[]string{"kafka-a", "nope"}, ExitUsage, "topic nope was not found on kafka-a"},
		"unknown":       {[]string{"nope", "t"}, ExitUsage, "cluster nope not found"},
		"unreachable":   {[]string{"kafka-b", "t"}, ExitClusterFail, "kafka-b: Cluster unreachable: timeout"},
	} {
		_, err := runMessagesCommand(reads, tt.args...)
		if ExitCode(err) != tt.exit || err == nil || !strings.Contains(err.Error(), tt.text) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
