package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

func runTopicChange(writes *fakeConsoleWrites, stdin string, args ...string) (string, string, error) {
	reads := &fakeConsoleReads{answers: map[string]string{"/api/clusters": whoamiReads["/api/clusters"]}}
	deps := SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}
	accessible := clusters.NewListAccessibleClusters(reads)
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewCreateTopicCommand(deps, accessible, topics.NewCreateTopic(writes)), NewDeleteTopicCommand(deps, accessible, topics.NewDeleteTopic(writes)))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestCreateTopicCommand(t *testing.T) {
	path := "/api/clusters/kafka-a/gateway/api/v1/topics"
	writes := &fakeConsoleWrites{answers: map[string]string{path: `{"name":"kafka3o-cli-p112","partitions":2,"replicationFactor":1}`}}
	out, prompt, err := runTopicChange(writes, "y\n", "create-topic", "kafka-a", "kafka3o-cli-p112", "--partitions", "2")
	if err != nil || out != "Created topic kafka3o-cli-p112 on kafka-a.\n" || !strings.Contains(prompt, "Create topic kafka3o-cli-p112 with 2 partition(s) and replication factor 1") ||
		writes.sent[0].Body != `{"name":"kafka3o-cli-p112","partitions":2,"replicationFactor":1}` {
		t.Fatalf("%v %q %q %+v", err, out, prompt, writes.sent)
	}
	for _, args := range [][]string{{"create-topic", "kafka-a", "bad name", "--yes"}, {"create-topic", "kafka-a", "t", "--partitions", "0", "--yes"}} {
		before := len(writes.sent)
		if _, _, err := runTopicChange(writes, "", args...); err == nil || ExitCode(err) != ExitUsage || len(writes.sent) != before {
			t.Errorf("%v: %v", args, err)
		}
	}
	exists := &fakeConsoleWrites{errors: map[string]error{path: console.Problem{Status: 409, Code: "topic_exists", Detail: `Topic "kafka3o-m10-verify" already exists`}}}
	if _, _, err := runTopicChange(exists, "", "create-topic", "kafka-a", "kafka3o-m10-verify", "--yes"); err == nil || err.Error() != `refused: Topic "kafka3o-m10-verify" already exists` {
		t.Errorf("exists: %v", err)
	}
}
