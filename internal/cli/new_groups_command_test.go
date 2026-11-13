package cli

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/groups"
)

var groupsReads = map[string]string{
	"/api/clusters": whoamiReads["/api/clusters"],
	"/api/clusters/kafka-a/gateway/api/v1/consumer-groups": `[
		{"id":"connect-kafka3o-probe-m16-zstd","protocol":"classic","state":"Empty","members":[],"topics":["kafka3o-m10-verify"],"partitions":[
			{"topic":"kafka3o-m10-verify","partition":0,"lag":"20"},{"topic":"kafka3o-m10-verify","partition":1,"lag":"1"}]},
		{"id":"connect-kafka3o-backup-e2e","protocol":"classic","state":"Empty","members":[],"topics":["kafka3o-m10-verify"],"partitions":[
			{"topic":"kafka3o-m10-verify","partition":0,"lag":"43"},{"topic":"kafka3o-m10-verify","partition":1,"lag":null},{"topic":"kafka3o-m10-verify","partition":2,"lag":null},{"topic":"kafka3o-m10-verify","partition":3,"lag":null}]},
		{"id":"connect-kafka3o-verify-running","protocol":"classic","state":"Stable","members":[{"id":"m"}],"topics":["kafka3o-m10-verify"],"partitions":[{"topic":"kafka3o-m10-verify","partition":0,"lag":"0"}]}]`,
	"/api/clusters/kafka-b/gateway/api/v1/consumer-groups": `[]`,
}

func runGroupsCommand(reads *fakeConsoleReads, args ...string) (string, string, error) {
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewGroupsCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads},
		groups.NewListGroups(clusters.NewListAccessibleClusters(reads), reads)))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"groups"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestGroupsCommand(t *testing.T) {
	out, _, err := runGroupsCommand(&fakeConsoleReads{answers: groupsReads})
	want := "CLUSTER  GROUP                           STATE   MEMBERS  TOPICS  TOTAL LAG\n" +
		"kafka-a  connect-kafka3o-backup-e2e      Empty   0        1       unknown (3 partitions)\n" +
		"kafka-a  connect-kafka3o-probe-m16-zstd  Empty   0        1       21\n" +
		"kafka-a  connect-kafka3o-verify-running  Stable  1        1       0\n"
	if err != nil || out != want {
		t.Fatalf("%v\n%s\nwant\n%s", err, out, want)
	}
	out, _, _ = runGroupsCommand(&fakeConsoleReads{answers: groupsReads}, "--filter", "M16")
	if out != "CLUSTER  GROUP                           STATE  MEMBERS  TOPICS  TOTAL LAG\nkafka-a  connect-kafka3o-probe-m16-zstd  Empty  0        1       21\n" {
		t.Fatalf("filter:\n%s", out)
	}
	out, _, _ = runGroupsCommand(&fakeConsoleReads{answers: groupsReads}, "--cluster", "kafka-b")
	if out != "No consumer groups.\n" {
		t.Fatalf("empty: %q", out)
	}
	out, _, _ = runGroupsCommand(&fakeConsoleReads{answers: groupsReads}, "-o", "json")
	var list groupList
	if json.Unmarshal([]byte(out), &list) != nil || len(list.Groups) != 3 || list.Groups[0].TotalLag != (groups.Lag{Kind: "unknown", Partitions: 3}) || list.Groups[1].TotalLag.Lag != "21" {
		t.Fatalf("json:\n%s", out)
	}
}

func TestGroupsCommandWithAFailingCluster(t *testing.T) {
	reads := &fakeConsoleReads{answers: groupsReads, errors: map[string]error{
		"/api/clusters/kafka-b/gateway/api/v1/consumer-groups": console.Problem{Status: 503, Code: "gateway_unavailable", Title: "The Gateway could not be reached"}}}
	out, errOut, err := runGroupsCommand(reads)
	if ExitCode(err) != ExitClusterFail || errOut != "kafka-b: Gateway unreachable: The Gateway could not be reached\n" || out == "" {
		t.Fatalf("%v %q\n%s", err, errOut, out)
	}
}
