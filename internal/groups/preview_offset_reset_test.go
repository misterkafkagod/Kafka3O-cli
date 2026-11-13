package groups

import (
	"context"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestPreviewOffsetReset(t *testing.T) {
	base := "/api/clusters/kafka-a/gateway/api/v1/consumer-groups/probe/offset-reset?"
	get := &fakeGetJSON{answers: map[string]string{
		base + "target=latest&topic=kafka3o-m10-verify":                            `{"group":"probe","topic":"kafka3o-m10-verify","partitions":[{"partition":0,"currentOffset":"5","targetOffset":"66"},{"partition":1,"currentOffset":null,"targetOffset":"3"}]}`,
		base + "target=timestamp&timestamp=1794456480000&topic=kafka3o-m10-verify": `{"group":"probe","topic":"kafka3o-m10-verify","partitions":[]}`,
	}}
	preview, err := NewPreviewOffsetReset(get).Execute(context.Background(), console.Session{}, "kafka-a", "probe", "kafka3o-m10-verify", ResetTarget{Kind: "latest"})
	if err != nil || len(preview.Partitions) != 2 || *preview.Partitions[0].CurrentOffset != "5" || preview.Partitions[1].CurrentOffset != nil {
		t.Fatalf("%+v %v", preview, err)
	}
	if _, err := NewPreviewOffsetReset(get).Execute(context.Background(), console.Session{}, "kafka-a", "probe", "kafka3o-m10-verify", ResetTarget{Kind: "timestamp", Timestamp: 1794456480000}); err != nil {
		t.Fatal(err)
	}
}
