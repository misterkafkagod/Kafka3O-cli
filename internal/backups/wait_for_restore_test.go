package backups

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// fakeListings answers each listing with the next prepared answer.
type fakeListings struct {
	answers  []string
	failures []clusters.ClusterFailure
	calls    int
}

func (f *fakeListings) Execute(_ context.Context, _ console.Session, cluster string) ([]ClusterBackups, []clusters.ClusterFailure, error) {
	if f.failures != nil {
		return nil, f.failures, nil
	}
	answer := f.answers[min(f.calls, len(f.answers)-1)]
	f.calls++
	var parsed struct {
		Backups []Backup `json:"backups"`
	}
	if err := json.Unmarshal([]byte(answer), &parsed); err != nil {
		return nil, nil, err
	}
	return []ClusterBackups{{Cluster: cluster, Backups: parsed.Backups}}, nil, nil
}

type fakeSleep struct {
	slept int
	err   error
}

func (f *fakeSleep) Execute(context.Context, time.Duration) error {
	f.slept++
	return f.err
}

func restoreAnswer(restore string) string {
	return `{"backups":[{"topic":"t","connector":"kafka3o-backup-t","state":"PAUSED","restoring":true,"restore":` + restore + `}]}`
}

func TestWaitForRestore(t *testing.T) {
	list := &fakeListings{answers: []string{
		`{"backups":[{"topic":"t","state":"PAUSED","restoring":true}]}`,
		restoreAnswer(`{"restoreId":"","state":"RUNNING","restored":0,"total":0,"finished":false,"error":"restore progress is unavailable: timeout"}`),
		restoreAnswer(`{"restoreId":"r1","state":"RUNNING","restored":2,"total":9,"finished":false}`),
		restoreAnswer(`{"restoreId":"r1","state":"RUNNING","restored":2,"total":9,"finished":false}`),
		restoreAnswer(`{"restoreId":"r1","state":"RUNNING","restored":9,"total":9,"finished":true}`),
	}}
	sleep := &fakeSleep{}
	var progress bytes.Buffer
	outcome, err := NewWaitForRestore(list, sleep, 5*time.Second).Execute(context.Background(), console.Session{}, "kafka-a", "t", "r1", &progress)
	if err != nil || outcome != (RestoreOutcome{Finished: true, Restored: 9, Total: 9}) || sleep.slept != 4 ||
		progress.String() != "restore running; progress not checked yet\nrestore progress is unavailable: timeout\nrestoring 2 of 9 records\n" {
		t.Fatalf("%+v %v %d\n%s", outcome, err, sleep.slept, progress.String())
	}
}

func TestWaitForRestoreEnds(t *testing.T) {
	for answer, want := range map[string]RestoreOutcome{
		restoreAnswer(`{"restoreId":"r1","state":"FAILED","restored":3,"total":9,"finished":false,"error":"S3 down"}`): {Error: "S3 down", Restored: 3, Total: 9},
		restoreAnswer(`{"restoreId":"r2","state":"RUNNING","restored":0,"total":5,"finished":false}`):                  {Gone: true},
		`{"backups":[{"topic":"t","state":"RUNNING"}]}`:                                                                {Gone: true},
		`{"backups":[]}`: {Gone: true},
	} {
		outcome, err := NewWaitForRestore(&fakeListings{answers: []string{answer}}, &fakeSleep{}, time.Second).Execute(context.Background(), console.Session{}, "kafka-a", "t", "r1", &bytes.Buffer{})
		if err != nil || outcome != want {
			t.Errorf("%s: %+v %v", answer, outcome, err)
		}
	}
	failing := &fakeListings{failures: []clusters.ClusterFailure{{Cluster: "kafka-a", Status: clusters.StatusUnavailable, Error: "storage down"}}}
	if _, err := NewWaitForRestore(failing, &fakeSleep{}, time.Second).Execute(context.Background(), console.Session{}, "kafka-a", "t", "r1", &bytes.Buffer{}); err == nil || err.Error() == "" {
		t.Fatalf("failing: %v", err)
	}
	running := &fakeListings{answers: []string{restoreAnswer(`{"restoreId":"r1","state":"RUNNING","restored":1,"total":9,"finished":false}`)}}
	if _, err := NewWaitForRestore(running, &fakeSleep{err: context.Canceled}, time.Second).Execute(context.Background(), console.Session{}, "kafka-a", "t", "r1", &bytes.Buffer{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}

func TestSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := NewSleep().Execute(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
	if err := NewSleep().Execute(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("slept: %v", err)
	}
}
