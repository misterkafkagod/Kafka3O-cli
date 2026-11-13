package cli

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func prepare(t *testing.T, input string, request ChangeRequest) (string, string, error) {
	t.Helper()
	reads := &fakeConsoleReads{answers: map[string]string{"/api/clusters": whoamiReads["/api/clusters"]}}
	var prompt bytes.Buffer
	reason, err := PrepareChange(context.Background(), signedIn, clusters.NewListAccessibleClusters(reads), bufio.NewReader(strings.NewReader(input)), &prompt, request)
	return reason, prompt.String(), err
}

func TestPrepareChangeOnNonProduction(t *testing.T) {
	request := ChangeRequest{Cluster: "kafka-a", Summary: []string{"Produce to kafka3o-m10-verify"}}
	reason, prompt, err := prepare(t, "y\n", request)
	if err != nil || reason != "" || prompt != "Change on kafka-a (Non-production):\n  Produce to kafka3o-m10-verify\nProceed? [y/N] " {
		t.Fatalf("%q %q %v", reason, prompt, err)
	}
	for _, answer := range []string{"n\n", "\n", "", "maybe\n"} {
		if _, _, err := prepare(t, answer, request); !errors.Is(err, console.ErrCanceled) {
			t.Errorf("%q: %v", answer, err)
		}
	}
	request.Yes = true
	if _, prompt, err := prepare(t, "", request); err != nil || strings.Contains(prompt, "Proceed?") {
		t.Fatalf("--yes: %q %v", prompt, err)
	}
	request.Reason = " ticket 7 "
	if reason, _, _ := prepare(t, "", request); reason != "ticket 7" {
		t.Fatalf("reason on non-production: %q", reason)
	}
}

func TestPrepareChangeOnProduction(t *testing.T) {
	request := ChangeRequest{Cluster: "kafka-b", Summary: []string{"Produce to orders"}}
	reason, prompt, err := prepare(t, "INC-42 replay\ny\n", request)
	if err != nil || reason != "INC-42 replay" || !strings.Contains(prompt, "Change on kafka-b (Production):") || !strings.Contains(prompt, "Change reason (required on Production): ") {
		t.Fatalf("%q %q %v", reason, prompt, err)
	}
	for _, input := range []string{"\n", "   \n", ""} {
		if _, _, err := prepare(t, input, request); !errors.Is(err, console.ErrCanceled) {
			t.Errorf("blank reason %q: %v", input, err)
		}
	}
	if _, _, err := prepare(t, "INC-42\nn\n", request); !errors.Is(err, console.ErrCanceled) {
		t.Errorf("reason then no: %v", err)
	}
	request.Yes = true
	if _, _, err := prepare(t, "", request); !errors.Is(err, ErrReasonRequired) || ExitCode(err) != ExitUsage {
		t.Errorf("--yes without --reason: %v", err)
	}
	request.Reason = "INC-42"
	if reason, prompt, err := prepare(t, "", request); err != nil || reason != "INC-42" || strings.Contains(prompt, "Change reason") {
		t.Errorf("--yes --reason: %q %q %v", reason, prompt, err)
	}
}

func TestPrepareChangeUnknownCluster(t *testing.T) {
	var unknown clusters.ErrUnknownCluster
	if _, _, err := prepare(t, "y\n", ChangeRequest{Cluster: "nope"}); !errors.As(err, &unknown) {
		t.Fatalf("%v", err)
	}
}
