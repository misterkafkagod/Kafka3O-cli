package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ChangeRequest describes one change before it is sent.
type ChangeRequest struct {
	Cluster string
	Summary []string
	Yes     bool
	Reason  string
}

// ErrReasonRequired means a Production change was asked for without a reason in a non-interactive run.
var ErrReasonRequired = errors.New("a change reason is required on a Production cluster: add --reason")

// PrepareChange shows the change, asks for a change reason on a Production cluster and asks "Proceed? [y/N]"
// unless --yes, like the UI's confirmations. It returns the reason to send; ErrCanceled means nothing may be sent.
func PrepareChange(ctx context.Context, session console.Session, accessible clusters.ListAccessibleClusters, input *bufio.Reader,
	prompt io.Writer, request ChangeRequest) (string, error) {
	all, err := accessible.Execute(ctx, session)
	if err != nil {
		return "", err
	}
	var cluster *clusters.AccessibleCluster
	for i := range all {
		if all[i].ID == request.Cluster {
			cluster = &all[i]
		}
	}
	if cluster == nil {
		return "", clusters.ErrUnknownCluster{Cluster: request.Cluster}
	}
	production := cluster.Environment == "PRODUCTION"
	fmt.Fprintf(prompt, "Change on %s (%s):\n", cluster.ID, describeEnvironment(cluster.Environment))
	for _, line := range request.Summary {
		fmt.Fprintf(prompt, "  %s\n", line)
	}
	reason := strings.TrimSpace(request.Reason)
	if production && reason == "" {
		if request.Yes {
			return "", ErrReasonRequired
		}
		fmt.Fprint(prompt, "Change reason (required on Production): ")
		line, readErr := input.ReadString('\n')
		reason = strings.TrimSpace(line)
		if reason == "" || (readErr != nil && line == "") {
			return "", console.ErrCanceled
		}
	}
	if !request.Yes {
		fmt.Fprint(prompt, "Proceed? [y/N] ")
		line, _ := input.ReadString('\n')
		if answer := strings.ToLower(strings.TrimSpace(line)); answer != "y" && answer != "yes" {
			return "", console.ErrCanceled
		}
	}
	return reason, nil
}
