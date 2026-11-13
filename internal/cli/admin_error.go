package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// adminError is a port of the UI's describeAdminError for the Console's administration problems. Signed out,
// "may have been applied" and an unreachable Console keep their own errors.
func adminError(err error) error {
	var problem console.Problem
	if err == nil || !errors.As(err, &problem) {
		return err
	}
	operation := ""
	if problem.OperationID != "" {
		operation = " (operation " + problem.OperationID + ")"
	}
	switch {
	case problem.Status >= 500 && strings.HasSuffix(problem.Code, "_audit_unavailable"):
		return fmt.Errorf("%w: the audit trail is unavailable, so nothing was changed; try again later%s", ErrServiceUnavailable, operation)
	case problem.Status >= 500:
		return fmt.Errorf("%w: the Console administration service is unavailable; try again later%s", ErrServiceUnavailable, operation)
	case problem.Status == 403 && problem.Code == "audit_access_denied":
		return errors.New("denied: Audit for a cluster requires ADMIN on it; all-cluster audit requires global ADMIN.")
	case problem.Status == 403 && problem.Code == "password_change_required":
		return errors.New("denied: Change your temporary password before administering the Console.")
	case problem.Status == 403 && problem.Code == "self_access_change":
		return errors.New("denied: You cannot change your own access. Ask another global administrator.")
	case problem.Status == 403:
		return errors.New("denied: Only a local global administrator can administer accounts, sessions, access and cluster environments.")
	case problem.Code == "policy_conflict":
		return errors.New("refused: Access changed since it was read. Check it again and retry.")
	case problem.Code == "last_global_admin" || problem.Code == "last_local_global_admin":
		return errors.New("refused: The last enabled local global administrator cannot be removed.")
	case problem.Code == "change_reason_required":
		return errors.New("refused: A change reason is required.")
	case problem.Status == 404:
		return fmt.Errorf("not found: %s", problem.Title)
	default:
		return fmt.Errorf("refused: %s", problem.Title)
	}
}
