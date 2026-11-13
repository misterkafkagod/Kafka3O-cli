package cli

import (
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestAdminError(t *testing.T) {
	for problem, want := range map[console.Problem]string{
		{Status: 403, Code: "audit_access_denied", Title: "Audit access denied"}:                             "denied: Audit for a cluster requires ADMIN on it; all-cluster audit requires global ADMIN.",
		{Status: 403, Code: "global_admin_required", Title: "Global administrator access required"}:          "denied: Only a local global administrator can administer accounts, sessions, access and cluster environments.",
		{Status: 403, Code: "password_change_required", Title: "Password change required"}:                   "denied: Change your temporary password before administering the Console.",
		{Status: 403, Code: "self_access_change", Title: "Administrators cannot change their own access"}:    "denied: You cannot change your own access. Ask another global administrator.",
		{Status: 409, Code: "policy_conflict", Title: "Access changed since it was read; reload and review"}: "refused: Access changed since it was read. Check it again and retry.",
		{Status: 409, Code: "last_local_global_admin", Title: "…"}:                                           "refused: The last enabled local global administrator cannot be removed.",
		{Status: 409, Code: "last_global_admin", Title: "…"}:                                                 "refused: The last enabled local global administrator cannot be removed.",
		{Status: 400, Code: "change_reason_required", Title: "A change reason is required"}:                  "refused: A change reason is required.",
		{Status: 404, Code: "account_not_found", Title: "Local account not found"}:                           "not found: Local account not found",
		{Status: 409, Code: "username_in_use", Title: "Local username is already in use"}:                    "refused: Local username is already in use",
	} {
		if err := adminError(problem); err == nil || err.Error() != want || ExitCode(err) != ExitUsage {
			t.Errorf("%s: %v", problem.Code, err)
		}
	}
	audit := adminError(console.Problem{Status: 503, Code: "access_change_audit_unavailable", OperationID: "op-7"})
	if audit.Error() != "the Console could not answer: the audit trail is unavailable, so nothing was changed; try again later (operation op-7)" || ExitCode(audit) != ExitClusterFail {
		t.Errorf("audit: %v", audit)
	}
	if err := adminError(console.Problem{Status: 503, Code: "account_list_unavailable"}); !errors.Is(err, ErrServiceUnavailable) {
		t.Errorf("unavailable: %v", err)
	}
	for _, passThrough := range []error{console.ErrSignedOut, console.ErrOutcomeUnknown, console.ErrConsoleUnavailable} {
		if err := adminError(passThrough); err != passThrough {
			t.Errorf("%v became %v", passThrough, err)
		}
	}
}
