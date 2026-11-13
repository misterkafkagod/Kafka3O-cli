package cli

import "testing"

func TestDescribeAccessDenial(t *testing.T) {
	for detail, want := range map[string]string{
		"Not permitted: production_admin_required": "This is a Production cluster: only an ADMIN for it may change it.",
		"Not permitted: change_reason_required":    "Production changes need a recorded change reason.",
		"Not permitted: environment_unclassified":  "This cluster is not classified yet, so all Kafka changes are blocked.",
		"Not permitted: no_role":                   "Your role does not allow this action on this cluster.",
		"":                                         "Your role does not allow this action on this cluster.",
	} {
		if got := describeAccessDenial(detail); got != want {
			t.Errorf("%q: %q", detail, got)
		}
	}
}
