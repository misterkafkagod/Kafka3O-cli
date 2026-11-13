package cli

import "strings"

// describeAccessDenial is a port of the UI's describeAccessDenial for a 403 "Not permitted: <denial>" problem.
func describeAccessDenial(detail string) string {
	switch strings.TrimPrefix(detail, "Not permitted: ") {
	case "production_admin_required":
		return "This is a Production cluster: only an ADMIN for it may change it."
	case "change_reason_required":
		return "Production changes need a recorded change reason."
	case "environment_unclassified":
		return "This cluster is not classified yet, so all Kafka changes are blocked."
	default:
		return "Your role does not allow this action on this cluster."
	}
}
