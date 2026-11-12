package console

import "testing"

func TestProblemError(t *testing.T) {
	for problem, want := range map[Problem]string{
		{Status: 403, Code: "forbidden", Title: "Forbidden", Detail: "Not permitted: change_reason_required"}: "Not permitted: change_reason_required (403 forbidden)",
		{Status: 503, Title: "Gateway unavailable"}:                                                           "Gateway unavailable (503)",
	} {
		if got := problem.Error(); got != want {
			t.Errorf("%q, want %q", got, want)
		}
	}
}
