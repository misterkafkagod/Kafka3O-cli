package cli

import "testing"

func TestDescribeEnvironment(t *testing.T) {
	for input, want := range map[string]string{"PRODUCTION": "Production", "NON_PRODUCTION": "Non-production", "STAGING": "STAGING"} {
		if got := describeEnvironment(input); got != want {
			t.Errorf("%s: %s", input, got)
		}
	}
}
