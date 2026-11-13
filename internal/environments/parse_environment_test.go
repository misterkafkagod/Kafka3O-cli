package environments

import "testing"

func TestParseEnvironment(t *testing.T) {
	for text, want := range map[string]string{"production": "PRODUCTION", "PRODUCTION": "PRODUCTION", "non-production": "NON_PRODUCTION", "NON_PRODUCTION": "NON_PRODUCTION"} {
		if environment, err := ParseEnvironment(text); err != nil || environment != want {
			t.Errorf("%s: %q %v", text, environment, err)
		}
	}
	for _, bad := range []string{"", "prod", "staging"} {
		if _, err := ParseEnvironment(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
