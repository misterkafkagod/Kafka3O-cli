package accounts

import "testing"

func TestValidateUsername(t *testing.T) {
	for _, valid := range []string{"a", "kafka3o-cli-test", "Ops.User_1", "x234567890123456789012345678901234567890123456789012345678901234"} {
		if err := ValidateUsername(valid); err != nil {
			t.Errorf("%q: %v", valid, err)
		}
	}
	for _, invalid := range []string{"", "-lead", ".lead", "has space", "émile", "x2345678901234567890123456789012345678901234567890123456789012345"} {
		if err := ValidateUsername(invalid); err == nil {
			t.Errorf("%q accepted", invalid)
		}
	}
}
