package accounts

import (
	"strings"
	"testing"
)

func TestValidateTemporaryPassword(t *testing.T) {
	for _, valid := range []string{"twelve-chars", "ççççççççççç1", strings.Repeat("a", 1024)} {
		if err := ValidateTemporaryPassword(valid); err != nil {
			t.Errorf("%q: %v", valid, err)
		}
	}
	for _, invalid := range []string{"", "eleven-char", strings.Repeat("a", 1025)} {
		if err := ValidateTemporaryPassword(invalid); err == nil {
			t.Errorf("%q accepted", invalid)
		}
	}
}
