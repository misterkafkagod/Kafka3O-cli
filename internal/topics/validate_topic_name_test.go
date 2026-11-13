package topics

import (
	"strings"
	"testing"
)

func TestValidateTopicName(t *testing.T) {
	for _, good := range []string{"kafka3o-cli-p112", "a", "A.b_c-9", strings.Repeat("x", 249)} {
		if err := ValidateTopicName(good); err != nil {
			t.Errorf("%q: %v", good, err)
		}
	}
	for _, bad := range []string{"", ".", "..", "with space", "slash/name", "ümlaut", strings.Repeat("x", 250)} {
		if err := ValidateTopicName(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
