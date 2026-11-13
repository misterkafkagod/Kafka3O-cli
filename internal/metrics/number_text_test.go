package metrics

import (
	"encoding/json"
	"testing"
)

func TestNumberText(t *testing.T) {
	number := json.Number("9007199254740993")
	if numberText(&number) != "9007199254740993" || numberText(nil) != "" {
		t.Fatal("numberText")
	}
}
