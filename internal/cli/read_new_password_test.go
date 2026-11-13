package cli

import (
	"bufio"
	"strings"
	"testing"
)

// passwordSequence answers each hidden prompt with the next password.
type passwordSequence struct {
	passwords []string
	prompts   []string
}

func (p *passwordSequence) Execute(prompt string) (string, error) {
	p.prompts = append(p.prompts, prompt)
	next := p.passwords[0]
	p.passwords = p.passwords[1:]
	return next, nil
}

func TestReadNewPassword(t *testing.T) {
	if password, err := readNewPassword(bufio.NewReader(strings.NewReader("stdin-password-1\r\ny\n")), true, nil); err != nil || password != "stdin-password-1" {
		t.Fatalf("stdin: %q %v", password, err)
	}
	hidden := &passwordSequence{passwords: []string{"hidden-password-1", "hidden-password-1"}}
	if password, err := readNewPassword(nil, false, hidden); err != nil || password != "hidden-password-1" || len(hidden.prompts) != 2 {
		t.Fatalf("hidden: %q %v %v", password, err, hidden.prompts)
	}
	for _, test := range []struct {
		stdin     string
		fromStdin bool
		hidden    []string
		want      string
	}{
		{"", true, nil, "--password-stdin: no password on standard input"},
		{"short\n", true, nil, "the temporary password needs at least 12 characters (and at most 1024 bytes); nothing was sent"},
		{"", false, []string{"hidden-password-1", "hidden-password-2"}, "the two passwords do not match"},
	} {
		_, err := readNewPassword(bufio.NewReader(strings.NewReader(test.stdin)), test.fromStdin, &passwordSequence{passwords: test.hidden})
		if err == nil || err.Error() != test.want {
			t.Errorf("%+v: %v", test, err)
		}
	}
}
