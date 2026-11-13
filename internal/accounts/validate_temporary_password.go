package accounts

import (
	"errors"
	"unicode/utf8"
)

// ValidateTemporaryPassword applies the Console's length policy before anything is sent.
func ValidateTemporaryPassword(password string) error {
	if utf8.RuneCountInString(password) < 12 || len(password) > 1024 {
		return errors.New("the temporary password needs at least 12 characters (and at most 1024 bytes)")
	}
	return nil
}
