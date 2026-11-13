package accounts

import "fmt"

// ValidateUsername mirrors the Console's local username rule: 1 to 64 letters and digits, with '.', '_' or '-'
// allowed after the first character. Usernames are compared case-insensitively.
func ValidateUsername(username string) error {
	valid := len(username) >= 1 && len(username) <= 64
	for i := 0; valid && i < len(username); i++ {
		character := username[i]
		letter := character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z'
		digit := character >= '0' && character <= '9'
		valid = letter || digit || i > 0 && (character == '.' || character == '_' || character == '-')
	}
	if !valid {
		return fmt.Errorf("invalid username %q: use 1 to 64 letters and digits, with '.', '_' or '-' after the first", username)
	}
	return nil
}
