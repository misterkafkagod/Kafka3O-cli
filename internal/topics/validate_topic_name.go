package topics

import (
	"fmt"
	"regexp"
)

var topicName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)

// ValidateTopicName applies Kafka's topic name rules before anything is sent.
func ValidateTopicName(name string) error {
	if !topicName.MatchString(name) || name == "." || name == ".." {
		return fmt.Errorf("invalid topic name %q: use 1 to 249 letters, digits, '.', '_' or '-' (not '.' or '..')", name)
	}
	return nil
}
