package topics

import (
	"errors"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// SectionReason is why one section of a view could not be read: the problem's detail or title, or the error text.
func SectionReason(err error) string {
	var problem console.Problem
	if errors.As(err, &problem) {
		if problem.Detail != "" {
			return problem.Detail
		}
		if problem.Title != "" {
			return problem.Title
		}
	}
	return err.Error()
}
