package console

import (
	"fmt"
	"net/http"
	"strconv"
)

// Problem is a Console or Gateway error answer (RFC 9457 problem details with the Kafka3O code).
type Problem struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	// OperationID is the Console's correlation id for an administration problem, if any.
	OperationID string `json:"operationId,omitempty"`
}

func (p Problem) Error() string {
	message := p.Detail
	if message == "" {
		message = p.Title
	}
	if message == "" {
		message = http.StatusText(p.Status)
	}
	reference := strconv.Itoa(p.Status)
	if p.Code != "" {
		reference += " " + p.Code
	}
	if p.OperationID != "" {
		reference += ", operation " + p.OperationID
	}
	return fmt.Sprintf("%s (%s)", message, reference)
}
