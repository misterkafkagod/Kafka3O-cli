package console

import (
	"fmt"
	"net/http"
)

// Problem is a Console or Gateway error answer (RFC 9457 problem details with the Kafka3O code).
type Problem struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

func (p Problem) Error() string {
	message := p.Detail
	if message == "" {
		message = p.Title
	}
	if message == "" {
		message = http.StatusText(p.Status)
	}
	if p.Code != "" {
		return fmt.Sprintf("%s (%d %s)", message, p.Status, p.Code)
	}
	return fmt.Sprintf("%s (%d)", message, p.Status)
}
