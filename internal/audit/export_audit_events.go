package audit

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ExportAuditEvents downloads the Console's CSV export of the matching audit events.
type ExportAuditEvents interface {
	Execute(ctx context.Context, session console.Session, query string) ([]byte, int, error)
}

type exportAuditEvents struct {
	get console.GetCSV
}

func NewExportAuditEvents(get console.GetCSV) ExportAuditEvents {
	return exportAuditEvents{get: get}
}

// Execute takes a query built by BuildAuditQuery without a limit or cursor, and returns the CSV unchanged with
// the number of events in it (records after the header; quoted cells may span lines).
func (e exportAuditEvents) Execute(ctx context.Context, session console.Session, query string) ([]byte, int, error) {
	path := "/api/audit/export"
	if query != "" {
		path += "?" + query
	}
	body, err := e.get.Execute(ctx, session, path)
	if err != nil {
		return nil, 0, err
	}
	reader := csv.NewReader(bytes.NewReader(body))
	reader.FieldsPerRecord = -1
	records := 0
	for {
		_, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("%w: the export is not valid CSV: %v", console.ErrConsoleUnavailable, err)
		}
		records++
	}
	return body, max(records-1, 0), nil
}
