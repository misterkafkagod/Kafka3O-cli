package output

import (
	"io"
	"strings"
	"text/tabwriter"
)

// WriteTable writes a header row and the rows as aligned columns; tabs and line breaks inside cells become spaces
// so a value can never break the layout.
func WriteTable(w io.Writer, headers []string, rows [][]string) error {
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	clean := strings.NewReplacer("\t", " ", "\n", " ", "\r", " ")
	for _, row := range append([][]string{headers}, rows...) {
		cells := make([]string, len(row))
		for i, cell := range row {
			cells[i] = clean.Replace(cell)
		}
		if _, err := io.WriteString(table, strings.Join(cells, "\t")+"\n"); err != nil {
			return err
		}
	}
	return table.Flush()
}
