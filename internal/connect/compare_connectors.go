package connect

import "strings"

// CompareConnectors orders connectors by name.
func CompareConnectors(a, b Connector) int {
	return strings.Compare(a.Name, b.Name)
}
