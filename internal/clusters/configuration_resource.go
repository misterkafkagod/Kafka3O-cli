package clusters

// ConfigurationResource is a topic or a broker whose configuration is read or changed.
type ConfigurationResource struct {
	Kind string // "topic" or "broker"
	Name string // topic name or broker id
}

// Setting is one configuration entry as the Gateway returns it; Value is nil when unset or sensitive.
type Setting struct {
	Name      string  `json:"name"`
	Value     *string `json:"value"`
	Sensitive bool    `json:"sensitive"`
	ReadOnly  bool    `json:"readOnly"`
	Source    string  `json:"source"`
	Type      string  `json:"type"`
}
