package backups

// Settings are the backup settings a user may choose; empty fields take the Gateway's defaults.
type Settings struct {
	StartFrom      string `json:"startFrom,omitempty"`
	Compression    string `json:"compression,omitempty"`
	RecordsPerFile int    `json:"recordsPerFile,omitempty"`
}

// MaxRecordsPerFile is the Gateway's limit; 0 means no limit.
const MaxRecordsPerFile = 1000000

// Backup action kinds, as the Gateway names them.
const (
	ActionPause  = "pause"
	ActionResume = "resume"
	ActionDelete = "delete"
)
