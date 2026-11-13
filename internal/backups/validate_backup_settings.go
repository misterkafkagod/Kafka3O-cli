package backups

import "fmt"

// ValidateBackupSettings refuses what the Gateway would refuse, before anything is sent.
func ValidateBackupSettings(settings Settings) error {
	switch settings.StartFrom {
	case "", "earliest", "latest":
	default:
		return fmt.Errorf("--start-from must be earliest or latest, not %q", settings.StartFrom)
	}
	switch settings.Compression {
	case "", "gzip", "zstd", "snappy", "none":
	default:
		return fmt.Errorf("--compression must be gzip, zstd, snappy or none, not %q", settings.Compression)
	}
	if settings.RecordsPerFile < 0 || settings.RecordsPerFile > MaxRecordsPerFile {
		return fmt.Errorf("--records-per-file must be between 0 (no limit) and %d, not %d", MaxRecordsPerFile, settings.RecordsPerFile)
	}
	return nil
}
