package backups

import "testing"

func TestValidateBackupSettings(t *testing.T) {
	for _, valid := range []Settings{{}, {StartFrom: "latest", Compression: "zstd", RecordsPerFile: 500}, {StartFrom: "earliest", Compression: "none", RecordsPerFile: MaxRecordsPerFile}, {Compression: "snappy"}, {Compression: "gzip"}} {
		if err := ValidateBackupSettings(valid); err != nil {
			t.Errorf("%+v: %v", valid, err)
		}
	}
	for settings, want := range map[Settings]string{
		{StartFrom: "now"}:                      `--start-from must be earliest or latest, not "now"`,
		{Compression: "lz4"}:                    `--compression must be gzip, zstd, snappy or none, not "lz4"`,
		{RecordsPerFile: -1}:                    "--records-per-file must be between 0 (no limit) and 1000000, not -1",
		{RecordsPerFile: MaxRecordsPerFile + 1}: "--records-per-file must be between 0 (no limit) and 1000000, not 1000001",
	} {
		if err := ValidateBackupSettings(settings); err == nil || err.Error() != want {
			t.Errorf("%+v: %v", settings, err)
		}
	}
}
