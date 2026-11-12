package cli

// ReadPassword asks for a password without showing it.
type ReadPassword interface {
	Execute(prompt string) (string, error)
}
