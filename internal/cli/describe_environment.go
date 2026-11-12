package cli

// describeEnvironment turns the Console's environment value into the words the UI uses.
func describeEnvironment(environment string) string {
	switch environment {
	case "PRODUCTION":
		return "Production"
	case "NON_PRODUCTION":
		return "Non-production"
	default:
		return environment
	}
}
