package console

import "time"

// Cookie names the Console sets at sign-in (cli-console-session recipe).
const (
	SessionCookieName = "__Host-kafka3o_session"
	CSRFCookieName    = "__Host-kafka3o_csrf"
	CSRFHeader        = "X-Kafka3O-CSRF"
)

// Session is a signed-in Console session as the CLI keeps it on disk (mode 0600). Token and CSRF are secrets and
// are never printed or logged.
type Session struct {
	ConsoleURL string    `json:"consoleUrl"`
	Username   string    `json:"username"`
	AccountID  string    `json:"accountId"`
	ExpiresAt  time.Time `json:"expiresAt"`
	Token      string    `json:"token"`
	CSRF       string    `json:"csrf"`
}
