package sessions

// Session is one active Console session as an administrator sees it; its token is never part of it.
type Session struct {
	ID                string `json:"id"`
	AccountID         string `json:"accountId"`
	Username          string `json:"username"`
	AuthSource        string `json:"authSource"`
	CreatedAt         string `json:"createdAt"`
	IdleExpiresAt     string `json:"idleExpiresAt"`
	AbsoluteExpiresAt string `json:"absoluteExpiresAt"`
	IsCurrent         bool   `json:"isCurrent"`
}
