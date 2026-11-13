package backups

import (
	"context"
	"time"
)

// Sleep waits for a duration, or until the context is done.
type Sleep interface {
	Execute(ctx context.Context, duration time.Duration) error
}

type sleep struct{}

func NewSleep() Sleep {
	return sleep{}
}

func (sleep) Execute(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
