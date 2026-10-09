// Package limit implements a shared, process-local callback budget.
package limit

import (
	"context"
	"errors"
)

// Limiter bounds concurrently executing callbacks. Create it with New and
// share its pointer; a Limiter must not be copied after construction.
// The zero value is invalid. Capacity is immutable and there is no Close.
type Limiter struct {
	slots chan struct{}
}

func New(capacity int) (*Limiter, error) {
	if capacity <= 0 {
		return nil, errors.New("limiter capacity must be positive")
	}
	return &Limiter{slots: make(chan struct{}, capacity)}, nil
}

func Validate(l *Limiter) error {
	if l != nil && l.slots == nil {
		return errors.New("limiter must be constructed with NewLimiter")
	}
	return nil
}

func Capacity(l *Limiter, local int) int {
	if l != nil && cap(l.slots) < local {
		return cap(l.slots)
	}
	return local
}

// Slots is used by the scheduler to wait for capacity together with results
// and cancellation. Sending acquires a permit. Nil disables that select case.
func Slots(l *Limiter) chan<- struct{} {
	if l == nil {
		return nil
	}
	return l.slots
}

func TryAcquire(l *Limiter) bool {
	if l == nil {
		return true
	}
	select {
	case l.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func Acquire(ctx context.Context, l *Limiter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if l == nil {
		return nil
	}
	select {
	case l.slots <- struct{}{}:
		if err := ctx.Err(); err != nil {
			Release(l)
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func Release(l *Limiter) {
	if l != nil {
		<-l.slots
	}
}
