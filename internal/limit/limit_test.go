package limit

import (
	"context"
	"errors"
	"testing"
)

func TestLimiter(t *testing.T) {
	for _, n := range []int{0, -1} {
		if _, err := New(n); err == nil {
			t.Fatal("accepted invalid capacity")
		}
	}
	l, err := New(2)
	if err != nil || Validate(l) != nil || Validate(nil) != nil || Validate(&Limiter{}) == nil {
		t.Fatal("validation")
	}
	if Capacity(l, 8) != 2 || Capacity(l, 1) != 1 || Capacity(nil, 8) != 8 || Slots(nil) != nil {
		t.Fatal("capacity")
	}
	if !TryAcquire(l) || !TryAcquire(l) || TryAcquire(l) {
		t.Fatal("permit count")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(Acquire(ctx, l), context.Canceled) {
		t.Fatal("pre-cancelled acquire")
	}
	Release(l)
	if err := Acquire(context.Background(), l); err != nil {
		t.Fatal(err)
	}
	Release(l)
	Release(l)
	if !TryAcquire(nil) || Acquire(context.Background(), nil) != nil {
		t.Fatal("nil budget")
	}
	Release(nil)
	Slots(l) <- struct{}{}
	Release(l)
}

func TestWaitingCancellationAndReuse(t *testing.T) {
	l, _ := New(1)
	TryAcquire(l)
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { close(ready); done <- Acquire(ctx, l) }()
	<-ready
	cancel()
	if !errors.Is(<-done, context.Canceled) {
		t.Fatal("waiting cancellation")
	}
	Release(l)
	if !TryAcquire(l) {
		t.Fatal("permit leaked")
	}
	Release(l)
}

type cancellingContext struct {
	context.Context
	checks int
}

func (c *cancellingContext) Err() error {
	c.checks++
	if c.checks > 1 {
		return context.Canceled
	}
	return nil
}

func TestCancellationAfterAcquireReleasesPermit(t *testing.T) {
	l, _ := New(1)
	ctx := &cancellingContext{Context: context.Background()}
	if !errors.Is(Acquire(ctx, l), context.Canceled) || !TryAcquire(l) {
		t.Fatal("cancelled acquisition leaked permit")
	}
	Release(l)
}
