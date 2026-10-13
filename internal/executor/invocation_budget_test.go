package executor

import (
	"errors"
	"testing"
)

func TestCheckInvocationBudget(t *testing.T) {
	for _, test := range []struct {
		name       string
		current    int
		additional int
		maximum    int
		wantErr    bool
	}{
		{name: "unlimited", current: 2, additional: 3},
		{name: "no additions", current: 2, additional: 0, maximum: 1},
		{name: "exactly at limit", current: 2, additional: 1, maximum: 3},
		{name: "addition exceeds limit", current: 2, additional: 2, maximum: 3, wantErr: true},
		{name: "already above limit", current: 4, additional: 1, maximum: 3, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := (invocationBudget{retained: test.current, maximum: test.maximum}).check(test.additional)
			if errors.Is(err, errInvocationBudget) != test.wantErr {
				t.Fatalf("checkInvocationBudget(%d, %d, %d) = %v", test.current, test.additional, test.maximum, err)
			}
		})
	}
}
