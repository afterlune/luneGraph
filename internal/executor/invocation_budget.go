package executor

import "errors"

var errInvocationBudget = errors.New("maximum invocation count exceeded")

// invocationBudget counts the full live position, not a partial candidate.
type invocationBudget struct {
	retained int
	maximum  int
}

func (b invocationBudget) check(additional int) error {
	if b.maximum == 0 || additional == 0 {
		return nil
	}
	if b.retained > b.maximum || additional > b.maximum-b.retained {
		return errInvocationBudget
	}
	return nil
}
