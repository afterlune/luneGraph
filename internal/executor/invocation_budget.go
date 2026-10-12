package executor

import "errors"

var errInvocationBudget = errors.New("maximum invocation count exceeded")

func checkInvocationBudget[S any](checkpoint *Checkpoint[S], additional, maximum int) error {
	if maximum == 0 || additional == 0 {
		return nil
	}
	current := len(checkpoint.Invocations)
	if current > maximum || additional > maximum-current {
		return errInvocationBudget
	}
	return nil
}
