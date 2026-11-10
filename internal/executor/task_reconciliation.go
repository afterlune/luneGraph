package executor

import "context"

// reconcileTasks drops speculative inputs invalidated by an accepted change and
// cancels callbacks whose invocation or activation no longer exists.
func reconcileTasks[S any](l *live[S], prepared map[string]workerTask[S], running map[taskRef]context.CancelFunc) {
	if len(prepared) == 0 && len(running) == 0 {
		return
	}
	for id := range prepared {
		lg, _, ok := l.group(id, -1)
		if !ok || !l.joinEligible(lg) {
			delete(prepared, id)
		}
	}
	cancelInvalidTasks(l, running)
}

func cancelInvalidTasks[S any](l *live[S], running map[taskRef]context.CancelFunc) {
	for ref, cancel := range running {
		if !l.valid(ref) {
			cancel()
		}
	}
}
