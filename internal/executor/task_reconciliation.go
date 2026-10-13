package executor

import "context"

// reconcileTasks drops speculative inputs invalidated by an accepted change and
// cancels callbacks whose invocation or activation no longer exists.
func reconcileTasks[S any](l *live[S], c Change[S], prepared map[taskRef]workerTask[S], running map[taskRef]context.CancelFunc) {
	for ref := range prepared {
		if !l.valid(ref) {
			delete(prepared, ref)
		}
	}
	for _, v := range c.UpsertInvocations {
		if v.GroupID != "" {
			delete(prepared, taskRef{id: v.GroupID, join: true})
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
