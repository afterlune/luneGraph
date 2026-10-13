package executor

import (
	"sync"

	"github.com/afterlune/luneGraph/internal/limit"
)

// workerPool owns callback workers only. Admission and cancellation remain with
// the driver, which drains results before closing the pool.
type workerPool[S any] struct {
	runner  *Runner[S]
	limiter *limit.Limiter
	tasks   chan workerTask[S]
	results chan workResult[S]
	workers sync.WaitGroup
	count   int
}

func (p *workerPool[S]) dispatch(task workerTask[S], running int) {
	if running > p.count {
		p.count++
		p.workers.Add(1)
		go func() {
			defer p.workers.Done()
			for task := range p.tasks {
				out := p.runner.executeTask(task)
				limit.Release(p.limiter)
				p.results <- out
			}
		}()
	}
	p.tasks <- task
}

func (p *workerPool[S]) close() {
	close(p.tasks)
	p.workers.Wait()
}
