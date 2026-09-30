package capacitytest

import "sync"

// Each batch addresses every slot once and drains before another batch starts.
// A slot is never advanced concurrently with itself.
type batchPool struct {
	jobs    chan int
	results chan error
	workers sync.WaitGroup
	count   int
}

func newBatchPool(count, workers int, call func(int) error) *batchPool {
	p := &batchPool{jobs: make(chan int, count), results: make(chan error, count), count: count}
	for range workers {
		p.workers.Go(func() {
			for i := range p.jobs {
				p.results <- call(i)
			}
		})
	}
	return p
}

func (p *batchPool) batch() error {
	for i := range p.count {
		p.jobs <- i
	}
	var first error
	for range p.count {
		if err := <-p.results; err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (p *batchPool) close() { close(p.jobs); p.workers.Wait() }
