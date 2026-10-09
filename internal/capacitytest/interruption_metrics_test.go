package capacitytest

import (
	"sort"
	"sync"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
)

// Keep the existing successful_calls metrics unchanged. This workload reports
// interrupted calls separately and times all non-cancelled expected outcomes.
type interruptionMeasurement struct {
	mu                                    sync.Mutex
	normal, interrupted, cancelled, steps uint64
	latencies                             [latencyWindow]time.Duration
	next, size                            int
	peak                                  resourceSample
}

type interruptionReport struct {
	Phase          string         `json:"phase"`
	Seconds        float64        `json:"seconds"`
	Normal         uint64         `json:"normal_calls"`
	Interrupted    uint64         `json:"interrupted_calls"`
	Cancelled      uint64         `json:"cancelled_calls"`
	Steps          uint64         `json:"observed_committed_steps"`
	CallsPerSecond float64        `json:"noncancelled_calls_per_second"`
	Samples        int            `json:"rolling_latency_samples"`
	P50MS          float64        `json:"rolling_noncancelled_p50_ms"`
	P95MS          float64        `json:"rolling_noncancelled_p95_ms"`
	Current        resourceSample `json:"current"`
	Peak           resourceSample `json:"sampled_peak"`
}

func (m *interruptionMeasurement) record(elapsed time.Duration, status graph.Status, steps uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.steps += steps
	switch status {
	case graph.StatusInterrupted:
		m.interrupted++
	case graph.StatusCancelled:
		m.cancelled++
		return
	default:
		m.normal++
	}
	m.latencies[m.next] = elapsed
	m.next = (m.next + 1) % latencyWindow
	if m.size < latencyWindow {
		m.size++
	}
}

func (m *interruptionMeasurement) snapshot(phase string, elapsed time.Duration, current resourceSample) interruptionReport {
	m.mu.Lock()
	defer m.mu.Unlock()
	if current.HeapBytes > m.peak.HeapBytes {
		m.peak.HeapBytes = current.HeapBytes
	}
	if current.Goroutines > m.peak.Goroutines {
		m.peak.Goroutines = current.Goroutines
	}
	values := append([]time.Duration(nil), m.latencies[:m.size]...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	r := interruptionReport{Phase: phase, Seconds: elapsed.Seconds(), Normal: m.normal, Interrupted: m.interrupted, Cancelled: m.cancelled, Steps: m.steps, Samples: len(values), Current: current, Peak: m.peak}
	if elapsed > 0 {
		r.CallsPerSecond = float64(m.normal+m.interrupted) / elapsed.Seconds()
	}
	if len(values) > 0 {
		r.P50MS = float64(values[(len(values)-1)*50/100]) / float64(time.Millisecond)
		r.P95MS = float64(values[(len(values)-1)*95/100]) / float64(time.Millisecond)
	}
	return r
}

func TestCapacityInterruptionMeasurements(t *testing.T) {
	var m interruptionMeasurement
	for i := 1; i <= 2048; i++ {
		status := graph.StatusInterrupted
		if i%2 == 0 {
			status = graph.StatusWaiting
		}
		m.record(time.Duration(i)*time.Millisecond, status, uint64(i%3))
	}
	m.record(time.Second, graph.StatusCancelled, 2)
	r := m.snapshot("drain", time.Second, resourceSample{100, 7})
	if r.Normal != 1024 || r.Interrupted != 1024 || r.Cancelled != 1 || r.Samples != 1024 || r.P50MS != 1536 || r.P95MS != 1996 || r.Steps != 2051 || r.CallsPerSecond != 2048 {
		t.Fatalf("report=%+v", r)
	}
	r = m.snapshot("recovery", time.Second, resourceSample{50, 3})
	if r.Peak.HeapBytes != 100 || r.Peak.Goroutines != 7 {
		t.Fatalf("lost peak=%+v", r)
	}
}
