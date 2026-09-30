package capacitytest

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

const latencyWindow = 1024

type resourceSample struct {
	HeapBytes  uint64 `json:"heap_bytes"`
	Goroutines int    `json:"goroutines"`
}

func resources(gc bool) resourceSample {
	if gc {
		runtime.GC()
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return resourceSample{HeapBytes: m.HeapAlloc, Goroutines: runtime.NumGoroutine()}
}

type measurement struct {
	mu                      sync.Mutex
	latencies               [latencyWindow]time.Duration
	next, size              int
	calls, cancelled, steps uint64
	peak                    resourceSample
}

type report struct {
	Phase          string         `json:"phase"`
	Seconds        float64        `json:"seconds"`
	Calls          uint64         `json:"successful_calls"`
	Cancelled      uint64         `json:"cancelled_calls"`
	Steps          uint64         `json:"successful_call_steps"`
	CallsPerSecond float64        `json:"calls_per_second"`
	LatencySamples int            `json:"rolling_latency_samples"`
	P50MS          float64        `json:"rolling_p50_ms"`
	P95MS          float64        `json:"rolling_p95_ms"`
	Current        resourceSample `json:"current"`
	SampledPeak    resourceSample `json:"sampled_peak"`
}

func (m *measurement) record(elapsed time.Duration, cancelled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cancelled {
		m.cancelled++
		return
	}
	m.calls++
	m.steps += 10
	m.latencies[m.next] = elapsed
	m.next = (m.next + 1) % latencyWindow
	if m.size < latencyWindow {
		m.size++
	}
}

func (m *measurement) snapshot(phase string, elapsed time.Duration, current resourceSample) report {
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
	r := report{Phase: phase, Seconds: elapsed.Seconds(), Calls: m.calls, Cancelled: m.cancelled, Steps: m.steps, LatencySamples: len(values), Current: current, SampledPeak: m.peak}
	if elapsed > 0 {
		r.CallsPerSecond = float64(m.calls) / elapsed.Seconds()
	}
	if len(values) > 0 {
		r.P50MS = float64(values[(len(values)-1)*50/100]) / float64(time.Millisecond)
		r.P95MS = float64(values[(len(values)-1)*95/100]) / float64(time.Millisecond)
	}
	return r
}

func soakDuration(raw string) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("LUNEGRAPH_SOAK_DURATION must be a positive Go duration, got %q", raw)
	}
	return d, nil
}

func TestBoundedMeasurements(t *testing.T) {
	var m measurement
	for i := 1; i <= 2048; i++ {
		m.record(time.Duration(i)*time.Millisecond, false)
	}
	m.record(time.Second, true)
	r := m.snapshot("check", time.Second, resourceSample{100, 7})
	if r.Calls != 2048 || r.Steps != 20480 || r.Cancelled != 1 || r.LatencySamples != 1024 || r.P50MS != 1536 || r.P95MS != 1996 || r.SampledPeak.HeapBytes != 100 {
		t.Fatalf("measurement = %+v", r)
	}
	r = m.snapshot("check", 2*time.Second, resourceSample{50, 3})
	if r.SampledPeak.HeapBytes != 100 || r.SampledPeak.Goroutines != 7 || r.CallsPerSecond != 1024 {
		t.Fatalf("measurement = %+v", r)
	}
	for _, raw := range []string{"bad", "0s", "-1s"} {
		if _, err := soakDuration(raw); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
	for _, raw := range []string{"", "60s"} {
		if _, err := soakDuration(raw); err != nil {
			t.Fatal(err)
		}
	}
}
