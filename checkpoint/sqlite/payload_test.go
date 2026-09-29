package sqlite

import "testing"

func TestPayloadBufferPoolCapacityLimit(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
		want     bool
	}{
		{name: "empty", capacity: 0, want: false},
		{name: "under limit", capacity: maxPooledPayloadCapacity - 1, want: true},
		{name: "at limit", capacity: maxPooledPayloadCapacity, want: true},
		{name: "over limit", capacity: maxPooledPayloadCapacity + 1, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buffer := make([]byte, 0, test.capacity)
			if got := payloadBufferPoolable(buffer); got != test.want {
				t.Fatalf("payloadBufferPoolable(cap=%d) = %t, want %t", test.capacity, got, test.want)
			}
		})
	}
}
