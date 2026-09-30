package sqlite

import graph "github.com/afterlune/luneGraph"

const maxPooledPayloadCapacity = 1 << 20

type payloadBuffer struct {
	data []byte
}

func (s *Store[S]) encodePayload(value graph.Checkpoint[S]) (*payloadBuffer, error) {
	buffer, ok := s.payloadBuffers.Get().(*payloadBuffer)
	if !ok {
		buffer = &payloadBuffer{}
	}
	payload, err := s.codec.Append(buffer.data[:0], value)
	if err != nil {
		if payload != nil {
			buffer.data = payload
		}
		s.releasePayloadBuffer(buffer)
		return nil, err
	}
	buffer.data = payload
	return buffer, nil
}

func (s *Store[S]) releasePayloadBuffer(buffer *payloadBuffer) {
	if buffer != nil && payloadBufferPoolable(buffer.data) {
		buffer.data = buffer.data[:0]
		s.payloadBuffers.Put(buffer)
	}
}

func payloadBufferPoolable(payload []byte) bool {
	return cap(payload) > 0 && cap(payload) <= maxPooledPayloadCapacity
}
