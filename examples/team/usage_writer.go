package main

import (
	"io"
	"sync"
)

type serializedWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

func newSerializedWriter(writer io.Writer) io.Writer {
	if writer == nil {
		return nil
	}
	return &serializedWriter{writer: writer}
}

func (w *serializedWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writer.Write(data)
}
