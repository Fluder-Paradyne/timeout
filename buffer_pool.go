package timeout

import (
	"bytes"
	"sync"
)

// BufferPool represents a pool of buffers.
// It uses sync.Pool to manage the reuse of buffers, reducing memory allocation and garbage collection overhead.
type BufferPool struct {
	pool sync.Pool
	mu   sync.Mutex
	last *bytes.Buffer
}

// Get returns a buffer from the buffer pool.
// If the pool is empty, a new buffer is created and returned.
// This method ensures the reuse of buffers, improving performance.
func (p *BufferPool) Get() *bytes.Buffer {
	p.mu.Lock()
	if p.last != nil {
		b := p.last
		p.last = nil
		p.mu.Unlock()
		return b
	}
	p.mu.Unlock()

	buf := p.pool.Get()
	if buf == nil {
		return &bytes.Buffer{}
	}
	return buf.(*bytes.Buffer)
}

// Put adds a buffer back to the pool.
// This method allows the buffer to be reused in the future, reducing the number of memory allocations.
func (p *BufferPool) Put(buf *bytes.Buffer) {
	p.mu.Lock()
	if p.last == nil {
		p.last = buf
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	p.pool.Put(buf)
}
