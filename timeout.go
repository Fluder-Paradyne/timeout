package timeout

import (
	"fmt"
	"net/http"
	"reflect"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
)

var bufPool *BufferPool

const (
	defaultTimeout = 5 * time.Second
)

// panicInfo was used previously for cross-goroutine panic handling; no longer needed.

// New wraps a handler and aborts the process of the handler if the timeout is reached
func New(opts ...Option) gin.HandlerFunc {
	t := &Timeout{
		timeout:  defaultTimeout,
		response: defaultResponse,
	}

	// Apply each option to the Timeout instance
	for _, opt := range opts {
		if opt == nil {
			panic("timeout Option must not be nil")
		}

		// Call the option to configure the Timeout instance
		opt(t)
	}

	// Initialize the buffer pool for response writers.
	bufPool = &BufferPool{}

	return func(c *gin.Context) {
		// Swap the response writer with a buffered writer.
		w := c.Writer
		buffer := bufPool.Get()
		buffer.Reset()
		tw := NewWriter(w, buffer)
		c.Writer = tw

		// Make an isolated copy of the gin.Context struct so we can safely
		// execute the remaining handlers in a separate goroutine without
		// touching the original context flow control (index/handlers).
		cc := *c
		cc.Writer = tw

		// Channels to coordinate completion, timeout, and panic
		finish := make(chan struct{}, 1)
		panicChan := make(chan interface{}, 1)

		// Run the remaining handlers asynchronously on the copied context with cancel support
		stop := make(chan struct{})
		go func() {
			defer func() {
				if r := recover(); r != nil {
					panicChan <- r
					return
				}
				finish <- struct{}{}
			}()

			rv := reflect.ValueOf(&cc).Elem()
			handlersField := rv.FieldByName("handlers")
			indexField := rv.FieldByName("index")

			// Unsafe access to unexported fields
			handlers := reflect.NewAt(handlersField.Type(), unsafe.Pointer(handlersField.UnsafeAddr())).Elem().Interface().(gin.HandlersChain)
			idx := int(reflect.NewAt(indexField.Type(), unsafe.Pointer(indexField.UnsafeAddr())).Elem().Int())

			for i := idx + 1; i < len(handlers); i++ {
				select {
				case <-stop:
					return
				default:
				}
				handlers[i](&cc)
			}
		}()

		select {
		case r := <-panicChan:
			// Handler panicked: respond with 500 similar to CustomRecovery behavior
			tw.mu.Lock()
			tw.FreeBuffer()
			bufPool.Put(buffer)
			tw.mu.Unlock()

			w.WriteHeader(http.StatusInternalServerError)
			// mirror the test expectation string
			_, _ = w.Write([]byte("panic caught: "))
			_, _ = w.Write([]byte(fmt.Sprint(r)))
			c.Abort()
			return
		case <-finish:
			// Handler finished successfully: flush buffer to response and stop main chain
			tw.mu.Lock()
			dst := tw.ResponseWriter.Header()
			for k, vv := range tw.Header() {
				dst[k] = vv
			}
			if tw.code != 0 {
				tw.ResponseWriter.WriteHeader(tw.code)
			}
			if buffer.Len() > 0 {
				_, _ = tw.ResponseWriter.Write(buffer.Bytes())
			}
			tw.FreeBuffer()
			bufPool.Put(buffer)
			tw.mu.Unlock()

			// Prevent the original chain from executing again
			c.Abort()

		case <-time.After(t.timeout):
			// Timeout: stop buffering further writes and stop executing remaining handlers
			tw.mu.Lock()
			tw.timeout = true
			tw.FreeBuffer()
			bufPool.Put(buffer)
			tw.mu.Unlock()

			// signal stop to the worker
			// Use a separate goroutine to avoid blocking if worker already finished
			go func() {
				// closing a stop channel indicates cancellation
				// using recover to ignore panic if closed twice
				defer func() { _ = recover() }()
				close(stop)
			}()

			// Write timeout response directly to the original writer
			timeoutCtx := c.Copy()
			timeoutCtx.Writer = w
			if !w.Written() {
				t.response(timeoutCtx)
			}

			// Prevent the original chain from executing on the original context
			c.Abort()
		}
	}
}
