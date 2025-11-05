package timeout

import (
	"fmt"
	"net/http"
	"reflect"
	"runtime/debug"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
)

var bufPool *BufferPool

const (
	defaultTimeout = 5 * time.Second
)

// panicInfo transmits both the panic value and the stack trace when a handler panics.
type panicInfo struct {
	Value interface{}
	Stack []byte
}

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

		// Make a safe copy of the gin.Context for execution in another goroutine
		// without copying the internal locks.
		cc := c.Copy()
		cc.Writer = tw
		// Retrieve handlers and current index from the original context, and
		// reattach them to the copied context so handler stepping works.
		origRV := reflect.ValueOf(c).Elem()
		origHandlersField := origRV.FieldByName("handlers")
		origIndexField := origRV.FieldByName("index")
		handlers := reflect.NewAt(origHandlersField.Type(), unsafe.Pointer(origHandlersField.UnsafeAddr())).Elem().Interface().(gin.HandlersChain)
		idx := int(reflect.NewAt(origIndexField.Type(), unsafe.Pointer(origIndexField.UnsafeAddr())).Elem().Int())
		ccRV := reflect.ValueOf(cc).Elem()
		ccHandlersField := ccRV.FieldByName("handlers")
		ccIndexField := ccRV.FieldByName("index")
		reflect.NewAt(ccHandlersField.Type(), unsafe.Pointer(ccHandlersField.UnsafeAddr())).Elem().Set(reflect.ValueOf(handlers))
		reflect.NewAt(ccIndexField.Type(), unsafe.Pointer(ccIndexField.UnsafeAddr())).Elem().SetInt(int64(idx))

		// Channels to coordinate completion, timeout, and panic
		finish := make(chan struct{}, 1)
		panicChan := make(chan panicInfo, 1)

		// Run the remaining handlers asynchronously on the copied context with cancel support
		stop := make(chan struct{})
		go func() {
			defer func() {
				if r := recover(); r != nil {
					panicChan <- panicInfo{Value: r, Stack: debug.Stack()}
					return
				}
				finish <- struct{}{}
			}()

			for i := idx + 1; i < len(handlers); i++ {
				select {
				case <-stop:
					return
				default:
				}
				handlers[i](cc)
			}
		}()

		select {
		case pi := <-panicChan:
			// Handler panicked
			tw.mu.Lock()
			tw.FreeBuffer()
			bufPool.Put(buffer)
			tw.mu.Unlock()

			if gin.IsDebugging() {
				// In debug mode, include stack trace for easier debugging
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("panic caught: "))
				_, _ = w.Write([]byte(fmt.Sprint(pi.Value)))
				_, _ = w.Write([]byte("\n"))
				_, _ = w.Write([]byte("Panic stack trace:\n"))
				_, _ = w.Write(pi.Stack)
				c.Abort()
				return
			}
			// In non-debug mode, rethrow for upstream recovery middleware
			panic(pi.Value)
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
