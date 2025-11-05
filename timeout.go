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
		hardStop: true,
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

		// Channels to coordinate completion, timeout, and panic
		finish := make(chan struct{}, 1)
		panicChan := make(chan panicInfo, 1)

		if t.hardStop {
			// Hard stop path using reflection/unsafe to step remaining handlers and cancel on timeout.
			cc := *c
			cc.Writer = tw

			stop := make(chan struct{})
			go func() {
				defer func() {
					if r := recover(); r != nil {
						panicChan <- panicInfo{Value: r, Stack: debug.Stack()}
						return
					}
					finish <- struct{}{}
				}()

				rv := reflect.ValueOf(&cc).Elem()
				handlersField := rv.FieldByName("handlers")
				indexField := rv.FieldByName("index")

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
			case pi := <-panicChan:
				tw.mu.Lock()
				tw.FreeBuffer()
				bufPool.Put(buffer)
				tw.mu.Unlock()

				if gin.IsDebugging() {
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte("panic caught: "))
					_, _ = w.Write([]byte(fmt.Sprint(pi.Value)))
					_, _ = w.Write([]byte("\n"))
					_, _ = w.Write([]byte("Panic stack trace:\n"))
					_, _ = w.Write(pi.Stack)
					c.Abort()
					return
				}
				panic(pi.Value)
			case <-finish:
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

				c.Abort()
			case <-time.After(t.timeout):
				tw.mu.Lock()
				tw.timeout = true
				tw.FreeBuffer()
				bufPool.Put(buffer)
				tw.mu.Unlock()

				go func() {
					defer func() { _ = recover() }()
					close(stop)
				}()

				timeoutCtx := c.Copy()
				timeoutCtx.Writer = w
				if !w.Written() {
					t.response(timeoutCtx)
				}
				c.Abort()
			}
			return
		}

		// Soft timeout path: run remaining handlers using copied context; no cancellation; drop late writes.
		cc := *c
		cc.Writer = tw
		go func() {
			defer func() {
				if r := recover(); r != nil {
					panicChan <- panicInfo{Value: r, Stack: debug.Stack()}
					return
				}
				finish <- struct{}{}
			}()
			rv := reflect.ValueOf(&cc).Elem()
			handlersField := rv.FieldByName("handlers")
			indexField := rv.FieldByName("index")

			handlers := reflect.NewAt(handlersField.Type(), unsafe.Pointer(handlersField.UnsafeAddr())).Elem().Interface().(gin.HandlersChain)
			idx := int(reflect.NewAt(indexField.Type(), unsafe.Pointer(indexField.UnsafeAddr())).Elem().Int())

			for i := idx + 1; i < len(handlers); i++ {
				handlers[i](&cc)
			}
		}()

		select {
		case pi := <-panicChan:
			tw.mu.Lock()
			tw.FreeBuffer()
			bufPool.Put(buffer)
			tw.mu.Unlock()

			if gin.IsDebugging() {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("panic caught: "))
				_, _ = w.Write([]byte(fmt.Sprint(pi.Value)))
				_, _ = w.Write([]byte("\n"))
				_, _ = w.Write([]byte("Panic stack trace:\n"))
				_, _ = w.Write(pi.Stack)
				return
			}
			panic(pi.Value)
		case <-finish:
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
		case <-time.After(t.timeout):
			tw.mu.Lock()
			tw.timeout = true
			tw.FreeBuffer()
			bufPool.Put(buffer)
			tw.mu.Unlock()

			timeoutCtx := c.Copy()
			timeoutCtx.Writer = w
			if !w.Written() {
				t.response(timeoutCtx)
			}
			// No abort in soft mode.
		}
	}
}
