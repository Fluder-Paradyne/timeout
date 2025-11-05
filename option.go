package timeout

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Option for timeout
type Option func(*Timeout)

// WithTimeout set timeout
func WithTimeout(timeout time.Duration) Option {
	return func(t *Timeout) {
		t.timeout = timeout
	}
}

// WithResponse add gin handler
func WithResponse(h gin.HandlerFunc) Option {
	return func(t *Timeout) {
		t.response = h
	}
}

// WithHardStop controls whether the middleware attempts to stop downstream
// handlers from running after the timeout is reached.
//
// When set to true (default), the middleware will try to prevent further
// handler execution after a timeout.
// When set to false (soft timeout), the middleware will return the timeout
// response, but allow the handler goroutine to continue; late writes are
// dropped by the buffered writer.
func WithHardStop(hard bool) Option {
	return func(t *Timeout) {
		t.hardStop = hard
	}
}

// WithSoftTimeout is a helper that enables soft timeout behavior.
func WithSoftTimeout() Option { return WithHardStop(false) }

func defaultResponse(c *gin.Context) {
	c.String(http.StatusRequestTimeout, http.StatusText(http.StatusRequestTimeout))
}

// Timeout struct
type Timeout struct {
	timeout  time.Duration
	response gin.HandlerFunc
	hardStop bool
}
