package mr

import "sync"
import "log"

type safeCounter struct {
	mu    sync.Mutex
	value int
}

func (c *safeCounter) Increment() {
	c.mu.Lock()
	c.value++
	c.mu.Unlock()
}

func (c *safeCounter) Value() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

func makeSafeCounter(initial int) safeCounter {
	return safeCounter{value: initial}
}

type DLogger struct { *log.Logger }

const debug = false

func (logger DLogger) DPrintf(format string, a ...interface{}) {
	if debug {
		logger.Printf(format, a...)
	}
}