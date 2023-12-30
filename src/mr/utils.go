package mr

import "sync"

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
