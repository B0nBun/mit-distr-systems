package raft

import (
	"time"
	"math/rand"
)

func minimum(n int, ints ...int) (min int) {
	min = n
	for _, num := range ints {
		if num < min {
			min = num
		}
	}
	return min
}

func maximum(n int, ints ...int) (max int) {
	max = n
	for _, num := range ints {
		if num > max {
			max = num
		}
	}
	return max
}

type RandomTicker struct {
	C       chan time.Time
	min     time.Duration
	max     time.Duration
	stopc   chan struct{}
	resetc  chan struct{}
}

func NewRandomTicker(min, max time.Duration) *RandomTicker {
	rt := &RandomTicker{
		C:       make(chan time.Time),
		min:     min,
		max:     max,
		stopc:   make(chan struct{}),
		resetc:  make(chan struct{}),
	}
	go rt.loop()
	return rt
}

// NOTE: Without rt.C reciever, this loop won't emit any ticks
func (rt *RandomTicker) loop() {
	for {
		timer := time.NewTimer(rt.nextInterval())
		select {
		case <-rt.stopc:
			return
		case <-rt.resetc:
			// pass
		case <-timer.C:
			rt.C <- time.Now()
		}
		timer.Stop()
	}
}

func (rt *RandomTicker) Reset() (stopped bool) {
	select {
	case <-rt.stopc:
		return true
	default:
		rt.resetc <- struct{}{}
		return false
	}
}

func (rt *RandomTicker) nextInterval() time.Duration {
	diff := (rt.max - rt.min).Nanoseconds()
	interval := rt.min.Nanoseconds() + rand.Int63n(diff)
	return time.Duration(interval) * time.Nanosecond
}

func (rt *RandomTicker) Stop() {
	close(rt.stopc)
}