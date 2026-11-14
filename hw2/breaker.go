package resilience

import (
	"context"
	"errors"
	"sync"
	"time"
)

type BreakerConfig struct {
	Window    int
	Threshold float64
	Cooldown  time.Duration
	Backoff   time.Duration
}

type CircuitBreaker struct {
	config   BreakerConfig
	mu       sync.Mutex
	circuits map[string]*circuit
	changed  chan struct{}
}

type circuit struct {
	history         []bool
	index, failures int
	until           time.Time
	probing         bool
	generation      uint64
}

func NewCircuitBreaker(config BreakerConfig) (*CircuitBreaker, error) {
	switch {
	case config.Window < 1:
		return nil, invalid("Window must be positive")
	case !(config.Threshold >= 0 && config.Threshold <= 1):
		return nil, invalid("Threshold must be in [0, 1]")
	case config.Cooldown <= 0:
		return nil, invalid("Cooldown must be positive")
	case config.Backoff < 0:
		return nil, invalid("Backoff must not be negative")
	}
	return &CircuitBreaker{config: config, circuits: make(map[string]*circuit), changed: make(chan struct{})}, nil
}

func (c *circuit) acquire(now time.Time) bool {
	if c.probing || now.Before(c.until) {
		return false
	}
	c.probing = !c.until.IsZero()
	return true
}

func (c *circuit) record(config BreakerConfig, err error) bool {
	if errors.Is(err, context.Canceled) {
		wasProbe := c.probing
		c.probing = false
		return wasProbe
	}
	failed := err != nil
	if c.probing {
		generation := c.generation + 1
		*c = circuit{generation: generation}
		if failed {
			c.until = time.Now().Add(config.Cooldown)
		}
		return true
	}
	if len(c.history) < config.Window {
		c.history = append(c.history, failed)
	} else {
		if c.history[c.index] {
			c.failures--
		}
		c.history[c.index] = failed
		c.index = (c.index + 1) % config.Window
	}
	if failed {
		c.failures++
	}
	if len(c.history) == config.Window && float64(c.failures)/float64(config.Window) > config.Threshold {
		c.until = time.Now().Add(config.Cooldown)
		c.generation++
		return true
	}
	return false
}

type circuitAttempt struct {
	index      int
	circuit    *circuit
	generation uint64
}

func (breaker *CircuitBreaker) selectReplica(ctx context.Context, pool []Replica, start int) (circuitAttempt, error) {
	for {
		if err := contextError(ctx); err != nil {
			return circuitAttempt{}, err
		}
		breaker.mu.Lock()
		now := time.Now()
		var earliest time.Time
		for offset := range pool {
			i := (start + offset) % len(pool)
			id := pool[i].ID
			c := breaker.circuits[id]
			if c == nil {
				c = &circuit{}
				breaker.circuits[id] = c
			}
			if c.acquire(now) {
				attempt := circuitAttempt{i, c, c.generation}
				breaker.mu.Unlock()
				return attempt, nil
			}
			if !c.probing && (earliest.IsZero() || c.until.Before(earliest)) {
				earliest = c.until
			}
		}
		changed := breaker.changed
		breaker.mu.Unlock()
		var wake <-chan time.Time
		var timer *time.Timer
		if !earliest.IsZero() {
			timer = time.NewTimer(time.Until(earliest))
			wake = timer.C
		}
		select {
		case <-ctx.Done():
		case <-changed:
		case <-wake:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

func (breaker *CircuitBreaker) finish(attempt circuitAttempt, err error) {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	if attempt.generation == attempt.circuit.generation && attempt.circuit.record(breaker.config, err) {
		close(breaker.changed)
		breaker.changed = make(chan struct{})
	}
}

func (breaker *CircuitBreaker) ExecuteRequest(ctx context.Context, pool []Replica, b Budget) (Response, error) {
	if breaker == nil || breaker.circuits == nil {
		return Response{}, invalid("use NewCircuitBreaker")
	}
	if err := validatePool(pool); err != nil {
		return Response{}, err
	}
	var selected circuitAttempt
	nextIndex := 0
	next := func(ctx context.Context) (Sender, error) {
		attempt, err := breaker.selectReplica(ctx, pool, nextIndex)
		if err != nil {
			return nil, err
		}
		selected = attempt
		nextIndex = (attempt.index + 1) % len(pool)
		return pool[attempt.index].Sender, nil
	}
	return retry(ctx, b, breaker.config.Backoff, next, func(_ Response, err error) { breaker.finish(selected, err) })
}
