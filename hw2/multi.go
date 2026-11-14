package resilience

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

func validatePool(pool []Replica) error {
	if len(pool) == 0 {
		return invalid("empty replica pool")
	}
	ids := make(map[string]bool, len(pool))
	for _, r := range pool {
		if r.ID == "" {
			return invalid("empty replica ID")
		}
		if ids[r.ID] {
			return invalid("duplicate replica ID: " + r.ID)
		}
		if r.Sender == nil {
			return invalid("nil Sender for replica " + r.ID)
		}
		ids[r.ID] = true
	}
	return nil
}

type RoundRobin struct {
	Strategy Client
	next     atomic.Uint64
}

type rotatingSender struct {
	senders  []Replica
	next     *atomic.Uint64
	limit    int
	attempts atomic.Uint64
}

func (pool *rotatingSender) Send(ctx context.Context) (Response, error) {
	if err := contextError(ctx); err != nil {
		return Response{}, err
	}
	index := pool.attempts.Add(1) - 1
	if index >= uint64(pool.limit) {
		return Response{}, ErrBudget
	}
	turn := pool.next.Add(1) - 1
	return pool.senders[turn%uint64(len(pool.senders))].Send(ctx)
}

func (rr *RoundRobin) ExecuteRequest(ctx context.Context, senders []Replica, b Budget) (Response, error) {
	if err := validatePool(senders); err != nil {
		return Response{}, err
	}
	if err := b.validateRetry(0); err != nil {
		return Response{}, err
	}
	strategy := rr.Strategy
	if strategy == nil {
		strategy = Retry{}
	}
	pool := &rotatingSender{senders: senders, limit: b.Attempts, next: &rr.next}
	return strategy.ExecuteRequest(ctx, pool, b)
}

type Hedging struct {
	Delay time.Duration
}

func (hedging Hedging) ExecuteRequest(ctx context.Context, senders []Replica, b Budget) (Response, error) {
	if err := validatePool(senders); err != nil {
		return Response{}, err
	}
	if err := b.validateTime(); err != nil {
		return Response{}, err
	}
	if hedging.Delay < 0 {
		return Response{}, invalid("hedging Delay must not be negative")
	}
	ctx, cancel := context.WithTimeout(ctx, b.TotalTimeout)
	defer cancel()
	answers := make(chan result, len(senders))
	start := func(sender Sender) {
		go func() {
			response, err := (Default{}).ExecuteRequest(ctx, sender, b)
			answers <- result{response, err}
		}()
	}
	var lastErr error
	start(senders[0])
	timer := time.NewTimer(hedging.Delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return Response{}, errors.Join(ctx.Err(), lastErr)
	case answer := <-answers:
		lastErr = answer.err
		if answer.err == nil && contextError(ctx) == nil {
			return answer.response, nil
		}
	case <-timer.C:
	}
	for _, sender := range senders[1:] {
		start(sender)
	}
	for {
		select {
		case <-ctx.Done():
			return Response{}, errors.Join(ctx.Err(), lastErr)
		case answer := <-answers:
			lastErr = answer.err
			if answer.err == nil && contextError(ctx) == nil {
				return answer.response, nil
			}
		}
	}
}
