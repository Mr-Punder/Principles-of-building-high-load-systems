package resilience

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func TestMultiClientRejectsEmptyPool(t *testing.T) {
	for _, client := range []MultiClient{
		&RoundRobin{}, Hedging{}, testBreaker(t, BreakerConfig{Window: 1, Threshold: 0.5, Cooldown: time.Second}),
	} {
		response, err := client.ExecuteRequest(t.Context(), nil, budget())
		checkResult(t, response, err, 0, ErrInvalidConfig)
	}
}

func TestMultiClientCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, client := range []MultiClient{
		&RoundRobin{}, Hedging{}, testBreaker(t, BreakerConfig{Window: 1, Threshold: 0.5, Cooldown: time.Second}),
	} {
		server := makeServerMock(step{code: 200})
		response, err := client.ExecuteRequest(ctx, replicas(server), budget())
		checkResult(t, response, err, 0, context.Canceled)
		checkCalls(t, server, 0)
	}
}

func TestRoundRobinRotation(t *testing.T) {
	a := makeServerMock(step{code: 500}, step{code: 200})
	b := makeServerMock(step{code: 400})
	c := makeServerMock(step{code: 503})
	response, err := (&RoundRobin{}).ExecuteRequest(t.Context(), replicas(a, b, c), budget())
	checkResult(t, response, err, 200, nil)
	checkCalls(t, a, 2)
	checkCalls(t, b, 1)
	checkCalls(t, c, 1)
}

func TestRoundRobinStopsOnSuccess(t *testing.T) {
	a, b := makeServerMock(step{code: 200}), makeServerMock(step{code: 500})
	response, err := (&RoundRobin{}).ExecuteRequest(t.Context(), replicas(a, b), budget())
	checkResult(t, response, err, 200, nil)
	checkCalls(t, a, 1)
	checkCalls(t, b, 0)
}

func TestRoundRobinSharedBudget(t *testing.T) {
	b := budget()
	b.Failures = 2
	a, other := makeServerMock(step{code: 500}), makeServerMock(step{code: 503})
	response, err := (&RoundRobin{}).ExecuteRequest(t.Context(), replicas(a, other), b)
	checkResult(t, response, err, 503, ErrBudget)
	checkCalls(t, a, 1)
	checkCalls(t, other, 1)
}

func TestRoundRobinTimeoutAndBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := makeServerMock(step{hang: true}), makeServerMock(step{code: 200})
		start := time.Now()
		client := RoundRobin{Strategy: Backoff{Delay: 10 * time.Millisecond}}
		response, err := client.ExecuteRequest(t.Context(), replicas(a, b), budget())
		checkResult(t, response, err, 200, nil)
		checkElapsed(t, start, 110*time.Millisecond)
		checkCalls(t, a, 1)
		checkCalls(t, b, 1)
	})
}

func TestRoundRobinBackoffUsesOneAttemptBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limits := budget()
		limits.Attempts = 2
		a := makeServerMock(step{code: 500})
		b := makeServerMock(step{code: 503})
		c := makeServerMock(step{code: 200})
		client := RoundRobin{Strategy: Backoff{Delay: 10 * time.Millisecond}}
		response, err := client.ExecuteRequest(t.Context(), replicas(a, b, c), limits)
		checkResult(t, response, err, 503, ErrBudget)
		checkCalls(t, a, 1)
		checkCalls(t, b, 1)
		checkCalls(t, c, 0)
	})
}

func TestHedgingFastSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := makeServerMock(step{code: 200}), makeServerMock(step{code: 200})
		response, err := (Hedging{Delay: 10 * time.Millisecond}).ExecuteRequest(t.Context(), replicas(a, b), budget())
		checkResult(t, response, err, 200, nil)
		checkCalls(t, a, 1)
		checkCalls(t, b, 0)
	})
}

func TestHedgingParallelRequestsAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := makeServerMock(step{hang: true}), makeServerMock(step{hang: true})
		c := makeServerMock(step{code: 200, delay: 5 * time.Millisecond})
		start := time.Now()
		response, err := (Hedging{Delay: 10 * time.Millisecond}).ExecuteRequest(t.Context(), replicas(a, b, c), budget())
		checkResult(t, response, err, 200, nil)
		checkElapsed(t, start, 15*time.Millisecond)
		synctest.Wait()
		for i, sender := range []*serverMock{a, b, c} {
			checkCalls(t, sender, 1)
			wantStart := start
			if i > 0 {
				wantStart = start.Add(10 * time.Millisecond)
			}
			if !sender.calls[0].Equal(wantStart) {
				t.Fatalf("sender %d started at %v, want %v", i, sender.calls[0], wantStart)
			}
		}
		if a.canceled != 1 || b.canceled != 1 {
			t.Fatal("losing requests were not canceled")
		}
	})
}

func TestHedgingEarlyError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := makeServerMock(step{code: 400}), makeServerMock(step{code: 200})
		start := time.Now()
		response, err := (Hedging{Delay: time.Second}).ExecuteRequest(t.Context(), replicas(a, b), budget())
		checkResult(t, response, err, 200, nil)
		checkElapsed(t, start, 0)
	})
}

func TestHedgingWaitsForSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := makeServerMock(step{code: 200, delay: 30 * time.Millisecond})
		b, c := makeServerMock(step{code: 500}), makeServerMock(step{code: 400})
		start := time.Now()
		response, err := (Hedging{Delay: 10 * time.Millisecond}).ExecuteRequest(t.Context(), replicas(a, b, c), budget())
		checkResult(t, response, err, 200, nil)
		checkElapsed(t, start, 30*time.Millisecond)
	})
}

func TestHedgingAllFailuresWaitForDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := Budget{TotalTimeout: time.Second}
		a, other := makeServerMock(step{code: 500}), makeServerMock(step{code: 400})
		start := time.Now()
		response, err := (Hedging{}).ExecuteRequest(t.Context(), replicas(a, other), b)
		checkResult(t, response, err, 0, context.DeadlineExceeded)
		checkElapsed(t, start, b.TotalTimeout)
		checkCalls(t, a, 1)
		checkCalls(t, other, 1)
	})
}

func TestHedgingDeadlineBeforeDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := Budget{TotalTimeout: 10 * time.Millisecond}
		a, other := makeServerMock(step{hang: true}), makeServerMock(step{code: 200})
		response, err := (Hedging{Delay: time.Second}).ExecuteRequest(t.Context(), replicas(a, other), b)
		checkResult(t, response, err, 0, context.DeadlineExceeded)
		synctest.Wait()
		checkCalls(t, other, 0)
	})
}

func TestRoundRobinRotatesSuccessfulRequests(t *testing.T) {
	rr := &RoundRobin{}
	a, b := makeServerMock(step{code: 200}), makeServerMock(step{code: 201})
	pool := replicas(a, b)
	for _, code := range []int{200, 201, 200, 201} {
		r, err := rr.ExecuteRequest(t.Context(), pool, budget())
		checkResult(t, r, err, code, nil)
	}
}
