package resilience

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestCircuitBreakerAllOpenDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		breaker := testBreaker(t, BreakerConfig{Window: 1, Threshold: 0.5, Cooldown: time.Second})
		a := makeServerMock(step{code: 500})
		b := makeServerMock(step{code: 503}, step{code: 200})
		start := time.Now()
		response, err := breaker.ExecuteRequest(t.Context(), replicas(a, b), budget())
		checkResult(t, response, err, 0, context.DeadlineExceeded)
		checkElapsed(t, start, time.Second)
		checkCalls(t, a, 1)
		checkCalls(t, b, 1)
	})
}

func TestCircuitBreakerRemembersServers(t *testing.T) {
	breaker := testBreaker(t, BreakerConfig{Window: 1, Threshold: 0.5, Cooldown: time.Hour})
	a, b := makeServerMock(step{code: 500}), makeServerMock(step{code: 200})
	response, err := breaker.ExecuteRequest(t.Context(), replicas(a, b), budget())
	checkResult(t, response, err, 200, nil)
	a, b = makeServerMock(step{code: 200}), makeServerMock(step{code: 200})
	response, err = breaker.ExecuteRequest(t.Context(), replicas(a, b), budget())
	checkResult(t, response, err, 200, nil)
	checkCalls(t, a, 0)
	checkCalls(t, b, 1)
}

func TestCircuitBreakerSlidingWindowAndThreshold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		breaker := testBreaker(t, BreakerConfig{Window: 2, Threshold: 0.5, Cooldown: time.Hour})
		b := budget()
		b.Attempts = 1
		for _, code := range []int{200, 500, 200, 500, 500} {
			sender := makeServerMock(step{code: code})
			response, err := breaker.ExecuteRequest(t.Context(), replicas(sender), b)
			var wantErr error
			if code == 500 {
				wantErr = ErrBudget
			}
			checkResult(t, response, err, code, wantErr)
			checkCalls(t, sender, 1)
		}
		sender := makeServerMock(step{code: 200})
		response, err := breaker.ExecuteRequest(t.Context(), replicas(sender), b)
		checkResult(t, response, err, 0, context.DeadlineExceeded)
		checkCalls(t, sender, 0)
	})
}

func TestCircuitBreakerRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		breaker := testBreaker(t, BreakerConfig{Window: 1, Threshold: 0.5, Cooldown: 20 * time.Millisecond})
		sender := makeServerMock(step{code: 500}, step{code: 200})
		start := time.Now()
		response, err := breaker.ExecuteRequest(t.Context(), replicas(sender), budget())
		checkResult(t, response, err, 200, nil)
		checkElapsed(t, start, 20*time.Millisecond)
		checkCalls(t, sender, 2)
		recovered := makeServerMock(step{code: 200})
		response, err = breaker.ExecuteRequest(t.Context(), replicas(recovered), budget())
		checkResult(t, response, err, 200, nil)
		checkElapsed(t, start, 20*time.Millisecond)
	})
}

func TestCircuitBreakerCountsFastErrorsAndTimeouts(t *testing.T) {
	for _, tc := range []struct {
		name string
		step step
	}{
		{"fast_error", step{code: 400}},
		{"timeout", step{hang: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				breaker := testBreaker(t, BreakerConfig{Window: 1, Threshold: 0.5, Cooldown: time.Second})
				a, b := makeServerMock(tc.step), makeServerMock(step{code: 200})
				response, err := breaker.ExecuteRequest(t.Context(), replicas(a, b), budget())
				checkResult(t, response, err, 200, nil)
				a, b = makeServerMock(step{code: 200}), makeServerMock(step{code: 200})
				response, err = breaker.ExecuteRequest(t.Context(), replicas(a, b), budget())
				checkResult(t, response, err, 200, nil)
				checkCalls(t, a, 0)
				checkCalls(t, b, 1)
			})
		})
	}
}

func TestCircuitBreakerConcurrentCalls(t *testing.T) {
	breaker := testBreaker(t, BreakerConfig{Window: 4, Threshold: 1, Cooldown: time.Second})
	b := budget()
	b.Attempts = 1
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code := 200
			var wantErr error
			if i%2 == 0 {
				code, wantErr = 500, ErrBudget
			}
			response, err := breaker.ExecuteRequest(t.Context(), replicas(makeServerMock(step{code: code})), b)
			if response.StatusCode != code || !errors.Is(err, wantErr) {
				t.Errorf("got (%+v, %v), want status %d and error %v", response, err, code, wantErr)
			}
		}()
	}
	wg.Wait()
}

func TestBreakerTracksReplicaID(t *testing.T) {
	breaker := testBreaker(t, BreakerConfig{Window: 1, Threshold: 0.5, Cooldown: time.Hour})
	bad := makeServerMock(step{code: 500})
	good := makeServerMock(step{code: 200})
	pool := []Replica{{"A", bad}, {"B", good}}
	response, err := breaker.ExecuteRequest(t.Context(), pool, budget())
	checkResult(t, response, err, 200, nil)
	pool[0], pool[1] = pool[1], pool[0]
	response, err = breaker.ExecuteRequest(t.Context(), pool, budget())
	checkResult(t, response, err, 200, nil)
	checkCalls(t, bad, 1)
	checkCalls(t, good, 2)
	replacement := makeServerMock(step{code: 201})
	response, err = breaker.ExecuteRequest(t.Context(), []Replica{{"C", replacement}}, budget())
	checkResult(t, response, err, 201, nil)
}

func TestBreakerCountsDeadlineOfActiveAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		breaker := testBreaker(t, BreakerConfig{Window: 1, Threshold: 0.5, Cooldown: time.Hour})
		limits := budget()
		limits.AttemptTimeout = 0
		bad, good := makeServerMock(step{hang: true}), makeServerMock(step{code: 200})
		pool := replicas(bad, good)
		r, err := breaker.ExecuteRequest(t.Context(), pool, limits)
		checkResult(t, r, err, 0, context.DeadlineExceeded)
		r, err = breaker.ExecuteRequest(t.Context(), pool, limits)
		checkResult(t, r, err, 200, nil)
		checkCalls(t, bad, 1)
	})
}

func TestBreakerAllowsOneRecoveryProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		breaker := testBreaker(t, BreakerConfig{Window: 1, Threshold: 0.5, Cooldown: time.Second})
		limits := budget()
		limits.Attempts = 1
		server := makeServerMock(step{code: 500}, step{code: 200, delay: 10 * time.Millisecond})
		pool := replicas(server)
		r, err := breaker.ExecuteRequest(t.Context(), pool, limits)
		checkResult(t, r, err, 500, ErrBudget)
		time.Sleep(time.Second)
		answers := make(chan result, 3)
		for range 3 {
			go func() {
				r, err := breaker.ExecuteRequest(t.Context(), pool, limits)
				answers <- result{r, err}
			}()
		}
		synctest.Wait()
		checkCalls(t, server, 2)
		for range 3 {
			answer := <-answers
			checkResult(t, answer.response, answer.err, 200, nil)
		}
		checkCalls(t, server, 4)
	})
}
