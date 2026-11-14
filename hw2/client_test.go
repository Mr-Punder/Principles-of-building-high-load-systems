package resilience

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"testing/synctest"
	"time"
)

func budget() Budget {
	return Budget{
		Attempts: 10, FastErrors: 10, Failures: 10, Timeouts: 10,
		TotalTimeout: time.Second, AttemptTimeout: 100 * time.Millisecond,
	}
}

func checkResult(t *testing.T, response Response, err error, code int, wantErr error) {
	t.Helper()
	if response.StatusCode != code || !errors.Is(err, wantErr) {
		t.Fatalf("got (%+v, %v), want status %d and error %v", response, err, code, wantErr)
	}
}

func checkCalls(t *testing.T, sender *serverMock, count int) {
	t.Helper()
	sender.mu.Lock()
	defer sender.mu.Unlock()
	if len(sender.calls) != count {
		t.Fatalf("got %d calls, want %d", len(sender.calls), count)
	}
}

func checkElapsed(t *testing.T, start time.Time, want time.Duration) {
	t.Helper()
	if elapsed := time.Since(start); elapsed != want {
		t.Fatalf("elapsed %v, want %v", elapsed, want)
	}
}

func TestDefaultResponses(t *testing.T) {
	for _, code := range []int{200, 204, 400, 503} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			sender := makeServerMock(step{code: code})
			response, err := (Default{}).ExecuteRequest(t.Context(), sender, budget())
			var wantErr error
			if code >= 400 {
				wantErr = ErrResponse
			}
			checkResult(t, response, err, code, wantErr)
			if response.Body != "payload" {
				t.Fatal("response body was lost")
			}
			checkCalls(t, sender, 1)
		})
	}
}

func TestDefaultTimeouts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		latency time.Duration
		attempt time.Duration
		want    time.Duration
	}{
		{"attempt", time.Second, 10 * time.Millisecond, 10 * time.Millisecond},
		{"total", 10 * time.Millisecond, time.Second, 10 * time.Millisecond},
		{"no_attempt_limit", 10 * time.Millisecond, 0, 10 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sender := makeServerMock(step{hang: true})
				start := time.Now()
				response, err := (Default{}).ExecuteRequest(t.Context(), sender, Budget{TotalTimeout: tc.latency, AttemptTimeout: tc.attempt})
				checkResult(t, response, err, 0, context.DeadlineExceeded)
				checkElapsed(t, start, tc.want)
				synctest.Wait()
				if sender.canceled != 1 {
					t.Fatal("sender was not canceled")
				}
			})
		})
	}
}

func TestDefaultCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	sender := makeServerMock(step{code: 200})
	response, err := (Default{}).ExecuteRequest(ctx, sender, budget())
	checkResult(t, response, err, 0, context.Canceled)
	checkCalls(t, sender, 0)
}

func TestDefaultStopsWaitingForUncooperativeSender(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sender := makeServerMock(step{code: 200, delay: time.Second, ignoreCancel: true})
		start := time.Now()
		response, err := (Default{}).ExecuteRequest(t.Context(), sender, Budget{TotalTimeout: 10 * time.Millisecond})
		checkResult(t, response, err, 0, context.DeadlineExceeded)
		checkElapsed(t, start, 10*time.Millisecond)
		time.Sleep(time.Second)
		synctest.Wait()
	})
}

func TestRetryMixedOutcomes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sender := makeServerMock(step{code: 500}, step{code: 400}, step{hang: true}, step{code: 200})
		b := budget()
		b.FastErrors, b.Failures, b.Timeouts = 2, 2, 2
		response, err := (Retry{}).ExecuteRequest(t.Context(), sender, b)
		checkResult(t, response, err, 200, nil)
		checkCalls(t, sender, 4)
	})
}

func TestRetryBudgets(t *testing.T) {
	for _, tc := range []struct {
		name  string
		step  step
		limit func(*Budget)
	}{
		{"attempts", step{code: 500}, func(b *Budget) { b.Attempts = 2 }},
		{"fast_errors", step{code: 400}, func(b *Budget) { b.FastErrors = 2 }},
		{"failures", step{code: 500}, func(b *Budget) { b.Failures = 2 }},
		{"timeouts", step{hang: true}, func(b *Budget) { b.Timeouts = 2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := budget()
				tc.limit(&b)
				sender := makeServerMock(tc.step)
				response, err := (Retry{}).ExecuteRequest(t.Context(), sender, b)
				checkResult(t, response, err, tc.step.code, ErrBudget)
				checkCalls(t, sender, 2)
			})
		})
	}
}

func TestRetryZeroErrorBudget(t *testing.T) {
	b := budget()
	b.Failures = 0
	sender := makeServerMock(step{code: 500}, step{code: 200})
	response, err := (Retry{}).ExecuteRequest(t.Context(), sender, b)
	checkResult(t, response, err, 500, ErrBudget)
	checkCalls(t, sender, 1)
}

func TestRetryTotalDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := budget()
		b.TotalTimeout = 150 * time.Millisecond
		sender := makeServerMock(step{hang: true})
		start := time.Now()
		response, err := (Retry{}).ExecuteRequest(t.Context(), sender, b)
		checkResult(t, response, err, 0, context.DeadlineExceeded)
		checkElapsed(t, start, b.TotalTimeout)
		checkCalls(t, sender, 2)
	})
}

func TestBackoffDelays(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := budget()
		b.AttemptTimeout = 5 * time.Millisecond
		sender := makeServerMock(step{code: 500}, step{code: 400}, step{hang: true}, step{code: 503}, step{code: 200})
		start := time.Now()
		response, err := (Backoff{Delay: 10 * time.Millisecond}).ExecuteRequest(t.Context(), sender, b)
		checkResult(t, response, err, 200, nil)
		checkCalls(t, sender, 5)
		for i, ms := range []int{0, 10, 10, 35, 75} {
			if got := sender.calls[i].Sub(start); got != time.Duration(ms)*time.Millisecond {
				t.Fatalf("call %d started at %v, want %dms", i, got, ms)
			}
		}
	})
}

func TestBackoffFastErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sender := makeServerMock(step{code: 400}, step{code: 401}, step{code: 200})
		start := time.Now()
		response, err := (Backoff{Delay: time.Second}).ExecuteRequest(t.Context(), sender, budget())
		checkResult(t, response, err, 200, nil)
		checkElapsed(t, start, 0)
	})
}

func TestBackoffDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sender := makeServerMock(step{code: 500})
		start := time.Now()
		response, err := (Backoff{Delay: time.Hour}).ExecuteRequest(t.Context(), sender, budget())
		checkResult(t, response, err, 0, context.DeadlineExceeded)
		checkElapsed(t, start, time.Second)
		checkCalls(t, sender, 1)
	})
}

func TestBackoffDoesNotWaitAfterLastAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := budget()
		b.Attempts = 1
		start := time.Now()
		response, err := (Backoff{Delay: time.Second}).ExecuteRequest(t.Context(), makeServerMock(step{code: 500}), b)
		checkResult(t, response, err, 500, ErrBudget)
		checkElapsed(t, start, 0)
	})
}

func TestBackoffCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		time.AfterFunc(10*time.Millisecond, cancel)
		sender := makeServerMock(step{code: 500})
		start := time.Now()
		response, err := (Backoff{Delay: time.Second}).ExecuteRequest(ctx, sender, budget())
		checkResult(t, response, err, 0, context.Canceled)
		checkElapsed(t, start, 10*time.Millisecond)
		checkCalls(t, sender, 1)
	})
}

func TestRetryKeepsLastAttemptError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limits := budget()
		limits.Attempts = 1
		_, err := (Retry{}).ExecuteRequest(t.Context(), makeServerMock(step{hang: true}), limits)
		if !errors.Is(err, ErrBudget) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("lost timeout or budget error: %v", err)
		}
	})
}

func TestLimitInFlightHoldsSlotUntilSendReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := makeServerMock(step{code: 200, delay: time.Second, ignoreCancel: true}, step{code: 201})
		limited, err := LimitInFlight(server, 1)
		if err != nil {
			t.Fatal(err)
		}
		limits := Budget{TotalTimeout: time.Millisecond}
		for range 2 {
			r, err := (Default{}).ExecuteRequest(t.Context(), limited, limits)
			checkResult(t, r, err, 0, context.DeadlineExceeded)
		}
		checkCalls(t, server, 1)
		time.Sleep(time.Second)
		r, err := (Default{}).ExecuteRequest(t.Context(), limited, limits)
		checkResult(t, r, err, 201, nil)
		checkCalls(t, server, 2)
	})
}
