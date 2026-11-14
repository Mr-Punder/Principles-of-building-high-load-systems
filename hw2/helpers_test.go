package resilience

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
)

type step struct {
	code         int
	delay        time.Duration
	hang         bool
	ignoreCancel bool
}

type serverMock struct {
	mu       sync.Mutex
	steps    []step
	calls    []time.Time
	canceled int
}

func makeServerMock(steps ...step) *serverMock {
	return &serverMock{steps: steps}
}

func (server *serverMock) Send(ctx context.Context) (Response, error) {
	server.mu.Lock()
	i := len(server.calls)
	server.calls = append(server.calls, time.Now())
	s := server.steps[min(i, len(server.steps)-1)]
	server.mu.Unlock()
	var err error
	if s.hang {
		<-ctx.Done()
		err = ctx.Err()
	} else if s.ignoreCancel {
		time.Sleep(s.delay)
	} else if s.delay > 0 {
		err = wait(ctx, s.delay)
	}
	if err != nil {
		server.mu.Lock()
		server.canceled++
		server.mu.Unlock()
		return Response{}, err
	}
	return Response{StatusCode: s.code, Body: "payload"}, nil
}

func replicas(senders ...Sender) []Replica {
	pool := make([]Replica, len(senders))
	for i, sender := range senders {
		pool[i] = Replica{ID: strconv.Itoa(i), Sender: sender}
	}
	return pool
}

func testBreaker(t *testing.T, config BreakerConfig) *CircuitBreaker {
	t.Helper()
	breaker, err := NewCircuitBreaker(config)
	if err != nil {
		t.Fatal(err)
	}
	return breaker
}
