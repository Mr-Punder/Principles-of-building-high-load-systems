package resilience

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidConfig = errors.New("invalid configuration")
	ErrResponse      = errors.New("unsuccessful response")
	ErrBudget        = errors.New("request budget exhausted")
)

type Response struct {
	StatusCode int
	Body       string
}

type Sender interface {
	Send(context.Context) (Response, error)
}

type Client interface {
	ExecuteRequest(context.Context, Sender, Budget) (Response, error)
}

type MultiClient interface {
	ExecuteRequest(context.Context, []Replica, Budget) (Response, error)
}

type Budget struct {
	Attempts       int
	FastErrors     int
	Failures       int
	Timeouts       int
	TotalTimeout   time.Duration
	AttemptTimeout time.Duration
}

type Replica struct {
	ID string
	Sender
}

func invalid(field string) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfig, field)
}

func (b Budget) validateTime() error {
	switch {
	case b.TotalTimeout <= 0:
		return invalid("TotalTimeout must be positive")
	case b.AttemptTimeout < 0:
		return invalid("AttemptTimeout must not be negative")
	}
	return nil
}

func (b Budget) validateRetry(delay time.Duration) error {
	switch {
	case b.Attempts < 1:
		return invalid("Attempts must be positive")
	case b.FastErrors < 0:
		return invalid("FastErrors must not be negative")
	case b.Failures < 0:
		return invalid("Failures must not be negative")
	case b.Timeouts < 0:
		return invalid("Timeouts must not be negative")
	case delay < 0:
		return invalid("backoff delay must not be negative")
	}
	return b.validateTime()
}

type result struct {
	response Response
	err      error
}

func contextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func call(ctx context.Context, sender Sender, timeout time.Duration) (Response, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if err := contextError(ctx); err != nil {
		return Response{}, err
	}
	answers := make(chan result, 1)
	go func() {
		response, err := sender.Send(ctx)
		answers <- result{response, err}
	}()
	select {
	case <-ctx.Done():
		return Response{}, ctx.Err()
	case answer := <-answers:
		if err := contextError(ctx); err != nil {
			return Response{}, err
		}
		if answer.err == nil && (answer.response.StatusCode < 200 || answer.response.StatusCode >= 300) {
			answer.err = ErrResponse
		}
		return answer.response, answer.err
	}
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return contextError(ctx)
	}
}

type limitedSender struct {
	sender Sender
	slots  chan struct{}
}

func LimitInFlight(sender Sender, max int) (Sender, error) {
	if sender == nil {
		return nil, invalid("Sender is nil")
	}
	if max < 1 {
		return nil, invalid("max in-flight attempts must be positive")
	}
	return &limitedSender{sender: sender, slots: make(chan struct{}, max)}, nil
}

func (limited *limitedSender) Send(ctx context.Context) (Response, error) {
	select {
	case limited.slots <- struct{}{}:
		defer func() { <-limited.slots }()
	case <-ctx.Done():
		return Response{}, ctx.Err()
	}
	if err := contextError(ctx); err != nil {
		return Response{}, err
	}
	return limited.sender.Send(ctx)
}
