package resilience

import (
	"context"
	"errors"
	"time"
)

type Default struct{}

func (Default) ExecuteRequest(ctx context.Context, sender Sender, b Budget) (Response, error) {
	if err := b.validateTime(); err != nil {
		return Response{}, err
	}
	if sender == nil {
		return Response{}, invalid("Sender is nil")
	}
	ctx, cancel := context.WithTimeout(ctx, b.TotalTimeout)
	defer cancel()
	return call(ctx, sender, b.AttemptTimeout)
}

type Retry struct{}

func (Retry) ExecuteRequest(ctx context.Context, sender Sender, b Budget) (Response, error) {
	return Backoff{}.ExecuteRequest(ctx, sender, b)
}

type Backoff struct {
	Delay time.Duration
}

func (s Backoff) ExecuteRequest(ctx context.Context, sender Sender, b Budget) (Response, error) {
	if sender == nil {
		return Response{}, invalid("Sender is nil")
	}
	return retry(ctx, b, s.Delay, func(context.Context) (Sender, error) {
		return sender, nil
	}, nil)
}

func retry(ctx context.Context, b Budget, delay time.Duration, next func(context.Context) (Sender, error), observe func(Response, error)) (Response, error) {
	if err := b.validateRetry(delay); err != nil {
		return Response{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, b.TotalTimeout)
	defer cancel()
	for attempt := 0; attempt < b.Attempts; attempt++ {
		if err := contextError(ctx); err != nil {
			return Response{}, err
		}
		sender, err := next(ctx)
		if err != nil {
			return Response{}, err
		}
		response, err := call(ctx, sender, b.AttemptTimeout)

		if observe != nil {
			observe(response, err)
		}
		if ctxErr := contextError(ctx); ctxErr != nil {
			return Response{}, ctxErr
		}
		if err == nil {
			return response, nil
		}
		fast := response.StatusCode >= 400 && response.StatusCode < 500 && errors.Is(err, ErrResponse)
		remaining := 0
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			b.Timeouts--
			remaining = b.Timeouts
		case fast:
			b.FastErrors--
			remaining = b.FastErrors
		default:
			b.Failures--
			remaining = b.Failures
		}
		if remaining <= 0 || attempt+1 == b.Attempts {
			return response, errors.Join(ErrBudget, err)
		}
		if !fast && delay > 0 {
			if err := wait(ctx, delay); err != nil {
				return Response{}, err
			}
			const maxDelay = time.Duration(1<<63 - 1)
			if delay > maxDelay/2 {
				delay = maxDelay
			} else {
				delay *= 2
			}
		}
	}
	return Response{}, ErrBudget
}
