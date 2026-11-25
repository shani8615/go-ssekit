package ssekit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"mime"
	"net/http"
	"time"
)

// Client consumes event streams.
type Client struct {
	HTTPClient *http.Client // default: no timeout (streams are long-lived)
	Header     http.Header  // extra headers, e.g. Authorization
	// InitialRetry is the reconnect delay until the server sends a retry
	// hint (default 1s). Delays double on consecutive failures up to MaxRetry.
	InitialRetry time.Duration
	MaxRetry     time.Duration // default 30s
	// MaxReconnects limits consecutive failed reconnects (0 = unlimited).
	MaxReconnects int
}

// StatusError is a non-retryable HTTP response (4xx other than 408/429).
type StatusError struct{ StatusCode int }

func (e *StatusError) Error() string {
	return fmt.Sprintf("sse: unexpected HTTP status %d", e.StatusCode)
}

// ErrStop can be returned by a handler to end Subscribe without an error.
var ErrStop = errors.New("sse: stop")

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{}
}

// Subscribe connects to url with GET and calls handle for every event,
// reconnecting with Last-Event-ID after network errors, server errors and
// clean disconnects. It returns when ctx ends, the server answers 204 (the
// spec's "stop reconnecting"), a non-retryable status arrives, or handle
// returns an error (ErrStop ends without error).
func (c *Client) Subscribe(ctx context.Context, url string, handle func(Event) error) error {
	retry := c.InitialRetry
	if retry <= 0 {
		retry = time.Second
	}
	maxRetry := c.MaxRetry
	if maxRetry <= 0 {
		maxRetry = 30 * time.Second
	}
	lastID := ""
	failures := 0
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		if lastID != "" {
			req.Header.Set("Last-Event-ID", lastID)
		}
		got, rd, err := c.run(req, handle)
		if rd != nil {
			lastID = rd.LastEventID()
			if rd.Retry() > 0 {
				retry = rd.Retry()
			}
		}
		switch {
		case errors.Is(err, ErrStop):
			return nil
		case errors.Is(err, errNoContent):
			return nil
		case ctx.Err() != nil:
			return ctx.Err()
		}
		var se *StatusError
		var he *handlerError
		if errors.As(err, &se) || errors.As(err, &he) {
			if he != nil {
				return he.err
			}
			return err
		}
		if got {
			failures = 0
		} else {
			failures++
			if c.MaxReconnects > 0 && failures > c.MaxReconnects {
				return fmt.Errorf("sse: giving up after %d failed reconnects: %w", failures-1, err)
			}
		}
		wait := retry << min(failures, 5)
		if failures == 0 {
			wait = retry
		}
		wait = min(wait, maxRetry)
		wait = wait/2 + rand.N(wait/2+1) // jitter
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// Do performs req once (any method, e.g. a POST to a streaming LLM API) and
// calls handle for every event until the stream ends. It does not reconnect.
func (c *Client) Do(req *http.Request, handle func(Event) error) error {
	_, _, err := c.run(req, handle)
	var he *handlerError
	switch {
	case errors.As(err, &he):
		if errors.Is(he.err, ErrStop) {
			return nil
		}
		return he.err
	case errors.Is(err, errNoContent), errors.Is(err, io.EOF), err == nil:
		return nil
	}
	return err
}

var errNoContent = errors.New("sse: 204 no content")

type handlerError struct{ err error }

func (e *handlerError) Error() string { return e.err.Error() }
func (e *handlerError) Unwrap() error { return e.err }

// run executes one connection. got reports whether any event was received.
func (c *Client) run(req *http.Request, handle func(Event) error) (got bool, rd *Reader, err error) {
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	for k, vs := range c.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return false, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode == http.StatusNoContent:
		return false, nil, errNoContent
	case resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return false, nil, fmt.Errorf("sse: HTTP %d", resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return false, nil, &StatusError{StatusCode: resp.StatusCode}
	}
	if mt, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); mt != "text/event-stream" {
		return false, nil, &StatusError{StatusCode: resp.StatusCode}
	}
	rd = NewReader(resp.Body)
	if id := req.Header.Get("Last-Event-ID"); id != "" {
		rd.lastID = id // keep it if the server sends no new IDs
	}
	for {
		e, err := rd.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return got, rd, nil
			}
			return got, rd, err
		}
		got = true
		if err := handle(e); err != nil {
			return got, rd, &handlerError{err}
		}
	}
}
