// Package sse implements Server-Sent Events for net/http: a server-side
// Stream, a fan-out Broker with Last-Event-ID replay, a spec-compliant
// Reader, and a reconnecting Client. It is well suited to streaming LLM
// tokens, progress updates and live dashboards. No dependencies.
package sse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Event is one server-sent event.
type Event struct {
	ID    string        // optional; becomes the client's Last-Event-ID
	Event string        // optional event type; clients default to "message"
	Data  string        // payload; may contain newlines
	Retry time.Duration // optional reconnection delay hint for the client
}

// ErrInvalidField is returned for IDs or event names containing line breaks
// (or NUL in IDs), which would corrupt the stream.
var ErrInvalidField = errors.New("sse: id and event must not contain line breaks")

// ErrStreamingUnsupported is returned when the ResponseWriter cannot flush.
var ErrStreamingUnsupported = errors.New("sse: response writer does not support flushing")

// StreamOptions configures Upgrade.
type StreamOptions struct {
	// WriteTimeout bounds each write so a stalled client can't block the
	// handler forever. 0 disables it.
	WriteTimeout time.Duration
}

// Stream writes events to one client. Safe for concurrent use.
type Stream struct {
	w    http.ResponseWriter
	rc   *http.ResponseController
	ctx  context.Context
	opts StreamOptions
	mu   sync.Mutex
	buf  bytes.Buffer
}

// Upgrade starts an event stream on w. It sets the SSE headers (including
// X-Accel-Buffering: no for nginx), writes the status line and flushes.
func Upgrade(w http.ResponseWriter, r *http.Request, opts ...StreamOptions) (*Stream, error) {
	s := &Stream{w: w, rc: http.NewResponseController(w), ctx: r.Context()}
	if len(opts) > 0 {
		s.opts = opts[0]
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if err := s.rc.Flush(); err != nil {
		if errors.Is(err, http.ErrNotSupported) {
			return nil, ErrStreamingUnsupported
		}
		return nil, err
	}
	return s, nil
}

// Context is done when the client disconnects.
func (s *Stream) Context() context.Context { return s.ctx }

// Send writes one event and flushes it.
func (s *Stream) Send(e Event) error {
	if strings.ContainsAny(e.Event, "\r\n") || strings.ContainsAny(e.ID, "\r\n\x00") {
		return ErrInvalidField
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf.Reset()
	if e.ID != "" {
		s.buf.WriteString("id: " + e.ID + "\n")
	}
	if e.Event != "" {
		s.buf.WriteString("event: " + e.Event + "\n")
	}
	if e.Retry > 0 {
		s.buf.WriteString("retry: " + strconv.FormatInt(e.Retry.Milliseconds(), 10) + "\n")
	}
	data := strings.ReplaceAll(strings.ReplaceAll(e.Data, "\r\n", "\n"), "\r", "\n")
	for _, line := range strings.Split(data, "\n") {
		s.buf.WriteString("data: ")
		s.buf.WriteString(line)
		s.buf.WriteByte('\n')
	}
	s.buf.WriteByte('\n')
	return s.write(s.buf.Bytes())
}

// SendJSON marshals v as the event data.
func (s *Stream) SendJSON(event string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.Send(Event{Event: event, Data: string(b)})
}

// Comment writes a comment line (": text"), which clients ignore. Useful as
// a keep-alive through proxies that close idle connections.
func (s *Stream) Comment(text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	text = strings.NewReplacer("\r", " ", "\n", " ").Replace(text)
	return s.write([]byte(": " + text + "\n\n"))
}

// KeepAlive sends a comment every interval until the client disconnects or
// stop is called.
func (s *Stream) KeepAlive(interval time.Duration) (stop func()) {
	ctx, cancel := context.WithCancel(s.ctx)
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if s.Comment("ping") != nil {
					return
				}
			}
		}
	}()
	return cancel
}

func (s *Stream) write(b []byte) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if s.opts.WriteTimeout > 0 {
		_ = s.rc.SetWriteDeadline(time.Now().Add(s.opts.WriteTimeout))
	}
	if _, err := s.w.Write(b); err != nil {
		return err
	}
	return s.rc.Flush()
}

// LastEventID returns the ID the client last received, from the
// Last-Event-ID header (set by browsers on reconnect) or the lastEventId
// query parameter (for clients that can't set headers).
func LastEventID(r *http.Request) string {
	if id := r.Header.Get("Last-Event-ID"); id != "" {
		return id
	}
	return r.URL.Query().Get("lastEventId")
}
