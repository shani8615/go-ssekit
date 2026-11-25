package ssekit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSendFormat(t *testing.T) {
	rec := httptest.NewRecorder()
	s, err := Upgrade(rec, httptest.NewRequest("GET", "/", nil))
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Send(Event{ID: "7", Event: "token", Data: "line1\r\nline2\nline3", Retry: 1500 * time.Millisecond})
	_ = s.Comment("ping\nx")
	want := "id: 7\nevent: token\nretry: 1500\ndata: line1\ndata: line2\ndata: line3\n\n: ping x\n\n"
	if got := rec.Body.String(); got != want {
		t.Fatalf("got %q\nwant %q", got, want)
	}
	h := rec.Header()
	if h.Get("Content-Type") != "text/event-stream" || h.Get("Cache-Control") != "no-cache" || h.Get("X-Accel-Buffering") != "no" {
		t.Fatalf("headers=%v", h)
	}
	if err := s.Send(Event{ID: "a\nb"}); !errors.Is(err, ErrInvalidField) {
		t.Fatal("newline in id must be rejected")
	}
}

func TestReaderSpecCases(t *testing.T) {
	// Adapted from the WHATWG HTML spec examples.
	in := "\xEF\xBB\xBFdata: first\r\n\r\n" + // BOM + CRLF
		": comment\n" +
		"event: add\ndata\ndata: \ndata:  two spaces\n\n" + // empty data lines, one-space strip only
		"id: 42\rdata: cr only\r\r" + // CR line endings
		"retry: 250\nretry: nope\n\n" + // retry without data: no dispatch
		"data: no id change\n\n" +
		"id\ndata: id cleared\n\n" +
		"field-without-colon\ndata:x\n\n" +
		"data: trailing event without blank line is dropped"
	r := NewReader(strings.NewReader(in))
	want := []Event{
		{Data: "first"},
		{Event: "add", Data: "\n\n two spaces"},
		{ID: "42", Data: "cr only"},
		{ID: "42", Data: "no id change"},
		{ID: "", Data: "id cleared"},
		{ID: "", Data: "x"},
	}
	for i, w := range want {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		if got != w {
			t.Fatalf("event %d: got %+v want %+v", i, got, w)
		}
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF, got %v", err)
	}
	if r.Retry() != 250*time.Millisecond {
		t.Fatalf("retry=%v", r.Retry())
	}
}

func TestRoundTrip(t *testing.T) {
	rec := httptest.NewRecorder()
	s, _ := Upgrade(rec, httptest.NewRequest("GET", "/", nil))
	in := []Event{{ID: "1", Event: "e", Data: "a\nb"}, {Data: ""}, {ID: "x y", Data: "unicode ✓"}}
	for _, e := range in {
		_ = s.Send(e)
	}
	r := NewReader(rec.Body)
	for i, w := range in {
		if w.ID == "" {
			w.ID = in[i-1].ID // IDs persist
		}
		if got, err := r.Next(); err != nil || got != w {
			t.Fatalf("event %d: got %+v err=%v want %+v", i, got, err, w)
		}
	}
}

func TestBrokerReplayAndSlowSubscriber(t *testing.T) {
	b := NewBroker(BrokerOptions{Replay: 3, Buffer: 1})
	for i := 1; i <= 5; i++ {
		b.Publish(Event{Data: fmt.Sprint(i)})
	}
	ch, cancel := b.Subscribe("3")
	if e := <-ch; e.ID != "4" {
		t.Fatalf("replay should start after id 3, got %s", e.ID)
	}
	if e := <-ch; e.ID != "5" {
		t.Fatalf("got %s", e.ID)
	}
	cancel()
	cancel() // idempotent

	old, cancelOld := b.Subscribe("1") // id 1 left the buffer: replay everything kept
	defer cancelOld()
	if e := <-old; e.ID != "3" {
		t.Fatalf("unknown id should replay from oldest kept, got %s", e.ID)
	}

	slow, _ := b.Subscribe("")
	for i := 0; i < 10; i++ {
		b.Publish(Event{Data: "x"})
	}
	n := 0
	for range slow {
		n++
	}
	if n == 0 || n > 10 {
		t.Fatalf("slow subscriber got %d events before being dropped", n)
	}
}

func TestBrokerOverHTTPWithResume(t *testing.T) {
	b := NewBroker(BrokerOptions{KeepAlive: -1})
	srv := httptest.NewServer(b)
	defer srv.Close()

	b.Publish(Event{Data: "before"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL, nil)
	req.Header.Set("Last-Event-ID", "0")
	var got []string
	done := make(chan error, 1)
	go func() {
		done <- (&Client{}).Do(req, func(e Event) error {
			got = append(got, e.ID+":"+e.Data)
			if len(got) == 3 {
				return ErrStop
			}
			return nil
		})
	}()
	waitFor(t, func() bool { return b.Subscribers() == 1 })
	b.Publish(Event{Data: "live1"})
	b.Publish(Event{Data: "live2"})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "1:before,2:live1,3:live2" {
		t.Fatalf("got %v", got)
	}
}

func TestClientReconnectsWithLastEventID(t *testing.T) {
	var conns atomic.Int32
	var mu sync.Mutex
	var seenIDs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := conns.Add(1)
		mu.Lock()
		seenIDs = append(seenIDs, r.Header.Get("Last-Event-ID"))
		mu.Unlock()
		switch n {
		case 1:
			s, _ := Upgrade(w, r)
			_ = s.Send(Event{ID: "1", Data: "a", Retry: 10 * time.Millisecond})
			_ = s.Send(Event{ID: "2", Data: "b"})
			// connection drops here
		case 2:
			w.WriteHeader(http.StatusBadGateway) // transient failure
		case 3:
			s, _ := Upgrade(w, r)
			_ = s.Send(Event{ID: "3", Data: "c"})
		default:
			w.WriteHeader(http.StatusNoContent) // stop
		}
	}))
	defer srv.Close()

	var data []string
	err := (&Client{InitialRetry: 10 * time.Millisecond}).Subscribe(context.Background(), srv.URL, func(e Event) error {
		data = append(data, e.Data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(data, "") != "abc" || strings.Join(seenIDs, ",") != ",2,2,3" {
		t.Fatalf("data=%v lastIDs=%v", data, seenIDs)
	}
}

func TestClientStopsOnClientError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(401) }))
	defer srv.Close()
	err := (&Client{}).Subscribe(context.Background(), srv.URL, func(Event) error { return nil })
	var se *StatusError
	if !errors.As(err, &se) || se.StatusCode != 401 {
		t.Fatalf("err=%v", err)
	}
}

func TestClientHandlerErrorAndMaxReconnects(t *testing.T) {
	boom := errors.New("boom")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, _ := Upgrade(w, r)
		_ = s.Send(Event{Data: "x"})
	}))
	if err := (&Client{}).Subscribe(context.Background(), srv.URL, func(Event) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("handler error not returned: %v", err)
	}
	srv.Close()
	err := (&Client{InitialRetry: time.Millisecond, MaxReconnects: 2}).Subscribe(context.Background(), srv.URL, func(Event) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "giving up") {
		t.Fatalf("err=%v", err)
	}
}

func TestDoWithPOSTLikeLLMStreaming(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(405)
			return
		}
		s, _ := Upgrade(w, r)
		for _, tok := range []string{`{"delta":"Hel"}`, `{"delta":"lo"}`, "[DONE]"} {
			_ = s.Send(Event{Data: tok})
		}
	}))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader(`{"stream":true}`))
	var toks []string
	err := (&Client{}).Do(req, func(e Event) error {
		if e.Data == "[DONE]" {
			return ErrStop
		}
		toks = append(toks, e.Data)
		return nil
	})
	if err != nil || len(toks) != 2 {
		t.Fatalf("toks=%v err=%v", toks, err)
	}
}

func TestKeepAliveAndDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rec := &syncRecorder{ResponseRecorder: httptest.NewRecorder()}
	s, _ := Upgrade(rec, httptest.NewRequest("GET", "/", nil).WithContext(ctx))
	stop := s.KeepAlive(5 * time.Millisecond)
	defer stop()
	waitFor(t, func() bool { return strings.Contains(rec.String(), ": ping\n\n") })
	cancel()
	if err := s.Send(Event{Data: "x"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("send after disconnect: %v", err)
	}
}

type syncRecorder struct {
	mu sync.Mutex
	*httptest.ResponseRecorder
}

func (r *syncRecorder) Write(b []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ResponseRecorder.Write(b)
}

func (r *syncRecorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Body.String()
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
}
