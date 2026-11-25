package ssekit

import (
	"net/http"
	"strconv"
	"sync"
	"time"
)

// BrokerOptions configures a Broker.
type BrokerOptions struct {
	Replay       int           // events kept for Last-Event-ID resume; default 100
	Buffer       int           // per-subscriber queue; default 64
	KeepAlive    time.Duration // comment interval in ServeHTTP; default 15s, <0 disables
	WriteTimeout time.Duration // per-write timeout in ServeHTTP; default 10s
}

// Broker fans events out to many subscribers and keeps a replay buffer so
// reconnecting clients receive what they missed. A subscriber that can't
// keep up is disconnected instead of slowing everyone down; on reconnect it
// resumes from its Last-Event-ID.
type Broker struct {
	opts BrokerOptions

	mu     sync.Mutex
	subs   map[*subscriber]struct{}
	replay []Event
	seq    uint64
	closed bool
}

type subscriber struct {
	ch chan Event
}

// NewBroker returns a Broker.
func NewBroker(opts BrokerOptions) *Broker {
	if opts.Replay <= 0 {
		opts.Replay = 100
	}
	if opts.Buffer <= 0 {
		opts.Buffer = 64
	}
	if opts.KeepAlive == 0 {
		opts.KeepAlive = 15 * time.Second
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = 10 * time.Second
	}
	return &Broker{opts: opts, subs: map[*subscriber]struct{}{}}
}

// Publish sends e to all subscribers. If e.ID is empty, a monotonically
// increasing ID is assigned. It returns the event as published.
func (b *Broker) Publish(e Event) Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return e
	}
	b.seq++
	if e.ID == "" {
		e.ID = strconv.FormatUint(b.seq, 10)
	}
	b.replay = append(b.replay, e)
	if len(b.replay) > b.opts.Replay {
		b.replay = b.replay[len(b.replay)-b.opts.Replay:]
	}
	for s := range b.subs {
		select {
		case s.ch <- e:
		default: // too slow: drop the subscriber; it will resume via Last-Event-ID
			delete(b.subs, s)
			close(s.ch)
		}
	}
	return e
}

// Subscribe returns a channel of events. Events after lastEventID that are
// still in the replay buffer are delivered first. The channel is closed by
// cancel, by Close, or when the subscriber falls too far behind.
func (b *Broker) Subscribe(lastEventID string) (events <-chan Event, cancel func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := &subscriber{ch: make(chan Event, b.opts.Buffer+b.opts.Replay)}
	if b.closed {
		close(s.ch)
		return s.ch, func() {}
	}
	if lastEventID != "" {
		start := -1
		for i, e := range b.replay {
			if e.ID == lastEventID {
				start = i + 1
				break
			}
		}
		if start == -1 {
			start = 0 // unknown ID (too old or from a previous process): replay what we have
		}
		for _, e := range b.replay[start:] {
			s.ch <- e
		}
	}
	b.subs[s] = struct{}{}
	var once sync.Once
	return s.ch, func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if _, ok := b.subs[s]; ok {
				delete(b.subs, s)
				close(s.ch)
			}
		})
	}
}

// Subscribers returns the number of connected subscribers.
func (b *Broker) Subscribers() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}

// Close disconnects all subscribers and stops accepting events.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	for s := range b.subs {
		delete(b.subs, s)
		close(s.ch)
	}
}

// ServeHTTP streams the broker's events to one client, resuming from its
// Last-Event-ID.
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s, err := Upgrade(w, r, StreamOptions{WriteTimeout: b.opts.WriteTimeout})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	events, cancel := b.Subscribe(LastEventID(r))
	defer cancel()
	if b.opts.KeepAlive > 0 {
		stop := s.KeepAlive(b.opts.KeepAlive)
		defer stop()
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case e, ok := <-events:
			if !ok {
				return
			}
			if s.Send(e) != nil {
				return
			}
		}
	}
}
