package sse

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
)

// ErrLineTooLong is returned when a single line exceeds MaxLineSize.
var ErrLineTooLong = errors.New("sse: line too long")

// MaxLineSize bounds one line of the stream (default 1 MiB).
var MaxLineSize = 1 << 20

// Reader parses an event stream as specified by the WHATWG HTML standard:
// CRLF/LF/CR line endings, comments, multi-line data, a persistent last
// event ID and retry hints.
type Reader struct {
	br      *bufio.Reader
	started bool
	lastID  string
	retry   time.Duration
}

// NewReader returns a Reader for r (typically an HTTP response body).
func NewReader(r io.Reader) *Reader { return &Reader{br: bufio.NewReader(r)} }

// LastEventID is the most recent ID seen, to send back on reconnect.
func (r *Reader) LastEventID() string { return r.lastID }

// Retry is the last reconnection delay requested by the server (0 if none).
func (r *Reader) Retry() time.Duration { return r.retry }

// Next returns the next event. At the end of the stream it returns io.EOF;
// a trailing event without a blank line is discarded, as the spec requires.
func (r *Reader) Next() (Event, error) {
	var data strings.Builder
	var hasData bool
	var eventType string
	for {
		line, err := r.readLine()
		if err != nil {
			return Event{}, err
		}
		if line == "" { // dispatch
			if !hasData {
				eventType = ""
				continue
			}
			return Event{ID: r.lastID, Event: eventType, Data: data.String()}, nil
		}
		if line[0] == ':' {
			continue // comment
		}
		field, value, found := strings.Cut(line, ":")
		if found {
			value = strings.TrimPrefix(value, " ")
		}
		switch field {
		case "data":
			if hasData {
				data.WriteByte('\n')
			}
			data.WriteString(value)
			hasData = true
		case "event":
			eventType = value
		case "id":
			if !strings.ContainsRune(value, 0) {
				r.lastID = value
			}
		case "retry":
			if ms, err := strconv.ParseUint(value, 10, 63); err == nil && value != "" {
				r.retry = time.Duration(ms) * time.Millisecond
			}
		}
	}
}

func (r *Reader) readLine() (string, error) {
	var buf bytes.Buffer
	for {
		c, err := r.br.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return "", io.EOF // incomplete line/event is discarded
			}
			return "", err
		}
		switch c {
		case '\n':
			return r.finish(buf.Bytes()), nil
		case '\r':
			if next, err := r.br.Peek(1); err == nil && next[0] == '\n' {
				_, _ = r.br.ReadByte()
			}
			return r.finish(buf.Bytes()), nil
		default:
			buf.WriteByte(c)
			if buf.Len() > MaxLineSize {
				return "", ErrLineTooLong
			}
		}
	}
}

func (r *Reader) finish(b []byte) string {
	if !r.started {
		r.started = true
		b = bytes.TrimPrefix(b, []byte("\xEF\xBB\xBF")) // UTF-8 BOM
	}
	return string(b)
}
