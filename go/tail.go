package loki

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// maxTailFrame bounds one WebSocket tail frame.
const maxTailFrame = 64 << 20

// LiveTailOptions configure Client.Tail.
type LiveTailOptions struct {
	// Start replays entries from Start before streaming live ones. Zero means
	// "live only" (Start = now); without it Loki would replay the last hour.
	Start time.Time
	// Limit bounds the replayed entries (Loki default 100).
	Limit int
	// DelayFor delays delivery to let late entries arrive (0 to 5 s, whole seconds).
	DelayFor time.Duration
}

// TailFrame is one message of a live tail.
type TailFrame struct {
	// Streams holds the new entries, categorized like QueryRange results.
	Streams []StreamResult
	// DroppedEntries lists entries Loki dropped because the tailer fell behind.
	DroppedEntries []DroppedEntry
}

// DroppedEntry identifies an entry dropped by a slow tail.
type DroppedEntry struct {
	Labels    map[string]string
	Timestamp time.Time
}

// TailSession is a running live tail. It implements TailHandle.
type TailSession struct {
	cancel context.CancelFunc
	done   chan struct{}
	conn   *websocket.Conn

	mu      sync.Mutex
	err     error
	dropped int64
	once    sync.Once
}

var _ TailHandle = (*TailSession)(nil)

// Stop closes the WebSocket (close code 1000) and waits for the reader to
// finish. It is idempotent.
func (s *TailSession) Stop() {
	s.once.Do(func() {
		_ = s.conn.Close(websocket.StatusNormalClosure, "")
		s.cancel()
	})
	<-s.done
}

// Done is closed when the tail ends (Stop, ctx done, server close or error).
func (s *TailSession) Done() <-chan struct{} { return s.done }

// Err returns why the tail ended: nil after Stop or a normal close, CANCELLED
// when ctx was cancelled, VALIDATION when Loki closed it for a bad query (for
// example a metric query), UNAVAILABLE on connection errors.
func (s *TailSession) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// DroppedCount is the number of entries Loki reported as dropped.
func (s *TailSession) DroppedCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
}

// Tail streams new entries matching a log query over a WebSocket (GET
// /loki/api/v1/tail). onEntries is called from one goroutine for each frame.
// The handshake carries the auth, tenant and categorize-labels headers;
// handshake failures are returned as errors (401 → AUTHENTICATION, 400 →
// VALIDATION). Metric queries and DelayFor above 5 s are rejected client side.
// The tail runs until Stop, ctx is done or the server closes it.
func (c *Client) Tail(ctx context.Context, logql string, onEntries func(TailFrame), opts LiveTailOptions) (*TailSession, error) {
	const op = "tail"
	switch {
	case onEntries == nil:
		return nil, ValidationError(op, "onEntries must not be nil")
	case !isLogQuery(logql):
		return nil, ValidationError(op, "only log queries (starting with a {...} selector) can be tailed")
	case opts.DelayFor < 0 || opts.DelayFor > MaxTailDelay:
		return nil, ValidationError(op, "delayFor must be between 0 and 5s")
	case opts.Limit < 0:
		return nil, ValidationError(op, "limit must not be negative")
	}
	if c.isClosed() {
		return nil, &Error{Code: ErrorCodeCancelled, Message: op + ": client is closed"}
	}
	if err := ctx.Err(); err != nil {
		return nil, ContextError(op, err)
	}
	start := opts.Start
	if start.IsZero() {
		start = time.Now()
	}
	q := url.Values{"query": {logql}, "start": {formatNano(start)}}
	setLimit(q, opts.Limit)
	if opts.DelayFor > 0 {
		q.Set("delay_for", strconv.Itoa(int(opts.DelayFor/time.Second)))
	}
	u := c.transport.resolve(&request{Path: pathTail, Query: q})

	header := c.headers()
	header.Set(HeaderEncodingFlags, "categorize-labels")
	header.Set("User-Agent", c.transport.userAgent)
	for k, v := range c.cfg.Headers {
		header.Set(k, v)
	}

	sctx, cancel := context.WithCancel(ctx)
	handshake := c.cfg.ConnectTimeout + c.cfg.RequestTimeout
	timer := time.AfterFunc(handshake, cancel)
	//nolint:bodyclose // coder/websocket owns the handshake response body ("You never need to close resp.Body")
	conn, resp, err := websocket.Dial(sctx, u.String(), &websocket.DialOptions{
		HTTPClient: c.wsHTTPClient(),
		HTTPHeader: header,
	})
	timedOut := !timer.Stop()
	if err != nil {
		cancel()
		return nil, c.tailDialError(ctx, op, resp, err, timedOut, handshake)
	}
	conn.SetReadLimit(maxTailFrame)
	s := &TailSession{cancel: cancel, done: make(chan struct{}), conn: conn}
	stopOnClose := func() { s.Stop() }
	c.onClose(stopOnClose)
	go s.read(sctx, ctx, onEntries)
	return s, nil
}

// wsHTTPClient is an HTTP/1.1-only client with the transport's TLS, proxy and
// dial settings (WebSocket upgrades need HTTP/1.1).
func (c *Client) wsHTTPClient() *http.Client {
	tr := c.transport.transport.Clone()
	tr.ForceAttemptHTTP2 = false
	tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	if tr.TLSClientConfig != nil {
		tr.TLSClientConfig.NextProtos = []string{"http/1.1"}
	}
	return &http.Client{Transport: tr, CheckRedirect: noCrossHostRedirect}
}

func (c *Client) tailDialError(ctx context.Context, op string, resp *http.Response, err error, timedOut bool, handshake time.Duration) error {
	if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
		var text string
		if resp.Body != nil {
			b, _ := io.ReadAll(resp.Body)
			text = firstLine(b)
		}
		return mapLokiError(StatusError(op, resp.StatusCode, requestID(resp.Header), text, c.transport.knownSecrets()))
	}
	if ctx.Err() != nil {
		return ContextError(op, ctx.Err())
	}
	if timedOut {
		return &Error{Code: ErrorCodeTimeout, Message: fmt.Sprintf("%s: handshake timeout of %s exceeded", op, handshake), Retryable: true}
	}
	return &Error{Code: ErrorCodeUnavailable, Message: op + ": websocket handshake failed: " + c.transport.redact(err.Error()), Retryable: true}
}

// tailWire is one tail frame on the wire.
type tailWire struct {
	Streams        json.RawMessage `json:"streams"`
	DroppedEntries []struct {
		Labels    map[string]string `json:"labels"`
		Timestamp string            `json:"timestamp"`
	} `json:"dropped_entries"`
}

// decodeTailFrame decodes one tail frame.
func decodeTailFrame(data []byte) (TailFrame, error) {
	var w tailWire
	if err := json.Unmarshal(data, &w); err != nil {
		return TailFrame{}, err
	}
	var f TailFrame
	if len(w.Streams) > 0 && string(w.Streams) != "null" {
		streams, err := decodeStreams(w.Streams)
		if err != nil {
			return TailFrame{}, err
		}
		f.Streams = streams
	}
	for _, d := range w.DroppedEntries {
		t, err := ParseUnixNano(d.Timestamp)
		if err != nil {
			return TailFrame{}, err
		}
		f.DroppedEntries = append(f.DroppedEntries, DroppedEntry{Labels: nonNilMap(d.Labels), Timestamp: t})
	}
	return f, nil
}

func (s *TailSession) read(sctx, parent context.Context, onEntries func(TailFrame)) {
	defer close(s.done)
	defer s.cancel()
	for {
		_, data, err := s.conn.Read(sctx)
		if err != nil {
			s.finish(parent, err)
			return
		}
		frame, derr := decodeTailFrame(data)
		if derr != nil {
			s.setErr(&Error{Code: ErrorCodeUnavailable, Message: "tail: invalid frame: " + derr.Error()})
			_ = s.conn.Close(websocket.StatusUnsupportedData, "invalid frame")
			return
		}
		if n := len(frame.DroppedEntries); n > 0 {
			s.mu.Lock()
			s.dropped += int64(n)
			s.mu.Unlock()
		}
		func() {
			defer func() { _ = recover() }()
			onEntries(frame)
		}()
	}
}

func (s *TailSession) setErr(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
}

// finish records why the read loop ended.
func (s *TailSession) finish(parent context.Context, err error) {
	if parent.Err() != nil {
		s.setErr(ContextError("tail", parent.Err()))
		return
	}
	var ce websocket.CloseError
	if errors.As(err, &ce) {
		reason := strings.TrimSpace(ce.Reason)
		switch {
		case isTailRejection(reason):
			s.setErr(ValidationError("tail", "closed by Loki: "+truncate(reason, maxVendorText)))
		case ce.Code == websocket.StatusNormalClosure || ce.Code == websocket.StatusGoingAway:
		default:
			s.setErr(&Error{Code: ErrorCodeUnavailable, Message: fmt.Sprintf("tail: closed by Loki (code %d): %s", ce.Code, truncate(reason, maxVendorText)), Retryable: true})
		}
		return
	}
	s.mu.Lock()
	stopped := s.err == nil && errors.Is(err, context.Canceled)
	s.mu.Unlock()
	if stopped {
		return // Stop() cancelled the read
	}
	s.setErr(&Error{Code: ErrorCodeUnavailable, Message: "tail: connection lost: " + err.Error(), Retryable: true, Err: err})
}

// isTailRejection reports whether a normal close reason is Loki rejecting the query.
func isTailRejection(reason string) bool {
	r := strings.ToLower(reason)
	return strings.Contains(r, "only log selector") || strings.Contains(r, "parse error") || strings.Contains(r, "not supported")
}
