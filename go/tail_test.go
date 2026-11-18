package loki

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// wsServer accepts a WebSocket on the tail path and runs script on it.
func wsServer(t *testing.T, script func(ctx context.Context, c *websocket.Conn, r *http.Request)) (*httptest.Server, chan *http.Request) {
	t.Helper()
	reqs := make(chan *http.Request, 4)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != pathTail {
			http.NotFound(w, r)
			return
		}
		reqs <- r.Clone(context.Background())
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		script(r.Context(), c, r)
	}))
	t.Cleanup(ts.Close)
	return ts, reqs
}

func TestTail(t *testing.T) {
	frameOK := string(loadFixture(t, "tail.ok").Response.Frame)
	frameCat := string(loadFixture(t, "tail.categorized-labels").Response.Frame)
	release := make(chan struct{})
	ts, reqs := wsServer(t, func(ctx context.Context, c *websocket.Conn, _ *http.Request) {
		_ = c.Write(ctx, websocket.MessageText, []byte(frameOK))
		_ = c.Write(ctx, websocket.MessageText, []byte(frameCat))
		_ = c.Write(ctx, websocket.MessageText, []byte(`{"streams":[],"dropped_entries":[{"labels":{"a":"b"},"timestamp":"1795035084588079388"}]}`))
		<-release
		_, _, _ = c.Read(ctx) // wait for the client close
	})
	c := newLoki(t, ts.URL, func(cfg *Config) {
		cfg.BearerToken = NewSecret("tail-token")
		cfg.TenantID = "tenant-a"
		cfg.Headers = map[string]string{"X-Extra": "1"}
	})
	var mu sync.Mutex
	var frames []TailFrame
	got := make(chan struct{}, 8)
	start := time.Unix(1795035000, 0)
	s, err := c.Tail(t.Context(), `{run="x"}`, func(f TailFrame) {
		mu.Lock()
		frames = append(frames, f)
		mu.Unlock()
		got <- struct{}{}
		if len(f.DroppedEntries) > 0 {
			panic("callback panics are recovered")
		}
	}, LiveTailOptions{Start: start, Limit: 10, DelayFor: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	r := <-reqs
	q := r.URL.Query()
	if q.Get("start") != "1795035000000000000" || q.Get("limit") != "10" || q.Get("delay_for") != "2" ||
		r.Header.Get("Authorization") != "Bearer tail-token" || r.Header.Get(HeaderTenant) != "tenant-a" ||
		r.Header.Get(HeaderEncodingFlags) != "categorize-labels" || r.Header.Get("X-Extra") != "1" || r.Header.Get("User-Agent") != UserAgent {
		t.Fatalf("handshake %v %v", q, r.Header)
	}
	for range 3 {
		<-got
	}
	close(release)
	mu.Lock()
	if frames[0].Streams[0].Entries[0].Line != "tail marker c023892b83" || frames[0].Streams[0].Labels["tail"] != "1" {
		t.Fatalf("frame 0 %+v", frames[0])
	}
	if len(frames[1].Streams) == 0 {
		t.Fatalf("frame 1 %+v", frames[1])
	}
	mu.Unlock()
	if s.DroppedCount() != 1 {
		t.Fatalf("dropped %d", s.DroppedCount())
	}
	s.Stop()
	s.Stop()
	<-s.Done()
	if s.Err() != nil {
		t.Fatalf("err after stop: %v", s.Err())
	}
}

func TestTailEnds(t *testing.T) {
	t.Run("metric query rejected by Loki close frame", func(t *testing.T) {
		ts, _ := wsServer(t, func(_ context.Context, c *websocket.Conn, _ *http.Request) {
			_ = c.Close(websocket.StatusInternalError, "only log selector is supported")
		})
		s, err := newLoki(t, ts.URL, nil).Tail(t.Context(), `{a="b"}`, func(TailFrame) {}, LiveTailOptions{})
		if err != nil {
			t.Fatal(err)
		}
		<-s.Done()
		if !errors.Is(s.Err(), ErrValidation) || !strings.Contains(s.Err().Error(), "only log selector") {
			t.Fatalf("%v", s.Err())
		}
	})
	t.Run("abnormal close and normal close", func(t *testing.T) {
		ts, _ := wsServer(t, func(_ context.Context, c *websocket.Conn, _ *http.Request) {
			_ = c.Close(websocket.StatusTryAgainLater, "busy")
		})
		s, err := newLoki(t, ts.URL, nil).Tail(t.Context(), `{a="b"}`, func(TailFrame) {}, LiveTailOptions{})
		if err != nil {
			t.Fatal(err)
		}
		<-s.Done()
		if !errors.Is(s.Err(), ErrUnavailable) {
			t.Fatalf("%v", s.Err())
		}
		ts2, _ := wsServer(t, func(_ context.Context, c *websocket.Conn, _ *http.Request) {
			_ = c.Close(websocket.StatusNormalClosure, "")
		})
		s2, _ := newLoki(t, ts2.URL, nil).Tail(t.Context(), `{a="b"}`, func(TailFrame) {}, LiveTailOptions{})
		<-s2.Done()
		if s2.Err() != nil {
			t.Fatal(s2.Err())
		}
	})
	t.Run("invalid frame", func(t *testing.T) {
		ts, _ := wsServer(t, func(ctx context.Context, c *websocket.Conn, _ *http.Request) {
			_ = c.Write(ctx, websocket.MessageText, []byte(`not json`))
			_, _, _ = c.Read(ctx)
		})
		s, err := newLoki(t, ts.URL, nil).Tail(t.Context(), `{a="b"}`, func(TailFrame) {}, LiveTailOptions{})
		if err != nil {
			t.Fatal(err)
		}
		<-s.Done()
		if !errors.Is(s.Err(), ErrUnavailable) || !strings.Contains(s.Err().Error(), "invalid frame") {
			t.Fatalf("%v", s.Err())
		}
	})
	t.Run("context cancel and client close", func(t *testing.T) {
		ts, _ := wsServer(t, func(ctx context.Context, c *websocket.Conn, _ *http.Request) {
			_, _, _ = c.Read(ctx)
		})
		ctx, cancel := context.WithCancel(t.Context())
		s, err := newLoki(t, ts.URL, nil).Tail(ctx, `{a="b"}`, func(TailFrame) {}, LiveTailOptions{})
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		<-s.Done()
		if !errors.Is(s.Err(), ErrCancelled) {
			t.Fatalf("%v", s.Err())
		}
		c := newLoki(t, ts.URL, nil)
		s2, err := c.Tail(t.Context(), `{a="b"}`, func(TailFrame) {}, LiveTailOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_ = c.Close()
		<-s2.Done()
		if _, err := c.Tail(t.Context(), `{a="b"}`, func(TailFrame) {}, LiveTailOptions{}); !errors.Is(err, ErrCancelled) {
			t.Fatalf("closed client: %v", err)
		}
	})
	t.Run("server goes away", func(t *testing.T) {
		ts, _ := wsServer(t, func(_ context.Context, c *websocket.Conn, _ *http.Request) {
			_ = c.CloseNow()
		})
		s, err := newLoki(t, ts.URL, nil).Tail(t.Context(), `{a="b"}`, func(TailFrame) {}, LiveTailOptions{})
		if err != nil {
			t.Fatal(err)
		}
		<-s.Done()
		if !errors.Is(s.Err(), ErrUnavailable) {
			t.Fatalf("%v", s.Err())
		}
	})
}

func TestTailHandshake(t *testing.T) {
	for name, code := range map[string]ErrorCode{
		"tail.mt-no-tenant":           ErrorCodeAuthentication,
		"tail.bad-query":              ErrorCodeValidation,
		"tail.delay-too-high":         ErrorCodeValidation,
		"http.not-found":              ErrorCodeNotFound,
		"ping.no-tenant":              ErrorCodeAuthentication,
		"transport.gateway-fault-502": ErrorCodeUnavailable,
	} {
		ts := newTestServer(t, serveFixture(t, name))
		_, err := newLoki(t, ts.URL, nil).Tail(t.Context(), `{a="b"}`, func(TailFrame) {}, LiveTailOptions{})
		if CodeOf(err) != code {
			t.Errorf("%s: %v", name, err)
		}
	}
	c := newLoki(t, "http://"+closedAddr(t), nil)
	for i, fn := range []func() error{
		func() error { _, err := c.Tail(t.Context(), `{a="b"}`, nil, LiveTailOptions{}); return err },
		func() error {
			_, err := c.Tail(t.Context(), `count_over_time({a="b"}[1m])`, func(TailFrame) {}, LiveTailOptions{})
			return err
		},
		func() error {
			_, err := c.Tail(t.Context(), `{a="b"}`, func(TailFrame) {}, LiveTailOptions{DelayFor: 6 * time.Second})
			return err
		},
		func() error {
			_, err := c.Tail(t.Context(), `{a="b"}`, func(TailFrame) {}, LiveTailOptions{Limit: -1})
			return err
		},
	} {
		if err := fn(); !errors.Is(err, ErrValidation) {
			t.Errorf("case %d: %v", i, err)
		}
	}
	if _, err := c.Tail(t.Context(), `{a="b"}`, func(TailFrame) {}, LiveTailOptions{}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unreachable: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Tail(ctx, `{a="b"}`, func(TailFrame) {}, LiveTailOptions{}); !errors.Is(err, ErrCancelled) {
		t.Fatalf("cancelled: %v", err)
	}
	// A server that never answers the handshake hits the handshake timeout.
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer hang.Close()
	slow := newLoki(t, hang.URL, func(cfg *Config) { cfg.ConnectTimeout, cfg.RequestTimeout = 50*time.Millisecond, 50*time.Millisecond })
	if _, err := slow.Tail(t.Context(), `{a="b"}`, func(TailFrame) {}, LiveTailOptions{}); !errors.Is(err, ErrTimeout) {
		t.Fatalf("handshake timeout: %v", err)
	}
	if _, err := decodeTailFrame([]byte(`{"streams":[{"stream":{},"values":[["x","l"]]}]}`)); err == nil {
		t.Fatal("bad stream ts")
	}
	if _, err := decodeTailFrame([]byte(`{"dropped_entries":[{"labels":{},"timestamp":"x"}]}`)); err == nil {
		t.Fatal("bad dropped ts")
	}
}
