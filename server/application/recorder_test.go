package application

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/argoproj/argo-cd/v3/util/terminalrecording"
)

func testRecorderLogger() *log.Entry {
	logger := log.New()
	logger.SetOutput(io.Discard)
	return log.NewEntry(logger)
}

func testSessionMeta() terminalrecording.Session {
	return terminalrecording.Session{
		ID:        "0123456789abcdef",
		StartTime: time.Now(),
		App:       "default/guestbook",
		User:      "alice@example.com",
		Cluster:   "in-cluster",
		Namespace: "default",
		Pod:       "guestbook-ui-7d9f8",
		Container: "main",
	}
}

// mockRecordingEndpoint is an in-process recording endpoint collecting each
// connection's frames in accept order.
type mockRecordingEndpoint struct {
	t      *testing.T
	server *httptest.Server

	mu        sync.Mutex
	sessions  []terminalrecording.Session
	fragments [][]terminalrecording.Frame
	active    *websocket.Conn
}

func newMockRecordingEndpoint(t *testing.T) *mockRecordingEndpoint {
	t.Helper()
	m := &mockRecordingEndpoint{t: t}
	m.server = httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.server.Close)
	return m
}

func (m *mockRecordingEndpoint) endpointURL() string {
	return "ws" + strings.TrimPrefix(m.server.URL, "http") + "/session"
}

func (m *mockRecordingEndpoint) handle(w http.ResponseWriter, r *http.Request) {
	session, err := terminalrecording.ParseSessionQuery(r.URL.Query())
	if err != nil {
		m.t.Errorf("mock endpoint received invalid session query: %v", err)
	}
	up := websocket.Upgrader{}
	conn, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	m.mu.Lock()
	idx := len(m.fragments)
	m.fragments = append(m.fragments, nil)
	m.sessions = append(m.sessions, session)
	m.active = conn
	m.mu.Unlock()
	defer conn.Close()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		frame, err := terminalrecording.ParseFrame(data)
		if err != nil {
			m.t.Errorf("mock endpoint received invalid frame %s: %v", data, err)
			continue
		}
		m.mu.Lock()
		m.fragments[idx] = append(m.fragments[idx], frame)
		m.mu.Unlock()
	}
}

// killActive closes the most recently accepted connection server-side.
func (m *mockRecordingEndpoint) killActive() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active != nil {
		_ = m.active.Close()
	}
}

func (m *mockRecordingEndpoint) snapshotFragments() [][]terminalrecording.Frame {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]terminalrecording.Frame, len(m.fragments))
	for i, f := range m.fragments {
		out[i] = append([]terminalrecording.Frame(nil), f...)
	}
	return out
}

func (m *mockRecordingEndpoint) allFrames() []terminalrecording.Frame {
	var out []terminalrecording.Frame
	for _, f := range m.snapshotFragments() {
		out = append(out, f...)
	}
	return out
}

func (m *mockRecordingEndpoint) snapshotSessions() []terminalrecording.Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]terminalrecording.Session(nil), m.sessions...)
}

// assertWellFormed checks the producer's invariants: consecutive seqs within a fragment,
// increasing seqs across fragments, non-decreasing ts.
func assertWellFormed(t *testing.T, fragments [][]terminalrecording.Frame) {
	t.Helper()
	prevSeq := -1
	prevTs := -1.0
	for fi, fragment := range fragments {
		for i, f := range fragment {
			assert.Greater(t, int(f.Seq), prevSeq, "fragment %d frame %d: seq went backwards", fi, i)
			if i > 0 {
				assert.Equal(t, prevSeq+1, int(f.Seq), "fragment %d frame %d: seq gap within a fragment", fi, i)
			}
			assert.GreaterOrEqual(t, f.Ts, prevTs, "fragment %d frame %d: ts went backwards", fi, i)
			prevSeq = int(f.Seq)
			prevTs = f.Ts
		}
	}
}

func TestSessionRecorderStreamsFrames(t *testing.T) {
	t.Parallel()
	ep := newMockRecordingEndpoint(t)
	meta := testSessionMeta()
	dialURL, err := recordingDialURL(ep.endpointURL(), meta)
	require.NoError(t, err)

	r := newSessionRecorder(dialURL, meta.StartTime, testRecorderLogger())
	r.recordOutput("$ ls\r\n")
	r.recordResize(120, 40)
	r.recordOutput("app.py\r\n")
	r.Close()

	require.Eventually(t, func() bool { return len(ep.allFrames()) == 4 }, 5*time.Second, 10*time.Millisecond)
	frames := ep.allFrames()

	assert.Equal(t, terminalrecording.OperationStdout, frames[0].Operation)
	assert.Equal(t, uint64(0), frames[0].Seq)
	assert.Equal(t, "$ ls\r\n", frames[0].Data)

	assert.Equal(t, terminalrecording.OperationResize, frames[1].Operation)
	assert.Equal(t, uint64(1), frames[1].Seq)
	assert.Equal(t, uint16(120), frames[1].Cols)
	assert.Equal(t, uint16(40), frames[1].Rows)

	assert.Equal(t, terminalrecording.OperationStdout, frames[2].Operation)
	assert.Equal(t, "app.py\r\n", frames[2].Data)

	assert.Equal(t, terminalrecording.OperationEnd, frames[3].Operation)
	assert.Equal(t, uint64(3), frames[3].Seq)
	require.NotNil(t, frames[3].Dropped)
	assert.Equal(t, int64(0), *frames[3].Dropped)

	assertWellFormed(t, ep.snapshotFragments())

	sessions := ep.snapshotSessions()
	require.Len(t, sessions, 1)
	assert.Equal(t, meta.ID, sessions[0].ID)
	assert.Equal(t, meta.App, sessions[0].App)
	assert.Equal(t, meta.User, sessions[0].User)
	assert.Equal(t, meta.Cluster, sessions[0].Cluster)
	assert.Equal(t, meta.Namespace, sessions[0].Namespace)
	assert.Equal(t, meta.Pod, sessions[0].Pod)
	assert.Equal(t, meta.Container, sessions[0].Container)
	assert.Equal(t, meta.StartTime.Unix(), sessions[0].StartTime.Unix())
}

func TestSessionRecorderRedialsAsNewFragment(t *testing.T) {
	t.Parallel()
	ep := newMockRecordingEndpoint(t)
	meta := testSessionMeta()
	dialURL, err := recordingDialURL(ep.endpointURL(), meta)
	require.NoError(t, err)

	r := newSessionRecorder(dialURL, meta.StartTime, testRecorderLogger())
	r.recordOutput("before disconnect")
	require.Eventually(t, func() bool { return len(ep.allFrames()) >= 1 }, 5*time.Second, 10*time.Millisecond)

	ep.killActive()

	// Keep producing frames until the redial lands and the second fragment starts filling.
	require.Eventually(t, func() bool {
		r.recordOutput("after disconnect")
		fragments := ep.snapshotFragments()
		return len(fragments) >= 2 && len(fragments[len(fragments)-1]) > 0
	}, 15*time.Second, 50*time.Millisecond)

	r.Close()
	require.Eventually(t, func() bool {
		frames := ep.allFrames()
		return len(frames) > 0 && frames[len(frames)-1].Operation == terminalrecording.OperationEnd
	}, 5*time.Second, 10*time.Millisecond)

	fragments := ep.snapshotFragments()
	require.GreaterOrEqual(t, len(fragments), 2)
	assertWellFormed(t, fragments)

	// Every fragment of the session presents the same identity.
	sessions := ep.snapshotSessions()
	for _, s := range sessions {
		assert.Equal(t, meta.ID, s.ID)
	}
}

func TestSessionRecorderUnreachableEndpointNeverBlocks(t *testing.T) {
	t.Parallel()
	// Port 1 on localhost refuses connections immediately.
	r := newSessionRecorder("ws://127.0.0.1:1/session", time.Now(), testRecorderLogger())
	for range 10 {
		r.recordOutput("data")
	}
	start := time.Now()
	r.Close()
	assert.Less(t, time.Since(start), 3*time.Second, "Close must not wait out the full backoff schedule")
}

func TestSessionRecorderPacesRedialsWhenConnectionsDieYoung(t *testing.T) {
	t.Parallel()
	// An endpoint that accepts every dial but immediately drops the connection,
	// like one whose sink persistently fails.
	var mu sync.Mutex
	dials := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		dials++
		mu.Unlock()
		up := websocket.Upgrader{}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_ = conn.Close()
	}))
	t.Cleanup(srv.Close)

	r := newSessionRecorder("ws"+strings.TrimPrefix(srv.URL, "http")+"/session", time.Now(), testRecorderLogger())
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		r.recordOutput("spam")
		time.Sleep(5 * time.Millisecond)
	}
	r.Close()

	mu.Lock()
	defer mu.Unlock()
	// The first dial is immediate; each young death then waits out the backoff (1s, 2s,
	// ...), so 1.5s of failures admits only the first few connections.
	assert.LessOrEqual(t, dials, 4, "young connection deaths must be paced by redial backoff")
}

func TestSessionRecorderDropsWhenBufferFull(t *testing.T) {
	t.Parallel()
	// No egress goroutine: frames stay queued so the buffer fills deterministically.
	r := &sessionRecorder{
		startTime: time.Now(),
		logger:    testRecorderLogger(),
		frames:    make(chan terminalrecording.Frame, 2),
		done:      make(chan struct{}),
		stop:      make(chan struct{}),
	}
	r.recordOutput("a")
	r.recordOutput("b")
	r.recordOutput("c")
	r.recordOutput("d")

	r.mu.Lock()
	assert.Equal(t, uint64(2), r.nextSeq, "dropped frames must not consume seqs")
	assert.Equal(t, int64(2), r.dropped)
	r.mu.Unlock()

	f := <-r.frames
	assert.Equal(t, uint64(0), f.Seq)
	assert.Equal(t, "a", f.Data)
	f = <-r.frames
	assert.Equal(t, uint64(1), f.Seq)
	assert.Equal(t, "b", f.Data)

	// With space freed, the next frame takes seq 2: drops consumed no seqs.
	r.recordOutput("e")
	f = <-r.frames
	assert.Equal(t, uint64(2), f.Seq)
	assert.Equal(t, "e", f.Data)
}

func TestSessionRecorderConcurrentProducers(t *testing.T) {
	t.Parallel()
	ep := newMockRecordingEndpoint(t)
	meta := testSessionMeta()
	dialURL, err := recordingDialURL(ep.endpointURL(), meta)
	require.NoError(t, err)
	r := newSessionRecorder(dialURL, meta.StartTime, testRecorderLogger())

	const writers = 4
	const outputsPerWriter = 100
	const resizes = 50
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range outputsPerWriter {
				r.recordOutput(fmt.Sprintf("w%d-%d", w, i))
			}
		})
	}
	wg.Go(func() {
		for i := range resizes {
			r.recordResize(uint16(80+i), 24)
		}
	})
	wg.Wait()
	r.Close()

	require.Eventually(t, func() bool {
		frames := ep.allFrames()
		return len(frames) > 0 && frames[len(frames)-1].Operation == terminalrecording.OperationEnd
	}, 5*time.Second, 10*time.Millisecond)

	fragments := ep.snapshotFragments()
	assertWellFormed(t, fragments)

	frames := ep.allFrames()
	end := frames[len(frames)-1]
	require.NotNil(t, end.Dropped)
	// One connection, so no transit loss: seq'd frames plus counted drops account for
	// every attempt.
	assert.Equal(t, int64(writers*outputsPerWriter+resizes), int64(end.Seq)+*end.Dropped)
	assert.Equal(t, int(end.Seq), len(frames)-1, "received data frames must match seqs consumed")
}

func TestStartSessionRecorderMintsIdentity(t *testing.T) {
	t.Parallel()
	ep := newMockRecordingEndpoint(t)
	meta := testSessionMeta()
	meta.ID = ""
	meta.StartTime = time.Time{}

	r, err := startSessionRecorder(meta, ep.endpointURL())
	require.NoError(t, err)
	r.recordOutput("hello")
	r.Close()

	require.Eventually(t, func() bool { return len(ep.snapshotSessions()) == 1 }, 5*time.Second, 10*time.Millisecond)
	session := ep.snapshotSessions()[0]
	assert.Regexp(t, `^[0-9a-f]{16}$`, session.ID)
	assert.False(t, session.StartTime.IsZero())
	assert.Equal(t, meta.App, session.App)
}

func TestRecordingDialURL(t *testing.T) {
	t.Parallel()
	meta := testSessionMeta()
	dialURL, err := recordingDialURL("ws://localhost:8090/session", meta)
	require.NoError(t, err)

	u, err := url.Parse(dialURL)
	require.NoError(t, err)
	assert.Equal(t, "ws", u.Scheme)
	assert.Equal(t, "localhost:8090", u.Host)
	assert.Equal(t, "/session", u.Path)

	parsed, err := terminalrecording.ParseSessionQuery(u.Query())
	require.NoError(t, err)
	assert.Equal(t, meta.ID, parsed.ID)
	assert.Equal(t, meta.App, parsed.App)

	_, err = recordingDialURL("://not-a-url", meta)
	require.Error(t, err)
}
