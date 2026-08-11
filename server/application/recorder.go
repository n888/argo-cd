package application

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"

	"github.com/argoproj/argo-cd/v3/util/terminalrecording"
)

const (
	// recorderFrameBufferSize bounds frames queued for the egress goroutine. When full,
	// frames are dropped rather than blocking, so a slow or unreachable recording endpoint
	// cannot stall the interactive terminal.
	recorderFrameBufferSize = 1024
	// recorderFlushTimeout bounds how long session teardown waits for the egress goroutine
	// to flush, so an unreachable endpoint cannot wedge session cleanup.
	recorderFlushTimeout = 5 * time.Second
	// recorderDialTimeout bounds a single dial attempt.
	recorderDialTimeout = 2 * time.Second
	// recorderWriteTimeout bounds a single frame write, so a hung endpoint surfaces as a
	// write error and a redial instead of blocking the egress goroutine.
	recorderWriteTimeout = 10 * time.Second
	// recorderRedialInitialBackoff and recorderRedialMaxBackoff pace redials, both when
	// dials fail and when established connections keep dying young. Producers are never
	// paced by this: they keep filling the buffer and then drop.
	recorderRedialInitialBackoff = time.Second
	recorderRedialMaxBackoff     = 30 * time.Second
	// recorderHealthyConnAge is how long a connection must live for its loss to be treated
	// as an isolated failure, redialed immediately. Connections dying younger indicate an
	// endpoint that accepts dials but cannot keep a connection (e.g. its sink cannot
	// write), so those redials wait out the backoff; without that pacing, dial succeeding
	// instantly means the producer would churn a connection per frame at round-trip speed.
	recorderHealthyConnAge = 30 * time.Second
)

// sessionRecorder streams one terminal session's recording frames to the recording
// endpoint. Producers (Write and the resize path of Read) enqueue frames onto a bounded
// channel under a mutex; the egress goroutine owns all network I/O, so a slow endpoint
// never blocks the terminal. The mutex makes check-closed/assign-seq/stamp-ts/send atomic,
// serializing the producers against each other and against Close, and making enqueue order
// equal seq and timestamp order. On connection loss the egress redials and continues as a
// new fragment; seqs continue across fragments, so in-flight loss surfaces as a seq gap.
type sessionRecorder struct {
	dialURL   string
	logger    *log.Entry
	startTime time.Time

	frames chan terminalrecording.Frame
	done   chan struct{} // closed by the egress goroutine after its final flush attempt
	stop   chan struct{} // closed by Close to abort dial and redial backoff waits

	// Egress-goroutine state: touched only by run/deliver/connect, so unlocked.
	connectedAt time.Time     // when the current connection was established
	redialWait  time.Duration // backoff before the next redial; 0 means redial immediately

	mu       sync.Mutex
	closed   bool
	nextSeq  uint64
	dropped  int64
	endFrame terminalrecording.Frame // set by Close before frames is closed; read by egress after drain
}

// startSessionRecorder mints meta's ID and StartTime, builds the dial URL, and starts a
// recorder.
func startSessionRecorder(meta terminalrecording.Session, endpoint string) (*sessionRecorder, error) {
	id, err := terminalrecording.NewSessionID()
	if err != nil {
		return nil, err
	}
	meta.ID = id
	meta.StartTime = time.Now()
	dialURL, err := recordingDialURL(endpoint, meta)
	if err != nil {
		return nil, err
	}
	logger := log.WithFields(log.Fields{
		"terminal_session_id":        meta.ID,
		"terminal_session_app":       meta.App,
		"terminal_session_user":      meta.User,
		"terminal_session_cluster":   meta.Cluster,
		"terminal_session_namespace": meta.Namespace,
		"terminal_session_pod":       meta.Pod,
		"terminal_session_container": meta.Container,
	})
	r := newSessionRecorder(dialURL, meta.StartTime, logger)
	logger.Info("recording terminal session")
	return r, nil
}

// recordingDialURL combines the endpoint base URL with the session's query parameters.
func recordingDialURL(endpoint string, session terminalrecording.Session) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("failed to parse recording endpoint URL: %w", err)
	}
	u.RawQuery = session.Query().Encode()
	return u.String(), nil
}

// newSessionRecorder builds a recorder and starts its egress goroutine.
func newSessionRecorder(dialURL string, startTime time.Time, logger *log.Entry) *sessionRecorder {
	r := &sessionRecorder{
		dialURL:   dialURL,
		logger:    logger,
		startTime: startTime,
		frames:    make(chan terminalrecording.Frame, recorderFrameBufferSize),
		done:      make(chan struct{}),
		stop:      make(chan struct{}),
	}
	go r.run()
	return r
}

// recordOutput enqueues a terminal output frame.
func (r *sessionRecorder) recordOutput(data string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return
	}
	ts := time.Since(r.startTime).Seconds()
	r.enqueueLocked(terminalrecording.NewStdoutFrame(r.nextSeq, ts, data))
}

// recordResize enqueues a terminal resize frame.
func (r *sessionRecorder) recordResize(cols, rows uint16) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return
	}
	ts := time.Since(r.startTime).Seconds()
	r.enqueueLocked(terminalrecording.NewResizeFrame(r.nextSeq, ts, cols, rows))
}

// enqueueLocked hands a frame to the egress goroutine without blocking: when the buffer is
// full the frame is dropped and counted, and its seq is not consumed. Callers must hold r.mu.
func (r *sessionRecorder) enqueueLocked(f terminalrecording.Frame) {
	select {
	case r.frames <- f:
		r.nextSeq++
	default:
		r.dropped++
		if r.dropped == 1 {
			r.logger.Warn("recording buffer full; dropping frames to avoid stalling the terminal (recording will be incomplete)")
		}
	}
}

// Close seals the frame stream with an end frame, then waits up to recorderFlushTimeout
// for the egress goroutine to deliver what it can.
func (r *sessionRecorder) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.endFrame = terminalrecording.NewEndFrame(r.nextSeq, time.Since(r.startTime).Seconds(), r.dropped)
	dropped := r.dropped
	close(r.frames) // safe under r.mu: producers check r.closed under the same lock before sending
	r.mu.Unlock()
	close(r.stop)

	select {
	case <-r.done:
	case <-time.After(recorderFlushTimeout):
		r.logger.Warnf("recording flush timed out after %s; the recording tail may be missing", recorderFlushTimeout)
	}
	if dropped > 0 {
		r.logger.Warnf("recording dropped %d frame(s) due to a slow or unreachable recording endpoint; recording is incomplete", dropped)
	}
}

// run is the egress goroutine: it drains the buffer onto the endpoint connection and
// delivers the end frame after the buffer is sealed.
func (r *sessionRecorder) run() {
	defer close(r.done)
	var conn *websocket.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	gaveUp := false
	undelivered := 0
	for f := range r.frames {
		if gaveUp {
			undelivered++
			continue
		}
		if conn = r.deliver(conn, f); conn == nil {
			gaveUp = true
			undelivered++
		}
	}
	if !gaveUp {
		if conn = r.deliver(conn, r.endFrame); conn != nil {
			// Best-effort graceful close so the endpoint finalizes the fragment promptly.
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		}
	}
	if undelivered > 0 {
		r.logger.Warnf("%d recording frame(s) could not be delivered before shutdown; the recording tail is missing", undelivered)
	}
}

// deliver writes one frame, redialing as needed, and returns the connection it succeeded
// on. A failed write is retried on the next connection so the frame's seq is not lost.
// Redials are immediate after a healthy connection is lost, but paced by capped backoff
// while connections keep dying young. It returns nil only when the recorder is stopping
// before the frame could be delivered.
func (r *sessionRecorder) deliver(conn *websocket.Conn, f terminalrecording.Frame) *websocket.Conn {
	for {
		if conn == nil {
			conn = r.connect()
			if conn == nil {
				return nil
			}
			r.connectedAt = time.Now()
		}
		_ = conn.SetWriteDeadline(time.Now().Add(recorderWriteTimeout))
		if err := conn.WriteJSON(f); err != nil {
			r.logger.Warnf("recording connection lost, redialing (recording continues as a new fragment): %v", err)
			_ = conn.Close()
			conn = nil
			if time.Since(r.connectedAt) >= recorderHealthyConnAge {
				r.redialWait = 0
			} else if !r.pauseBeforeRedial() {
				return nil
			}
			continue
		}
		return conn
	}
}

// pauseBeforeRedial waits out the redial backoff after a connection died young, doubling
// it up to recorderRedialMaxBackoff for the next such loss. It returns false when the
// recorder starts stopping during the wait.
func (r *sessionRecorder) pauseBeforeRedial() bool {
	if r.redialWait == 0 {
		r.redialWait = recorderRedialInitialBackoff
	}
	select {
	case <-r.stop:
		return false
	case <-time.After(r.redialWait):
	}
	r.redialWait = min(r.redialWait*2, recorderRedialMaxBackoff)
	return true
}

// connect dials the recording endpoint with capped exponential backoff until it succeeds
// or the recorder is stopping.
func (r *sessionRecorder) connect() *websocket.Conn {
	dialer := websocket.Dialer{HandshakeTimeout: recorderDialTimeout}
	backoff := recorderRedialInitialBackoff
	warned := false
	for {
		conn, _, err := dialer.DialContext(context.Background(), r.dialURL, nil)
		if err == nil {
			r.logger.Debug("connected to recording endpoint")
			return conn
		}
		if warned {
			r.logger.Debugf("still failing to dial recording endpoint: %v", err)
		} else {
			r.logger.Warnf("failed to dial recording endpoint (will keep retrying with backoff): %v", err)
			warned = true
		}
		select {
		case <-r.stop:
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, recorderRedialMaxBackoff)
	}
}
