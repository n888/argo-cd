package application

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/argoproj/argo-cd/v3/common"
	httputil "github.com/argoproj/argo-cd/v3/util/http"
	"github.com/argoproj/argo-cd/v3/util/rbac"
	util_session "github.com/argoproj/argo-cd/v3/util/session"
	"github.com/argoproj/argo-cd/v3/util/settings"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/tools/remotecommand"
)

const (
	ReconnectCode    = 1
	ReconnectMessage = "\nReconnect because the token was refreshed...\n"
)

var upgrader = func() websocket.Upgrader {
	upgrader := websocket.Upgrader{}
	upgrader.HandshakeTimeout = time.Second * 2
	upgrader.CheckOrigin = func(_ *http.Request) bool {
		return true
	}
	return upgrader
}()

// terminalSession implements PtyHandler
type terminalSession struct {
	ctx            context.Context
	wsConn         *websocket.Conn
	sizeChan       chan remotecommand.TerminalSize
	doneChan       chan struct{}
	readLock       sync.Mutex
	writeLock      sync.Mutex
	sessionManager *util_session.SessionManager
	token          *string
	appRBACName    string
	terminalOpts   *TerminalOptions
	recorder       *asciicastRecorder
}

// getToken get auth token from web socket request
func getToken(r *http.Request) (string, error) {
	cookies := r.Cookies()
	return httputil.JoinCookies(common.AuthCookieName, cookies)
}

// newTerminalSession create terminalSession
func newTerminalSession(ctx context.Context, w http.ResponseWriter, r *http.Request, responseHeader http.Header, sessionManager *util_session.SessionManager, appRBACName string, userName string, podName string, container string, terminalOpts *TerminalOptions) (*terminalSession, error) {
	token, err := getToken(r)
	if err != nil {
		return nil, err
	}

	conn, err := upgrader.Upgrade(w, r, responseHeader)
	if err != nil {
		return nil, err
	}
	session := &terminalSession{
		ctx:            ctx,
		wsConn:         conn,
		sizeChan:       make(chan remotecommand.TerminalSize),
		doneChan:       make(chan struct{}),
		sessionManager: sessionManager,
		token:          &token,
		appRBACName:    appRBACName,
		terminalOpts:   terminalOpts,
	}
	if terminalOpts.RecordingEnabled {
		startTime := time.Now()
		logger := log.WithFields(log.Fields{
			"terminal_session_app":       appRBACName,
			"terminal_session_user":      userName,
			"terminal_session_pod":       podName,
			"terminal_session_container": container,
		})
		switch terminalOpts.RecordingOutput {
		case settings.RecordingOutputFile:
			f, err := openRecordingFile(terminalOpts.RecordingPath, startTime, appRBACName, userName, podName, container)
			if err != nil {
				logger.Errorf("disabling recording for this session: %v", err)
			} else {
				session.recorder = newAsciicastRecorder(startTime, logger, false, f)
				logger.Infof("recording session to %s", f.Name())
			}
		case settings.RecordingOutputStdout:
			session.recorder = newAsciicastRecorder(startTime, logger, true, nil)
		}
	}

	return session, nil
}

// openRecordingFile renders the recording path template, creates the resulting directory, and
// opens the .cast file for this session. Timestamps in both the directory template and the
// filename use UTC so recordings land in deterministic date directories across replicas.
func openRecordingFile(pathTemplate string, startTime time.Time, appRBACName, userName, podName, container string) (*os.File, error) {
	dir, err := settings.RenderTerminalSessionRecordingPath(pathTemplate, startTime)
	if err != nil {
		return nil, fmt.Errorf("failed to render recording path template: %w", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create recording directory %s: %w", dir, err)
	}
	timestamp := startTime.UTC().Format("20060102-150405.000")
	// Build the basename from the session identifiers, then sanitize the whole thing:
	// ":" and "/" become "_" so no component can inject a path separator and escape
	// the recording directory. Sanitizing the basename (not the joined path) keeps the
	// rendered recording directory's own separators intact.
	sanitizer := strings.NewReplacer(":", "_", "/", "_")
	basename := sanitizer.Replace(fmt.Sprintf("%s-%s-%s-%s-%s.cast", appRBACName, userName, podName, container, timestamp))
	filename := filepath.Join(dir, basename)
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to open recording file %s: %w", filename, err)
	}
	return f, nil
}

// Done closes the done channel and flushes/closes the recorder.
func (t *terminalSession) Done() {
	if t.recorder != nil {
		t.recorder.Close()
	}
	close(t.doneChan)
}

func (t *terminalSession) StartKeepalives(dur time.Duration) {
	ticker := time.NewTicker(dur)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := t.Ping()
			if err != nil {
				log.Errorf("ping error: %v", err)
				return
			}
		case <-t.doneChan:
			return
		}
	}
}

// Next called in a loop from remotecommand as long as the process is running
func (t *terminalSession) Next() *remotecommand.TerminalSize {
	select {
	case size := <-t.sizeChan:
		return &size
	case <-t.doneChan:
		return nil
	}
}

// reconnect send reconnect code to client and ask them init new ws session
func (t *terminalSession) reconnect() (int, error) {
	reconnectCommand, _ := json.Marshal(TerminalCommand{
		Code: ReconnectCode,
	})
	reconnectMessage, _ := json.Marshal(TerminalMessage{
		Operation: "stdout",
		Data:      ReconnectMessage,
	})
	t.writeLock.Lock()
	err := t.wsConn.WriteMessage(websocket.TextMessage, reconnectMessage)
	if err != nil {
		log.Errorf("write message err: %v", err)
		return 0, err
	}
	err = t.wsConn.WriteMessage(websocket.TextMessage, reconnectCommand)
	if err != nil {
		log.Errorf("write message err: %v", err)
		return 0, err
	}
	t.writeLock.Unlock()
	return 0, nil
}

func (t *terminalSession) validatePermissions(p []byte) (int, error) {
	permissionDeniedMessage, _ := json.Marshal(TerminalMessage{
		Operation: "stdout",
		Data:      "Permission denied",
	})
	if err := t.terminalOpts.Enf.EnforceErr(t.ctx.Value("claims"), rbac.ResourceApplications, rbac.ActionGet, t.appRBACName); err != nil {
		err = t.wsConn.WriteMessage(websocket.TextMessage, permissionDeniedMessage)
		if err != nil {
			log.Errorf("permission denied message err: %v", err)
		}
		return copy(p, EndOfTransmission), common.PermissionDeniedAPIError
	}

	if err := t.terminalOpts.Enf.EnforceErr(t.ctx.Value("claims"), rbac.ResourceExec, rbac.ActionCreate, t.appRBACName); err != nil {
		err = t.wsConn.WriteMessage(websocket.TextMessage, permissionDeniedMessage)
		if err != nil {
			log.Errorf("permission denied message err: %v", err)
		}
		return copy(p, EndOfTransmission), common.PermissionDeniedAPIError
	}
	return 0, nil
}

func (t *terminalSession) performValidationsAndReconnect(p []byte) (int, error) {
	// In disable auth mode, no point verifying the token or validating permissions
	if t.terminalOpts.DisableAuth {
		return 0, nil
	}

	// check if token still valid
	_, newToken, err := t.sessionManager.VerifyToken(t.ctx, *t.token)
	// err in case if token is revoked, newToken in case if refresh happened
	if err != nil || newToken != "" {
		// need to send reconnect code in case if token was refreshed
		return t.reconnect()
	}
	code, err := t.validatePermissions(p)
	if err != nil {
		return code, err
	}

	return 0, nil
}

// Read called in a loop from remote command as long as the process is running
func (t *terminalSession) Read(p []byte) (int, error) {
	code, err := t.performValidationsAndReconnect(p)
	if err != nil {
		return code, err
	}

	t.readLock.Lock()
	_, message, err := t.wsConn.ReadMessage()
	t.readLock.Unlock()
	if err != nil {
		if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
			log.Errorf("unexpected closer error: %v", err)
			return copy(p, EndOfTransmission), err
		}
		log.Errorf("read message error: %v", err)
		return copy(p, EndOfTransmission), err
	}
	var msg TerminalMessage
	if err := json.Unmarshal(message, &msg); err != nil {
		log.Errorf("read parse message err: %v", err)
		return copy(p, EndOfTransmission), err
	}
	switch msg.Operation {
	case "stdin":
		return copy(p, msg.Data), nil
	case "resize":
		t.sizeChan <- remotecommand.TerminalSize{Width: msg.Cols, Height: msg.Rows}
		if t.recorder != nil {
			t.recorder.recordResize(msg.Cols, msg.Rows)
		}
		return 0, nil
	default:
		return copy(p, EndOfTransmission), fmt.Errorf("unknown message type %s", msg.Operation)
	}
}

// Ping called periodically to ensure connection stays alive through load balancers
func (t *terminalSession) Ping() error {
	t.writeLock.Lock()
	err := t.wsConn.WriteMessage(websocket.PingMessage, []byte("ping"))
	t.writeLock.Unlock()
	if err != nil {
		log.Errorf("ping message err: %v", err)
	}
	return err
}

// Write called from remote command whenever there is any output
func (t *terminalSession) Write(p []byte) (int, error) {
	data := string(p)
	if t.recorder != nil {
		t.recorder.recordOutput(data)
	}
	msg, err := json.Marshal(TerminalMessage{
		Operation: "stdout",
		Data:      data,
	})
	if err != nil {
		log.Errorf("write parse message err: %v", err)
		return 0, err
	}
	t.writeLock.Lock()
	err = t.wsConn.WriteMessage(websocket.TextMessage, msg)
	t.writeLock.Unlock()
	if err != nil {
		log.Errorf("write message err: %v", err)
		return 0, err
	}
	return len(p), nil
}

// Close closes websocket connection
func (t *terminalSession) Close() error {
	return t.wsConn.Close()
}

const (
	// defaultRecordingCols and defaultRecordingRows are the terminal dimensions recorded in
	// the asciicast header when output is captured before the client sends its initial resize.
	defaultRecordingCols uint16 = 80
	defaultRecordingRows uint16 = 24

	// recorderFrameBufferSize bounds how many asciicast frames may queue for the background
	// writer before frames are dropped. Dropping (rather than blocking the producer) keeps a
	// slow recording sink from stalling the interactive terminal.
	recorderFrameBufferSize = 1024
	// recorderFlushTimeout bounds how long session teardown waits for the writer to flush
	// buffered frames and close the sink, so a hung sink cannot wedge session cleanup.
	recorderFlushTimeout = 5 * time.Second
)

// asciicastRecorder serializes a terminal session into asciicast v2 frames. Frames are
// marshalled on the calling goroutine, keeping their timestamps accurate, and handed to a
// dedicated writer goroutine over a bounded channel, so slow sink I/O never blocks the
// interactive terminal. When the buffer is full, frames are dropped rather than queued.
type asciicastRecorder struct {
	startTime   time.Time
	logger      *log.Entry
	logToStdout bool
	sink        io.WriteCloser // optional file sink; nil for stdout-only recording

	frames chan string
	done   chan struct{} // closed by the writer goroutine once it has flushed and closed the sink

	mu         sync.Mutex
	headerSent bool
	closed     bool
	dropped    int
}

// newAsciicastRecorder builds a recorder and starts its background writer goroutine. sink may
// be nil to record to stdout logs only.
func newAsciicastRecorder(startTime time.Time, logger *log.Entry, logToStdout bool, sink io.WriteCloser) *asciicastRecorder {
	r := &asciicastRecorder{
		startTime:   startTime,
		logger:      logger,
		logToStdout: logToStdout,
		sink:        sink,
		frames:      make(chan string, recorderFrameBufferSize),
		done:        make(chan struct{}),
	}
	go r.run()
	return r
}

// recordOutput enqueues a terminal output frame.
func (r *asciicastRecorder) recordOutput(data string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return
	}
	if !r.headerSent {
		r.sendHeaderLocked(defaultRecordingCols, defaultRecordingRows)
	}
	ts := time.Since(r.startTime).Seconds()
	line, _ := json.Marshal([]any{ts, "o", data})
	r.enqueueLocked(string(line))
}

// recordResize enqueues a terminal resize event.
func (r *asciicastRecorder) recordResize(cols, rows uint16) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return
	}
	if !r.headerSent {
		r.sendHeaderLocked(cols, rows)
		return
	}
	ts := time.Since(r.startTime).Seconds()
	line, _ := json.Marshal([]any{ts, "r", fmt.Sprintf("%dx%d", cols, rows)})
	r.enqueueLocked(string(line))
}

// sendHeaderLocked writes the asciicast v2 header.
func (r *asciicastRecorder) sendHeaderLocked(cols, rows uint16) {
	header := map[string]any{
		"version":   2,
		"width":     cols,
		"height":    rows,
		"timestamp": r.startTime.Unix(),
	}
	line, _ := json.Marshal(header)
	r.enqueueLocked(string(line))
	r.headerSent = true
}

// enqueueLocked attempts to send a frame to the writer; drops the frame if the buffer is full.
// Callers must hold r.mu.
func (r *asciicastRecorder) enqueueLocked(line string) {
	select {
	case r.frames <- line:
	default:
		r.dropped++
		if r.dropped == 1 {
			r.logger.Warnf("recording buffer full; dropping frames to avoid stalling the terminal (recording will be incomplete)")
		}
	}
}

// run is the background writer that flushes frames to the sink.
func (r *asciicastRecorder) run() {
	defer close(r.done)
	if r.sink != nil {
		defer r.sink.Close()
	}
	var writeErrLogged bool
	for line := range r.frames {
		if r.logToStdout {
			r.logger.Infof("%s", line)
		}
		if r.sink != nil {
			if _, err := io.WriteString(r.sink, line+"\n"); err != nil && !writeErrLogged {
				r.logger.Errorf("failed to write recording frame, recording may be incomplete: %v", err)
				writeErrLogged = true
			}
		}
	}
}

// Close shuts down the recorder and waits for the buffer to flush.
func (r *asciicastRecorder) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	dropped := r.dropped
	close(r.frames) // safe under r.mu: producers check r.closed under the same lock before sending
	r.mu.Unlock()

	select {
	case <-r.done:
	case <-time.After(recorderFlushTimeout):
		r.logger.Warnf("recording flush timed out after %s; some frames may be lost", recorderFlushTimeout)
	}
	if dropped > 0 {
		r.logger.Warnf("recording dropped %d frame(s) due to a slow sink; recording is incomplete", dropped)
	}
}
