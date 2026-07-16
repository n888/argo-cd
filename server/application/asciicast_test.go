package application

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAsciicastRecorder(t *testing.T) {
	logger, hook := test.NewNullLogger()
	entry := logger.WithField("test", "asciicast")

	recorder := newAsciicastRecorder(time.Now(), entry, true, nil)
	recorder.recordResize(80, 24) // first event sends the header
	recorder.recordOutput("hello")
	recorder.recordResize(100, 40)
	recorder.Close() // flush the background writer before asserting

	require.Len(t, hook.Entries, 3)
	assert.Contains(t, hook.Entries[0].Message, "{\"height\":24,\"timestamp\":")
	assert.Contains(t, hook.Entries[0].Message, "\"version\":2,\"width\":80}")
	assert.Contains(t, hook.Entries[1].Message, "\"o\",\"hello\"]")
	assert.Contains(t, hook.Entries[2].Message, "\"r\",\"100x40\"]")

	// Verify each frame is valid JSON.
	for i, e := range hook.Entries {
		message := e.Message
		var data any
		require.NoError(t, json.Unmarshal([]byte(message), &data), "Entry %d should be valid JSON", i)
	}
}

func TestAsciicastRecorder_OutputBeforeResize(t *testing.T) {
	logger, hook := test.NewNullLogger()
	entry := logger.WithField("test", "asciicast")

	recorder := newAsciicastRecorder(time.Now(), entry, true, nil)
	// Output before any resize still records, lazily emitting a header with the default
	// terminal size so early output is not lost.
	recorder.recordOutput("hello")
	// A later resize emits a resize frame rather than a second header.
	recorder.recordResize(100, 40)
	recorder.Close()

	require.Len(t, hook.Entries, 3)
	assert.Contains(t, hook.Entries[0].Message, "\"version\":2,\"width\":80}")
	assert.Contains(t, hook.Entries[1].Message, "\"o\",\"hello\"")
	assert.Contains(t, hook.Entries[2].Message, "\"r\",\"100x40\"")
}

func TestAsciicastRecorder_File(t *testing.T) {
	tempFile, err := os.CreateTemp(t.TempDir(), "asciicast-test-*.cast")
	require.NoError(t, err)

	recorder := newAsciicastRecorder(time.Now(), log.WithField("test", "asciicast"), false, tempFile)
	recorder.recordResize(80, 24)
	recorder.recordOutput("hello world")
	recorder.Close() // flushes and closes the file

	content, err := os.ReadFile(tempFile.Name())
	require.NoError(t, err)
	assert.Contains(t, string(content), "\"version\":2")
	assert.Contains(t, string(content), "\"o\",\"hello world\"")
}

// blockingSink blocks the first Write until release is closed, simulating a hung sink.
type blockingSink struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingSink) Write(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.release
	return len(p), nil
}

func (b *blockingSink) Close() error { return nil }

// TestAsciicastRecorder_SlowSinkDoesNotBlock verifies that a slow/hung recording sink does not
// stall the producer (terminalSession.Write), so the interactive terminal stays responsive.
// Frames beyond the buffer are dropped rather than queued.
func TestAsciicastRecorder_SlowSinkDoesNotBlock(t *testing.T) {
	sink := &blockingSink{started: make(chan struct{}), release: make(chan struct{})}
	recorder := newAsciicastRecorder(time.Now(), log.WithField("test", "slowsink"), false, sink)

	// Prime the writer so it parks inside a blocked Write on the hung sink.
	recorder.recordOutput("first")
	<-sink.started

	// With recording decoupled, producing many frames against the hung sink must not block.
	done := make(chan struct{})
	go func() {
		for range recorderFrameBufferSize + 100 {
			recorder.recordOutput(strings.Repeat("x", 256))
		}
		close(done)
	}()

	select {
	case <-done:
		// producer never blocked on the hung sink
	case <-time.After(2 * time.Second):
		close(sink.release) // avoid leaking the blocked writer goroutine
		t.Fatal("recordOutput blocked on a slow sink; recording I/O is not decoupled")
	}

	// Frames beyond the buffer should have been dropped, not queued unbounded.
	recorder.mu.Lock()
	dropped := recorder.dropped
	recorder.mu.Unlock()
	assert.Positive(t, dropped, "expected frames to be dropped while the sink is hung")

	close(sink.release) // unblock the writer so Close can flush and join
	recorder.Close()
}

func TestOpenRecordingFile(t *testing.T) {
	// 2026-07-15 01:30 at UTC+2 is 2026-07-14 23:30 UTC; the dated directory and the filename
	// timestamp must both use the UTC date.
	startTime := time.Date(2026, time.July, 15, 1, 30, 0, 0, time.FixedZone("UTC+2", 2*60*60))

	t.Run("date template creates dated directory", func(t *testing.T) {
		base := t.TempDir()
		f, err := openRecordingFile(base+"/{{.Year}}/{{.Month}}/{{.Day}}", startTime, "default/guestbook", "alice:admin", "pod-1", "main")
		require.NoError(t, err)
		defer f.Close()

		dir := filepath.Join(base, "2026", "07", "14")
		info, err := os.Stat(dir)
		require.NoError(t, err)
		assert.True(t, info.IsDir())
		assert.Equal(t, os.FileMode(0o700), info.Mode().Perm())

		// Session identifiers are sanitized (":" and "/" -> "_") and the timestamp is UTC.
		assert.Equal(t, filepath.Join(dir, "default_guestbook-alice_admin-pod-1-main-20260714-233000.000.cast"), f.Name())
		fileInfo, err := os.Stat(f.Name())
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), fileInfo.Mode().Perm())
	})

	t.Run("plain path works unchanged", func(t *testing.T) {
		base := t.TempDir()
		f, err := openRecordingFile(base, startTime, "app", "user", "pod", "main")
		require.NoError(t, err)
		defer f.Close()
		assert.Equal(t, base, filepath.Dir(f.Name()))
	})

	t.Run("invalid template returns error", func(t *testing.T) {
		_, err := openRecordingFile(t.TempDir()+"/{{.Year", startTime, "app", "user", "pod", "main")
		require.ErrorContains(t, err, "failed to render recording path template")
	})

	t.Run("directory creation failure returns error", func(t *testing.T) {
		blocker := filepath.Join(t.TempDir(), "blocker")
		require.NoError(t, os.WriteFile(blocker, []byte("not a directory"), 0o600))
		_, err := openRecordingFile(blocker+"/{{.Year}}", startTime, "app", "user", "pod", "main")
		require.ErrorContains(t, err, "failed to create recording directory")
	})
}
