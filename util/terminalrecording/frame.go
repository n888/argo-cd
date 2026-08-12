// Package terminalrecording is the wire protocol for streaming terminal
// session recordings from argocd-server to a recording endpoint.
//
// Session identity is sent once per connection, in the dial URL query
// parameters (see Session). Frames wrap the stdout and resize payloads of the
// terminal UI protocol in a seq/ts envelope. Stdin is never sent.
//
// A connection carries one contiguous fragment of a recording. When it drops,
// the producer redials and the next fragment starts. Seqs are per session and
// only assigned to frames that actually entered the pipeline, so a seq gap
// between fragments means loss in transit, while drops at the source are
// counted in the end frame.
package terminalrecording

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Frame operations. Stdout and resize keep the operation names of the terminal
// UI's TerminalMessage protocol. The end operation only exists on the
// recording wire.
const (
	OperationStdout = "stdout"
	OperationResize = "resize"
	OperationEnd    = "end"
)

// Frame is one recording event on the wire. Operation decides which fields
// beyond Seq and Ts are set. Session identity is carried by the connection
// rather than repeated on every frame (see Session).
type Frame struct {
	Operation string `json:"operation"`
	// Seq numbers frames per session in enqueue order, continuing across
	// reconnects.
	Seq uint64 `json:"seq"`
	// Ts is seconds since session start, which is the Asciicast v2 time base.
	// Offsets being absolute, a lost frame doesn't shift the timing of
	// anything after it.
	Ts float64 `json:"ts"`

	// Data carries one chunk of terminal output on stdout frames.
	Data string `json:"data,omitempty"`

	// Cols and Rows carry the applied terminal dimensions on resize frames.
	Cols uint16 `json:"cols,omitempty"`
	Rows uint16 `json:"rows,omitempty"`

	// Dropped is set on end frames: how many frames the source dropped over
	// the whole session (buffer full). It's a pointer so end frames serialize
	// "dropped":0 while other operations omit the key.
	Dropped *int64 `json:"dropped,omitempty"`
}

// NewStdoutFrame builds a frame carrying one chunk of terminal output.
func NewStdoutFrame(seq uint64, ts float64, data string) Frame {
	return Frame{
		Operation: OperationStdout,
		Seq:       seq,
		Ts:        ts,
		Data:      data,
	}
}

// NewResizeFrame builds a frame recording an applied terminal resize.
func NewResizeFrame(seq uint64, ts float64, cols, rows uint16) Frame {
	return Frame{
		Operation: OperationResize,
		Seq:       seq,
		Ts:        ts,
		Cols:      cols,
		Rows:      rows,
	}
}

// NewEndFrame builds the frame that closes a session. seq is one past the
// last data frame's seq, and dropped is the session's source-drop count.
func NewEndFrame(seq uint64, ts float64, dropped int64) Frame {
	return Frame{
		Operation: OperationEnd,
		Seq:       seq,
		Ts:        ts,
		Dropped:   &dropped,
	}
}

// ParseFrame decodes and validates one wire frame.
func ParseFrame(data []byte) (Frame, error) {
	var f Frame
	if err := json.Unmarshal(data, &f); err != nil {
		return Frame{}, fmt.Errorf("failed to unmarshal recording frame: %w", err)
	}
	if err := f.Validate(); err != nil {
		return Frame{}, err
	}
	return f, nil
}

// Validate checks what can be checked on a single frame. Ordering across
// frames (seq continuity, ts monotonicity) is the receiver's problem.
func (f Frame) Validate() error {
	if f.Ts < 0 {
		return fmt.Errorf("recording frame has negative ts %v", f.Ts)
	}
	switch f.Operation {
	case OperationStdout:
		// empty data is fine - it replays as an empty output event
	case OperationResize:
		if f.Cols == 0 || f.Rows == 0 {
			return fmt.Errorf("resize frame has invalid dimensions %dx%d", f.Cols, f.Rows)
		}
	case OperationEnd:
		if f.Dropped == nil {
			return errors.New("end frame is missing dropped count")
		}
		if *f.Dropped < 0 {
			return fmt.Errorf("end frame has negative dropped count %d", *f.Dropped)
		}
	default:
		return fmt.Errorf("unknown recording frame operation %q", f.Operation)
	}
	return nil
}
