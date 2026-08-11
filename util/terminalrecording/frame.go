// Package terminalrecording defines the wire protocol for streaming terminal
// session recordings from argocd-server to a recording endpoint.
//
// Session identity is sent once per connection, in the dial URL query
// parameters (Session). Frames carry a seq/ts envelope plus the stdout and
// resize payloads of the terminal UI protocol; stdin is never sent.
//
// One connection carries one contiguous fragment of a recording; on
// connection loss the producer redials and the next fragment begins. Seqs are
// per session and only assigned to frames that enter the pipeline, so a seq
// gap between fragments measures transit loss, while source drops are counted
// in the end frame.
package terminalrecording

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Frame operations. Stdout and resize match the operation names of the
// terminal UI's TerminalMessage protocol; end exists only on the recording
// wire.
const (
	OperationStdout = "stdout"
	OperationResize = "resize"
	OperationEnd    = "end"
)

// Frame is one recording event on the wire. Operation determines which fields
// beyond Seq and Ts are set. Session identity is carried by the connection,
// not repeated per frame (see Session).
type Frame struct {
	Operation string `json:"operation"`
	// Seq numbers frames per session in enqueue order, continuing across
	// reconnects.
	Seq uint64 `json:"seq"`
	// Ts is seconds since session start, the Asciicast v2 time base. Absolute
	// offsets keep later timing intact when a frame is lost.
	Ts float64 `json:"ts"`

	// Data carries one chunk of terminal output on stdout frames.
	Data string `json:"data,omitempty"`

	// Cols and Rows carry the applied terminal dimensions on resize frames.
	Cols uint16 `json:"cols,omitempty"`
	Rows uint16 `json:"rows,omitempty"`

	// Dropped, on end frames, counts frames dropped at the source over the
	// whole session (buffer full). A pointer so end frames serialize
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
// last data frame's seq; dropped is the session's source-drop count.
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

// Validate checks single-frame invariants. Cross-frame ordering (seq
// continuity, ts monotonicity) is the receiver's concern.
func (f Frame) Validate() error {
	if f.Ts < 0 {
		return fmt.Errorf("recording frame has negative ts %v", f.Ts)
	}
	switch f.Operation {
	case OperationStdout:
		// Empty data is allowed; it replays as an empty output event.
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
