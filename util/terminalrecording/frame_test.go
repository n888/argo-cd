package terminalrecording

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFrameRoundTrip(t *testing.T) {
	t.Parallel()
	dropped := int64(3)
	tests := []struct {
		name  string
		frame Frame
	}{
		{name: "stdout", frame: NewStdoutFrame(0, 0.041, "$ ls\r\napp.py\r\n")},
		{name: "stdout with ANSI escapes", frame: NewStdoutFrame(7, 12.5, "\x1b[31mred\x1b[0m")},
		{name: "stdout with empty data", frame: NewStdoutFrame(8, 12.6, "")},
		{name: "resize", frame: NewResizeFrame(1, 3.22, 120, 40)},
		{name: "end with no drops", frame: NewEndFrame(2, 41.503, 0)},
		{name: "end with drops", frame: Frame{Operation: OperationEnd, Seq: 9, Ts: 60, Dropped: &dropped}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			data, err := json.Marshal(tt.frame)
			require.NoError(t, err)
			parsed, err := ParseFrame(data)
			require.NoError(t, err)
			assert.Equal(t, tt.frame, parsed)
		})
	}
}

func TestEndFrameSerializesZeroDropped(t *testing.T) {
	t.Parallel()
	data, err := json.Marshal(NewEndFrame(2, 41.503, 0))
	require.NoError(t, err)
	assert.Contains(t, string(data), `"dropped":0`)
}

func TestStdoutFrameOmitsUnrelatedFields(t *testing.T) {
	t.Parallel()
	data, err := json.Marshal(NewStdoutFrame(0, 0, "hi"))
	require.NoError(t, err)
	assert.NotContains(t, string(data), "dropped")
	assert.NotContains(t, string(data), "cols")
	assert.NotContains(t, string(data), "rows")
}

func TestParseFrameRejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		data    string
		wantErr string
	}{
		{name: "invalid json", data: `{`, wantErr: "failed to unmarshal"},
		{name: "missing operation", data: `{"seq":0,"ts":0}`, wantErr: "unknown recording frame operation"},
		{name: "unknown operation", data: `{"operation":"stdin","seq":0,"ts":0,"data":"secret"}`, wantErr: "unknown recording frame operation"},
		{name: "negative ts", data: `{"operation":"stdout","seq":0,"ts":-1,"data":"x"}`, wantErr: "negative ts"},
		{name: "resize with zero cols", data: `{"operation":"resize","seq":1,"ts":1,"cols":0,"rows":40}`, wantErr: "invalid dimensions"},
		{name: "resize with zero rows", data: `{"operation":"resize","seq":1,"ts":1,"cols":80,"rows":0}`, wantErr: "invalid dimensions"},
		{name: "end without dropped", data: `{"operation":"end","seq":2,"ts":5}`, wantErr: "missing dropped count"},
		{name: "end with negative dropped", data: `{"operation":"end","seq":2,"ts":5,"dropped":-1}`, wantErr: "negative dropped count"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseFrame([]byte(tt.data))
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
