package settings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestExecTerminalRecordingSettings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		data         map[string]string
		wantEnabled  bool
		wantEndpoint string
	}{
		{
			name:        "disabled by default",
			data:        map[string]string{},
			wantEnabled: false,
		},
		{
			name: "enabled with ws endpoint",
			data: map[string]string{
				"exec.terminal.recording.enabled":  "true",
				"exec.terminal.recording.endpoint": "ws://localhost:8090/session",
			},
			wantEnabled:  true,
			wantEndpoint: "ws://localhost:8090/session",
		},
		{
			name: "enabled with wss endpoint",
			data: map[string]string{
				"exec.terminal.recording.enabled":  "true",
				"exec.terminal.recording.endpoint": "wss://recorder.argocd.svc:8090/session",
			},
			wantEnabled:  true,
			wantEndpoint: "wss://recorder.argocd.svc:8090/session",
		},
		{
			name: "enabled without endpoint is disabled",
			data: map[string]string{
				"exec.terminal.recording.enabled": "true",
			},
			wantEnabled: false,
		},
		{
			name: "enabled with non-websocket scheme is disabled",
			data: map[string]string{
				"exec.terminal.recording.enabled":  "true",
				"exec.terminal.recording.endpoint": "http://localhost:8090/session",
			},
			wantEnabled:  false,
			wantEndpoint: "http://localhost:8090/session",
		},
		{
			name: "enabled with unparsable endpoint is disabled",
			data: map[string]string{
				"exec.terminal.recording.enabled":  "true",
				"exec.terminal.recording.endpoint": "ws://[::1",
			},
			wantEnabled:  false,
			wantEndpoint: "ws://[::1",
		},
		{
			name: "enabled with missing host is disabled",
			data: map[string]string{
				"exec.terminal.recording.enabled":  "true",
				"exec.terminal.recording.endpoint": "ws:///session",
			},
			wantEnabled:  false,
			wantEndpoint: "ws:///session",
		},
		{
			name: "enabled with query string is disabled",
			data: map[string]string{
				"exec.terminal.recording.enabled":  "true",
				"exec.terminal.recording.endpoint": "ws://localhost:8090/session?x=1",
			},
			wantEnabled:  false,
			wantEndpoint: "ws://localhost:8090/session?x=1",
		},
		{
			name: "disabled ignores invalid endpoint",
			data: map[string]string{
				"exec.terminal.recording.endpoint": "not-a-url",
			},
			wantEnabled:  false,
			wantEndpoint: "not-a-url",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, settingsManager := fixtures(t.Context(), tt.data, func(secret *corev1.Secret) {
				secret.Data["server.secretkey"] = nil
			})
			settings, err := settingsManager.GetSettings()
			require.NoError(t, err)
			assert.Equal(t, tt.wantEnabled, settings.ExecTerminalRecordingEnabled)
			assert.Equal(t, tt.wantEndpoint, settings.ExecTerminalRecordingEndpoint)
		})
	}
}
