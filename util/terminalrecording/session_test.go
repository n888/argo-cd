package terminalrecording

import (
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSessionID(t *testing.T) {
	t.Parallel()
	hexPattern := regexp.MustCompile(`^[0-9a-f]{16}$`)
	seen := make(map[string]bool)
	for range 200 {
		id, err := NewSessionID()
		require.NoError(t, err)
		assert.Regexp(t, hexPattern, id)
		assert.False(t, seen[id], "generated duplicate session id %s", id)
		seen[id] = true
	}
}

func TestSessionQueryRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		session Session
		// wantUser overrides the expected User when parsing normalizes it.
		wantUser string
	}{
		{
			name: "typical session",
			session: Session{
				ID:        "7f3c9b1ea2d40c58",
				StartTime: time.Unix(1752791400, 0).UTC(),
				App:       "default/guestbook",
				User:      "alice@example.com",
				Cluster:   "in-cluster",
				Namespace: "default",
				Pod:       "guestbook-ui-7d9f8",
				Container: "main",
			},
		},
		{
			name: "values needing URL escaping",
			session: Session{
				ID:        "00ff00ff00ff00ff",
				StartTime: time.Unix(1, 0).UTC(),
				App:       "team a/app+dev",
				User:      "büro user&x=1",
				Cluster:   "https://kubernetes.default.svc",
				Namespace: "ns",
				Pod:       "pod",
				Container: "c",
			},
		},
		{
			name: "empty metadata fields",
			session: Session{
				ID:        "7f3c9b1ea2d40c58",
				StartTime: time.Unix(1752791400, 0).UTC(),
			},
			wantUser: UserAnonymous,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			encoded := tt.session.Query().Encode()
			values, err := url.ParseQuery(encoded)
			require.NoError(t, err)
			parsed, err := ParseSessionQuery(values)
			require.NoError(t, err)
			want := tt.session
			if tt.wantUser != "" {
				want.User = tt.wantUser
			}
			assert.Equal(t, want, parsed)
		})
	}
}

func TestParseSessionQueryDefaultsAnonymousUser(t *testing.T) {
	t.Parallel()
	q := url.Values{}
	q.Set("sessionId", "7f3c9b1ea2d40c58")
	q.Set("start", "1752791400")
	parsed, err := ParseSessionQuery(q)
	require.NoError(t, err)
	assert.Equal(t, UserAnonymous, parsed.User)
}

func TestParseSessionQueryIgnoresUnknownParams(t *testing.T) {
	t.Parallel()
	q := url.Values{}
	q.Set("sessionId", "7f3c9b1ea2d40c58")
	q.Set("start", "1752791400")
	q.Set("futureParam", "ignored")
	parsed, err := ParseSessionQuery(q)
	require.NoError(t, err)
	assert.Equal(t, "7f3c9b1ea2d40c58", parsed.ID)
}

func TestParseSessionQueryRejects(t *testing.T) {
	t.Parallel()
	valid := url.Values{}
	valid.Set("sessionId", "7f3c9b1ea2d40c58")
	valid.Set("start", "1752791400")

	tests := []struct {
		name    string
		mutate  func(url.Values)
		wantErr string
	}{
		{name: "missing sessionId", mutate: func(q url.Values) { q.Del("sessionId") }, wantErr: "missing sessionId"},
		{name: "oversized sessionId", mutate: func(q url.Values) { q.Set("sessionId", strings.Repeat("a", 65)) }, wantErr: "exceeds 64 characters"},
		{name: "missing start", mutate: func(q url.Values) { q.Del("start") }, wantErr: "missing start time"},
		{name: "non-numeric start", mutate: func(q url.Values) { q.Set("start", "yesterday") }, wantErr: "invalid start time"},
		{name: "zero start", mutate: func(q url.Values) { q.Set("start", "0") }, wantErr: "invalid start time"},
		{name: "negative start", mutate: func(q url.Values) { q.Set("start", "-5") }, wantErr: "invalid start time"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q, err := url.ParseQuery(valid.Encode())
			require.NoError(t, err)
			tt.mutate(q)
			_, err = ParseSessionQuery(q)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
