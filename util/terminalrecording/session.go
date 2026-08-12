package terminalrecording

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/argoproj/argo-cd/v3/util/rand"
)

// sessionIDLength is the hex length of a session ID. 64 random bits makes
// collisions a non-issue at any realistic session volume while keeping
// filenames short.
const sessionIDLength = 16

// maxSessionIDLength bounds IDs accepted from the wire, since they end up in
// filenames and log fields.
const maxSessionIDLength = 64

// UserAnonymous is recorded when a session arrives with no user field.
const UserAnonymous = "anonymous"

// Dial URL query parameter names, shared by producer and endpoint.
const (
	queryParamSessionID = "sessionId"
	queryParamStart     = "start"
	queryParamApp       = "app"
	queryParamUser      = "user"
	queryParamCluster   = "cluster"
	queryParamNamespace = "namespace"
	queryParamPod       = "pod"
	queryParamContainer = "container"
)

// Session identifies one recorded exec session. It is sent once per
// connection in the dial URL query parameters, and every frame on the
// connection inherits it. Cluster and Namespace are included because the app
// name alone doesn't pin down which pod was execed into.
type Session struct {
	// ID comes from NewSessionID at the producer and is shared by all
	// fragments of one session.
	ID string
	// StartTime plus a frame's Ts gives the frame's wall-clock time no matter
	// how late it's delivered. Carried as unix seconds, so sub-second
	// precision is lost.
	StartTime time.Time
	App       string
	User      string
	Cluster   string
	Namespace string
	Pod       string
	Container string
}

// NewSessionID returns a cryptographically random session ID. Keep it
// unguessable - a future resume feature would treat the ID as a bearer key.
func NewSessionID() (string, error) {
	id, err := rand.RandHex(sessionIDLength)
	if err != nil {
		return "", fmt.Errorf("failed to generate terminal recording session id: %w", err)
	}
	return id, nil
}

// Query encodes the session into dial URL query parameters.
func (s Session) Query() url.Values {
	q := url.Values{}
	q.Set(queryParamSessionID, s.ID)
	q.Set(queryParamStart, strconv.FormatInt(s.StartTime.Unix(), 10))
	q.Set(queryParamApp, s.App)
	q.Set(queryParamUser, s.User)
	q.Set(queryParamCluster, s.Cluster)
	q.Set(queryParamNamespace, s.Namespace)
	q.Set(queryParamPod, s.Pod)
	q.Set(queryParamContainer, s.Container)
	return q
}

// ParseSessionQuery decodes the query parameters of a recording connection. A
// missing user becomes UserAnonymous. The other metadata fields can be empty
// and need sanitizing before any filesystem use. Unknown parameters are
// ignored.
func ParseSessionQuery(q url.Values) (Session, error) {
	id := q.Get(queryParamSessionID)
	if id == "" {
		return Session{}, errors.New("recording connection is missing sessionId")
	}
	if len(id) > maxSessionIDLength {
		return Session{}, fmt.Errorf("recording connection sessionId exceeds %d characters", maxSessionIDLength)
	}
	startStr := q.Get(queryParamStart)
	if startStr == "" {
		return Session{}, errors.New("recording connection is missing start time")
	}
	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		return Session{}, fmt.Errorf("recording connection has invalid start time %q: %w", startStr, err)
	}
	if start <= 0 {
		return Session{}, fmt.Errorf("recording connection has invalid start time %d", start)
	}
	user := q.Get(queryParamUser)
	if user == "" {
		user = UserAnonymous
	}
	return Session{
		ID:        id,
		StartTime: time.Unix(start, 0).UTC(),
		App:       q.Get(queryParamApp),
		User:      user,
		Cluster:   q.Get(queryParamCluster),
		Namespace: q.Get(queryParamNamespace),
		Pod:       q.Get(queryParamPod),
		Container: q.Get(queryParamContainer),
	}, nil
}
