package terminalrecording

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/argoproj/argo-cd/v3/util/rand"
)

// sessionIDLength is the hex length of a session identifier: 64 random bits,
// collision-safe at realistic session volumes, short enough for filenames.
const sessionIDLength = 16

// maxSessionIDLength bounds identifiers accepted from the wire; the endpoint
// embeds them in filenames and log fields.
const maxSessionIDLength = 64

// UserAnonymous stands in for an anonymous session with a missing user field.
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
// connection, in the dial URL query parameters; frames inherit it from their
// connection. Cluster and Namespace locate the pod, which the app name alone
// does not determine.
type Session struct {
	// ID is minted by the producer with NewSessionID and shared by all
	// fragments of one session.
	ID string
	// StartTime plus a frame's Ts is its wall-clock time regardless of
	// delivery delay. Carried as unix seconds; sub-second precision is lost.
	StartTime time.Time
	App       string
	User      string
	Cluster   string
	Namespace string
	Pod       string
	Container string
}

// NewSessionID returns a cryptographically random session identifier. It must
// not be guessable: a future resume feature would treat it as a bearer key.
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

// ParseSessionQuery decodes the dial URL query parameters of a recording
// connection. A missing user becomes UserAnonymous; other metadata fields may
// be empty and must be sanitized before filesystem use. Unknown parameters are
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
