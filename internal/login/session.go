package login

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"
)

// User represents a logged-in client session.
type User struct {
	Session   int64
	ID        string
	IP        string
	Hostname  string
	Version   string
	Group     string
	LoginTime time.Time
}

// SessionManager manages client login sessions.
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[int64]*User
	password string
}

func NewSessionManager(password string) *SessionManager {
	return &SessionManager{
		sessions: make(map[int64]*User),
		password: password,
	}
}

// Login authenticates a user and returns a session token. Returns 0 on failure.
func (sm *SessionManager) Login(id, pass, ip string) int64 {
	if sm.password != "" && pass != sm.password {
		return 0
	}

	session := generateSession()
	user := &User{
		Session:   session,
		ID:        id,
		IP:        ip,
		Group:     "default",
		LoginTime: time.Now(),
	}

	sm.mu.Lock()
	sm.sessions[session] = user
	sm.mu.Unlock()

	return session
}

// OkSession returns true if the session is valid.
func (sm *SessionManager) OkSession(session int64) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	_, ok := sm.sessions[session]
	return ok
}

// GetUser returns the user associated with a session.
func (sm *SessionManager) GetUser(session int64) *User {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.sessions[session]
}

// GetAllUsers returns all currently logged-in users.
func (sm *SessionManager) GetAllUsers() []*User {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	users := make([]*User, 0, len(sm.sessions))
	for _, u := range sm.sessions {
		users = append(users, u)
	}
	return users
}

func generateSession() int64 {
	var b [8]byte
	rand.Read(b[:])
	v := int64(binary.BigEndian.Uint64(b[:]))
	if v < 0 {
		v = -v
	}
	if v == 0 {
		v = 1
	}
	return v
}
