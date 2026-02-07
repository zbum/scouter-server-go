package service

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// HandlerFunc is a TCP service handler.
// din reads the request payload, dout writes the response.
// login indicates whether the client has been authenticated.
type HandlerFunc func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool)

// Registry holds registered service handlers keyed by command name.
type Registry struct {
	handlers map[string]HandlerFunc
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]HandlerFunc),
	}
}

// Register associates a handler with a command name.
func (r *Registry) Register(cmd string, handler HandlerFunc) {
	r.handlers[cmd] = handler
}

// Get returns the handler for a command, or nil.
func (r *Registry) Get(cmd string) HandlerFunc {
	return r.handlers[cmd]
}
