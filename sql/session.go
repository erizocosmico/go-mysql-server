package sql

import (
	"context"

	"gopkg.in/src-d/go-mysql-server.v0/config"
)

// Session holds the session data.
type Session interface {
	// Config of the session. It also has access to the global config.
	Config() *config.Config
}

// BaseSession is the basic session type.
type BaseSession struct {
	cfg *config.Config
}

// NewBaseSession creates a new basic session.
func NewBaseSession(config *config.Config) Session {
	return &BaseSession{cfg: config}
}

// Config returns the config of the session.
func (s BaseSession) Config() *config.Config {
	return s.cfg
}

// Context of the query execution.
type Context struct {
	context.Context
	Session
}

// NewContext creates a new query context.
func NewContext(ctx context.Context, session Session) *Context {
	return &Context{ctx, session}
}

// NewEmptyContext creates an empty context.
func NewEmptyContext() *Context {
	return NewContext(context.TODO(), NewBaseSession(config.New()))
}
