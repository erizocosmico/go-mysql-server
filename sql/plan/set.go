package plan

import (
	"fmt"
	"io"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/config"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Scope of the config update.
type Scope bool

const (
	// SessionScope means the config update is to the session config.
	SessionScope = Scope(false)
	// GlobalScope means the config update is to the global config.
	GlobalScope = Scope(true)
)

func (s Scope) String() string {
	switch s {
	case SessionScope:
		return "SESSION"
	case GlobalScope:
		return "GLOBAL"
	}
	return ""
}

// Update represents a single update operation to the configuration.
type Update struct {
	// Name of the configuration key.
	Name string
	// Expression that will be evaluated to get the configuration value.
	Value sql.Expression
}

// Set updates configuration values and returns the result of those updates.
type Set struct {
	Scope   Scope
	Updates []Update
}

// NewSet creates a new Set node.
func NewSet(scope Scope, updates []Update) *Set {
	return &Set{scope, updates}
}

// Resolved implements the Node interface.
func (s *Set) Resolved() bool {
	for _, u := range s.Updates {
		if !u.Value.Resolved() {
			return false
		}
	}

	return true
}

// Children implements the Node interface.
func (Set) Children() []sql.Node { return nil }

func (s *Set) String() string {
	var children = make([]string, len(s.Updates))
	for i, u := range s.Updates {
		children[i] = fmt.Sprintf("%s = %s", u.Name, u.Value)
	}

	p := sql.NewTreePrinter()
	_ = p.WriteNode("SET(%s)", s.Scope)
	_ = p.WriteChildren(children...)
	return p.String()
}

// Schema implements the Node interface.
func (s *Set) Schema() sql.Schema {
	return sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: false},
		{Name: "value", Type: sql.Text, Nullable: false},
	}
}

// ErrUnableToAccessGlobalConfig is returned when global config cannot be
// accessed.
var ErrUnableToAccessGlobalConfig = errors.NewKind("unable to access global config, session config parent is empty")

// RowIter implements the Node interface.
func (s *Set) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var conf *config.Config
	if s.Scope == GlobalScope {
		if ctx.Config().Parent() == nil {
			return nil, ErrUnableToAccessGlobalConfig.New()
		}

		conf = ctx.Config().Parent()
	} else {
		conf = ctx.Config()
	}

	pairs := make([]pair, len(s.Updates))
	for i, u := range s.Updates {
		val, err := u.Value.Eval(ctx, nil)
		if err != nil {
			return nil, err
		}

		if sql.IsNumber(u.Value.Type()) {
			if sql.IsDecimal(u.Value.Type()) {
				val, err = sql.Float64.Convert(val)
				if err != nil {
					return nil, err
				}

				pairs[i] = pair{u.Name, val}
				conf.SetFloat(u.Name, val.(float64))
			} else {
				val, err = sql.Int64.Convert(val)
				if err != nil {
					return nil, err
				}

				pairs[i] = pair{u.Name, val}
				conf.SetInt(u.Name, val.(int64))
			}
		} else if u.Value.Type() == sql.Boolean {
			val, err = sql.Boolean.Convert(val)
			if err != nil {
				return nil, err
			}

			pairs[i] = pair{u.Name, val}
			conf.SetBool(u.Name, val.(bool))
		} else {
			val, err = sql.Text.Convert(val)
			if err != nil {
				return nil, err
			}

			pairs[i] = pair{u.Name, val}
			conf.SetString(u.Name, val.(string))
		}

		// TODO: support arrays?
	}

	return &pairIterator{p: pairs}, nil
}

// TransformUp implements the Node interface.
func (s *Set) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(NewSet(s.Scope, s.Updates))
}

// TransformExpressionsUp implements the Node interface.
func (s *Set) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	var updates = make([]Update, len(s.Updates))
	for i, u := range s.Updates {
		expr, err := u.Value.TransformUp(f)
		if err != nil {
			return nil, err
		}
		updates[i] = Update{
			Name:  u.Name,
			Value: expr,
		}
	}
	return NewSet(s.Scope, updates), nil
}

type pair struct {
	name  string
	value interface{}
}

type pairIterator struct {
	p   []pair
	idx int
}

func (i *pairIterator) Next() (sql.Row, error) {
	if i.idx >= len(i.p) {
		return nil, io.EOF
	}

	i.idx++
	return sql.NewRow(
		i.p[i.idx-1].name,
		i.p[i.idx-1].value,
	), nil
}

func (i *pairIterator) Close() error {
	i.idx = len(i.p)
	return nil
}
