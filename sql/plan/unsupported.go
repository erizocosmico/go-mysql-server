package plan

import (
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Unsupported is a node which is unsupported, but it's implemented as
// a way to make the query not fail.
type Unsupported struct {
	// Message will be logged as a warning when the query executes.
	Message string
	// ResultSchema is the schema for the result rows.
	ResultSchema sql.Schema
	// Result will be returned in the iterator of this node.
	Result []sql.Row
}

// NewUnsupported creates a new unsupported node.
func NewUnsupported(message string, schema sql.Schema, result ...sql.Row) *Unsupported {
	return &Unsupported{message, schema, result}
}

// Resolved implements the Node interface.
func (Unsupported) Resolved() bool { return true }

// Schema implements the Node interface.
func (u *Unsupported) Schema() sql.Schema { return u.ResultSchema }

// Children implements the Node interface.
func (Unsupported) Children() []sql.Node { return nil }

// RowIter implements the Node interface.
func (u *Unsupported) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("Unsupported")
	logrus.Warn(u.Message)
	return sql.NewSpanIter(span, sql.RowsToRowIter(u.Result...)), nil
}

func (Unsupported) String() string { return "unsupported" }

// TransformExpressionsUp implements the Node interface.
func (u *Unsupported) TransformExpressionsUp(fn sql.TransformExprFunc) (sql.Node, error) {
	return u, nil
}

// TransformUp implements the Node interface.
func (u *Unsupported) TransformUp(fn sql.TransformNodeFunc) (sql.Node, error) {
	return u, nil
}
