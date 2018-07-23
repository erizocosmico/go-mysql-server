package plan

import (
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type UpdateExpr struct {
	Column string
	Value  sql.Expression
}

type Update struct {
	UnaryNode
	Updates []UpdateExpr
}

func NewUpdate(updates []UpdateExpr, child sql.Node) *Update {
	return &Update{
		UnaryNode{Child: child},
		updates,
	}
}

var _ sql.Node = (*Update)(nil)

var ErrNotUpdatable = errors.NewKind("no updatable table was found")

func (u *Update) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Update")
	defer span.Finish()

	var table sql.Updater
	Inspect(u.Child, func(n sql.Node) bool {
		u, ok := n.(sql.Updater)
		if ok {
			table = u
		}

		return true
	})

	if table == nil {
		return nil, ErrNotUpdatable.New("")
	}

	if err := table.StartUpdate(ctx); err != nil {
		return nil, err
	}

	for {

	}

	return sql.RowsToRowIter(), nil
}
