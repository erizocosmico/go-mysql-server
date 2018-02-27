package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

type Subquery struct {
	node sql.Node
}

func NewSubquery(node sql.Node) *Subquery {
	return &Subquery{node}
}

func (s *Subquery) Eval(row sql.Row) (interface{}, error) {
	return nil, nil
}

func (s *Subquery) IsNullable() bool { return true }
func (s *Subquery) Name() string     { return "subquery" }
func (s *Subquery) Type() sql.Type   { return sql.Tuple }
func (s *Subquery) Resolved() bool   { return true }
func (s *Subquery) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return s
}
