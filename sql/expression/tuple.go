package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// Tuple is a fixed-size collection of expressions.
// A tuple of size 1 is treated as the expression itself.
type Tuple []sql.Expression

// NewTuple creates a new Tuple expression.
func NewTuple(exprs ...sql.Expression) Tuple {
	return Tuple(exprs)
}

// Eval implements the Expression interface.
func (t Tuple) Eval(row sql.Row) (interface{}, error) {
	if len(t) == 1 {
		return t[0].Eval(row)
	}

	var result = make([]interface{}, len(t))
	for i, e := range t {
		v, err := e.Eval(row)
		if err != nil {
			return nil, err
		}

		result[i] = v
	}

	return result, nil
}

// IsNullable implements the Expression interface.
func (t Tuple) IsNullable() bool {
	if len(t) == 1 {
		return t[0].IsNullable()
	}

	return false
}

// Name implements the Expression interface.
func (t Tuple) Name() string {
	if len(t) == 1 {
		return t[0].Name()
	}

	return "tuple"
}

// Resolved implements the Expression interface.
func (t Tuple) Resolved() bool {
	for _, e := range t {
		if !e.Resolved() {
			return false
		}
	}

	return true
}

// Type implements the Expression interface.
func (t Tuple) Type() sql.Type {
	if len(t) == 1 {
		return t[0].Type()
	}

	return sql.Tuple
}

// TransformUp implements the Expression interface.
func (t Tuple) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	var exprs = make([]sql.Expression, len(t))
	for i, e := range t {
		exprs[i] = f(e)
	}

	return f(Tuple(exprs))
}
