package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// UnaryExpression is an expression that has only one children.
type UnaryExpression struct {
	Child sql.Expression
}

// Resolved implements the Expression interface.
func (p UnaryExpression) Resolved() bool {
	return p.Child.Resolved()
}

// IsNullable returns whether the expression can be null.
func (p UnaryExpression) IsNullable() bool {
	return p.Child.IsNullable()
}

// BinaryExpression is an expression that has two children.
type BinaryExpression struct {
	Left  sql.Expression
	Right sql.Expression
}

// Resolved implements the Expression interface.
func (p BinaryExpression) Resolved() bool {
	return p.Left.Resolved() && p.Right.Resolved()
}

// IsNullable returns whether the expression can be null.
func (p BinaryExpression) IsNullable() bool {
	return p.Left.IsNullable() || p.Right.IsNullable()
}

var defaultFunctions = map[string]interface{}{
	"count": NewCount,
	"first": NewFirst,
}

// RegisterDefaults registers the aggregations in the catalog.
func RegisterDefaults(c *sql.Catalog) error {
	for k, v := range defaultFunctions {
		if err := c.RegisterFunction(k, v); err != nil {
			return err
		}
	}

	return nil
}
