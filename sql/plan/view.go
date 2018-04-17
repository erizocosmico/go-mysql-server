package plan

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// View is a special table created from the result of a query.
type View struct {
	name   string
	Plan   sql.Node
	schema sql.Schema
}

// NewView creates a new view with the given plan and name.
func NewView(name string, plan sql.Node) *View {
	childSchema := plan.Schema()
	var schema = make(sql.Schema, len(childSchema))
	for i, col := range childSchema {
		c := *col
		c.Source = name
		schema[i] = &c
	}

	return &View{name, plan, schema}
}

// Name implements tha Table interface.
func (v *View) Name() string { return v.name }

// Resolved implements the Node interface.
func (v *View) Resolved() bool { return true }

// Schema implements the Node interface.
func (v *View) Schema() sql.Schema { return v.schema }

// Children implements the Node interface.
func (v *View) Children() []sql.Node { return nil }

// RowIter implements the Node interface.
func (v *View) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	return v.Plan.RowIter(ctx)
}

// TransformExpressionsUp implements the Node interface.
func (v *View) TransformExpressionsUp(fn sql.TransformExprFunc) (sql.Node, error) {
	return v, nil
}

// TransformUp implements the Node interface.
func (v *View) TransformUp(fn sql.TransformNodeFunc) (sql.Node, error) {
	return fn(v)
}

func (v *View) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("View(%s)", v.name)
	var schema = make([]string, len(v.schema))
	for i, col := range v.schema {
		schema[i] = fmt.Sprintf(
			"Column(%s, %s, nullable=%v)",
			col.Name,
			col.Type.Type().String(),
			col.Nullable,
		)
	}
	_ = p.WriteChildren(schema...)
	return p.String()
}

// CreateView is a node that will create a view.
type CreateView struct {
	Name string
	Plan sql.Node
}

// NewCreateView creates a new Create View node.
func NewCreateView(name string, plan sql.Node) *CreateView {
	return &CreateView{name, plan}
}

// Resolved implements the Node interface.
func (v *CreateView) Resolved() bool { return true }

// Schema implements the Node interface.
func (v *CreateView) Schema() sql.Schema { return nil }

// Children implements the Node interface.
func (v *CreateView) Children() []sql.Node { return []sql.Node{v.Plan} }

// RowIter implements the Node interface.
func (v *CreateView) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

// TransformExpressionsUp implements the Node interface.
func (v *CreateView) TransformExpressionsUp(fn sql.TransformExprFunc) (sql.Node, error) {
	plan, err := v.Plan.TransformExpressionsUp(fn)
	if err != nil {
		return nil, err
	}
	return NewCreateView(v.Name, plan), nil
}

// TransformUp implements the Node interface.
func (v *CreateView) TransformUp(fn sql.TransformNodeFunc) (sql.Node, error) {
	plan, err := v.Plan.TransformUp(fn)
	if err != nil {
		return nil, err
	}
	return fn(NewCreateView(v.Name, plan))
}

func (v *CreateView) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("CreateView(%s)", v.Name)
	_ = p.WriteChildren(v.Plan.String())
	return p.String()
}
