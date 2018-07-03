package plan

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// ShowDatabases is a node that shows the databases.
type ShowDatabases struct {
	Catalog *sql.Catalog
}

// NewShowDatabases creates a new show databases node.
func NewShowDatabases() *ShowDatabases {
	return new(ShowDatabases)
}

// Resolved implements the Resolvable interface.
func (p *ShowDatabases) Resolved() bool {
	return p.Catalog != nil
}

// Children implements the Node interface.
func (*ShowDatabases) Children() []sql.Node {
	return nil
}

// Schema implements the Node interface.
func (*ShowDatabases) Schema() sql.Schema {
	return sql.Schema{{
		Name:     "database",
		Type:     sql.Text,
		Nullable: false,
	}}
}

// RowIter implements the Node interface.
func (p *ShowDatabases) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var rows = make([]sql.Row, len(p.Catalog.Databases))
	for i, db := range p.Catalog.Databases {
		rows[i] = sql.Row{db.Name()}
	}

	return sql.RowsToRowIter(rows...), nil
}

// TransformUp implements the Transformable interface.
func (p *ShowDatabases) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	np := *p
	return f(&np)
}

// TransformExpressionsUp implements the Transformable interface.
func (p *ShowDatabases) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return p, nil
}

func (p ShowDatabases) String() string {
	return "ShowDatabases"
}
