package plan

import (
	"sort"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// ShowTables is a node that shows the database tables.
type ShowTables struct {
	Database sql.Database
}

// NewShowTables creates a new show tables node given a database.
func NewShowTables(database sql.Database) *ShowTables {
	return &ShowTables{
		Database: database,
	}
}

// Resolved implements the Resolvable interface.
func (p *ShowTables) Resolved() bool {
	_, ok := p.Database.(*sql.UnresolvedDatabase)
	return !ok
}

// Children implements the Node interface.
func (*ShowTables) Children() []sql.Node {
	return nil
}

// Schema implements the Node interface.
func (*ShowTables) Schema() sql.Schema {
	return sql.Schema{{
		Name:     "table",
		Type:     sql.Text,
		Nullable: false,
	}}
}

// RowIter implements the Node interface.
func (p *ShowTables) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var tableNames []string
	for key := range p.Database.Tables() {
		tableNames = append(tableNames, key)
	}

	sort.Strings(tableNames)

	var rows = make([]sql.Row, len(tableNames))
	for i, t := range tableNames {
		rows[i] = sql.Row{t}
	}

	return sql.RowsToRowIter(rows...), nil
}

// TransformUp implements the Transformable interface.
func (p *ShowTables) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(NewShowTables(p.Database))
}

// TransformExpressionsUp implements the Transformable interface.
func (p *ShowTables) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return p, nil
}

func (p ShowTables) String() string {
	return "ShowTables"
}
