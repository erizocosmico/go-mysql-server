package plan

import (
	"context"
	"io"
	"strings"
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Project is a projection of certain expression from the children node.
type Project struct {
	UnaryNode
	// Expression projected.
	Projections []sql.Expression
	Parallelism int
}

// NewProject creates a new projection.
func NewProject(expressions []sql.Expression, child sql.Node) *Project {
	return &Project{
		UnaryNode:   UnaryNode{child},
		Projections: expressions,
	}
}

// SetParallelism sets the maximum number of goroutines to use to process
// this projection in parallel.
func (p *Project) SetParallelism(parallelism int) {
	p.Parallelism = parallelism
}

// Schema implements the Node interface.
func (p *Project) Schema() sql.Schema {
	var s = make(sql.Schema, len(p.Projections))
	for i, e := range p.Projections {
		var name string
		if n, ok := e.(sql.Nameable); ok {
			name = n.Name()
		} else {
			name = e.String()
		}

		var table string
		if t, ok := e.(sql.Tableable); ok {
			table = t.Table()
		}

		s[i] = &sql.Column{
			Name:     name,
			Type:     e.Type(),
			Nullable: e.IsNullable(),
			Source:   table,
		}
	}
	return s
}

// Resolved implements the Resolvable interface.
func (p *Project) Resolved() bool {
	return p.UnaryNode.Child.Resolved() &&
		expressionsResolved(p.Projections...)
}

// RowIter implements the Node interface.
func (p *Project) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span(
		"plan.Project",
		opentracing.Tag{
			Key:   "projections",
			Value: len(p.Projections),
		},
		opentracing.Tag{
			Key:   "parallelism",
			Value: p.Parallelism,
		},
	)

	i, err := p.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}

	var childIter sql.RowIter
	if p.Parallelism >= 1 {
		childIter = newParallelIter(p.Projections, i, ctx, p.Parallelism)
	} else {
		childIter = &iter{p.Projections, i, ctx}
	}

	return sql.NewSpanIter(span, childIter), nil
}

// TransformUp implements the Transformable interface.
func (p *Project) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := p.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	np := NewProject(p.Projections, child)
	np.Parallelism = p.Parallelism
	return f(np)
}

// TransformExpressionsUp implements the Transformable interface.
func (p *Project) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	exprs, err := transformExpressionsUp(f, p.Projections)
	if err != nil {
		return nil, err
	}

	child, err := p.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	np := NewProject(exprs, child)
	np.Parallelism = p.Parallelism
	return np, nil
}

func (p *Project) String() string {
	pr := sql.NewTreePrinter()
	var exprs = make([]string, len(p.Projections))
	for i, expr := range p.Projections {
		exprs[i] = expr.String()
	}

	if p.Parallelism <= 1 {
		_ = pr.WriteNode("Project(%s)", strings.Join(exprs, ", "))
	} else {
		_ = pr.WriteNode(
			"Project(%s, parallelism=%d)",
			strings.Join(exprs, ", "),
			p.Parallelism,
		)
	}

	_ = pr.WriteChildren(p.Child.String())
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (p *Project) Expressions() []sql.Expression {
	return p.Projections
}

// TransformExpressions implements the Expressioner interface.
func (p *Project) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	projects, err := transformExpressionsUp(f, p.Projections)
	if err != nil {
		return nil, err
	}

	return NewProject(projects, p.Child), nil
}

type iter struct {
	projections []sql.Expression
	child       sql.RowIter
	ctx         *sql.Context
}

func (i *iter) Next() (sql.Row, error) {
	row, err := i.child.Next()
	if err != nil {
		return nil, err
	}
	return project(i.ctx, i.projections, row)
}

func (i *iter) Close() error {
	return i.child.Close()
}

type parallelIter struct {
	projections []sql.Expression
	child       sql.RowIter
	ctx         *sql.Context
	parallelism int

	cancel context.CancelFunc
	rows   chan sql.Row
	errors chan error
	done   bool

	mut      sync.Mutex
	finished bool
}

func newParallelIter(
	projections []sql.Expression,
	child sql.RowIter,
	ctx *sql.Context,
	parallelism int,
) *parallelIter {
	var cancel context.CancelFunc
	ctx.Context, cancel = context.WithCancel(ctx.Context)

	return &parallelIter{
		projections: projections,
		child:       child,
		ctx:         ctx,
		parallelism: parallelism,
		cancel:      cancel,
		errors:      make(chan error, parallelism),
	}
}

func (i *parallelIter) Next() (sql.Row, error) {
	if i.done {
		return nil, io.EOF
	}

	if i.rows == nil {
		i.rows = make(chan sql.Row, i.parallelism)
		go i.start()
	}

	select {
	case row, ok := <-i.rows:
		if !ok {
			i.close()
			return nil, io.EOF
		}
		return row, nil
	case err := <-i.errors:
		i.close()
		return nil, err
	}
}

func (i *parallelIter) nextRow() (sql.Row, bool) {
	i.mut.Lock()
	defer i.mut.Unlock()

	if i.finished {
		return nil, true
	}

	row, err := i.child.Next()
	if err != nil {
		if err == io.EOF {
			i.finished = true
		} else {
			i.errors <- err
		}
		return nil, true
	}

	return row, false
}

func (i *parallelIter) start() {
	var wg sync.WaitGroup
	wg.Add(i.parallelism)
	for j := 0; j < i.parallelism; j++ {
		go func() {
			defer wg.Done()

			for {
				select {
				case <-i.ctx.Done():
					i.errors <- context.Canceled
					return
				default:
				}

				row, stop := i.nextRow()
				if stop {
					return
				}

				row, err := project(i.ctx, i.projections, row)
				if err != nil {
					i.errors <- err
					return
				}

				i.rows <- row
			}
		}()
	}

	wg.Wait()
	close(i.rows)
}

func (i *parallelIter) close() {
	if !i.done {
		i.cancel()
		i.done = true
	}
}

func (i *parallelIter) Close() error {
	i.close()
	return i.child.Close()
}

func project(
	s *sql.Context,
	projections []sql.Expression,
	row sql.Row,
) (sql.Row, error) {
	var fields []interface{}
	for _, expr := range projections {
		f, err := expr.Eval(s, row)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}
	return sql.NewRow(fields...), nil
}
