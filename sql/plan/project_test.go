package plan

import (
	"io"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression/function"
)

func TestProject(t *testing.T) {
	t.Run("no parallelism", func(t *testing.T) {
		testProject(t, 0)
	})

	t.Run("parallelism", func(t *testing.T) {
		testProject(t, 5)
	})

	child := mem.NewTable("test", sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
		{Name: "col2", Type: sql.Text, Nullable: true},
	})

	t.Run("schema", func(t *testing.T) {
		require := require.New(t)
		p := NewProject(nil, NewResolvedTable(child))
		require.Equal(0, len(p.Schema()))

		p = NewProject([]sql.Expression{
			expression.NewAlias(
				expression.NewGetField(1, sql.Text, "col2", true),
				"foo",
			),
		}, NewResolvedTable(child))
		schema := sql.Schema{
			{Name: "foo", Type: sql.Text, Nullable: true},
		}
		require.Equal(schema, p.Schema())
	})

}

func BenchmarkProject(b *testing.B) {
	b.Run("no parallelism", func(b *testing.B) {
		benchProject(b, 0)
	})

	b.Run("parallelism", func(b *testing.B) {
		benchProject(b, runtime.NumCPU())
	})
}

func benchProject(b *testing.B, parallelism int) {
	require := require.New(b)
	ctx := sql.NewEmptyContext()

	substring, err := function.NewSubstring(
		expression.NewGetField(0, sql.Text, "strfield", true),
		expression.NewLiteral(int64(0), sql.Int64),
		expression.NewLiteral(int64(1), sql.Int64),
	)
	require.NoError(err)

	expressions := []sql.Expression{
		substring,
		expression.NewGetField(1, sql.Float64, "floatfield", true),
		expression.NewGetField(2, sql.Boolean, "boolfield", false),
		expression.NewGetField(3, sql.Int32, "intfield", false),
		expression.NewGetField(4, sql.Int64, "bigintfield", false),
		expression.NewGetField(5, sql.Blob, "blobfield", false),
	}

	for i := 0; i < b.N; i++ {
		d := NewProject(expressions, NewResolvedTable(benchtable))
		d.SetParallelism(parallelism)

		iter, err := d.RowIter(ctx)
		require.NoError(err)
		require.NotNil(iter)

		rows, err := sql.RowIterToRows(iter)
		require.NoError(err)
		require.Len(rows, 150)
	}
}

func testProject(t *testing.T, parallelism int) {
	t.Helper()
	require := require.New(t)
	ctx := sql.NewEmptyContext()
	child := mem.NewTable("test", sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
		{Name: "col2", Type: sql.Text, Nullable: true},
	})

	input := []sql.Row{
		{"col1_1", "col2_1"},
		{"col1_2", "col2_2"},
		{"col1_3", "col2_3"},
		{"col1_4", "col2_4"},
		{"col1_5", "col2_5"},
	}

	for _, row := range input {
		require.NoError(child.Insert(sql.NewEmptyContext(), row))
	}

	p := NewProject(
		[]sql.Expression{expression.NewGetField(1, sql.Text, "col2", true)},
		NewResolvedTable(child),
	)
	p.SetParallelism(parallelism)

	iter, err := p.RowIter(ctx)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		{"col2_1"},
		{"col2_2"},
		{"col2_3"},
		{"col2_4"},
		{"col2_5"},
	}

	require.ElementsMatch(expected, rows)

	_, err = iter.Next()
	require.Equal(io.EOF, err)
}
