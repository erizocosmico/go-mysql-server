package expression

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestTuple(t *testing.T) {
	require := require.New(t)

	tup := NewTuple(
		NewLiteral(int64(1), sql.Int64),
		NewLiteral(float64(3.14), sql.Float64),
		NewLiteral("foo", sql.Text),
	)

	require.False(tup.IsNullable())
	require.True(tup.Resolved())
	require.Equal(sql.Tuple, tup.Type())

	result, err := tup.Eval(nil)
	require.NoError(err)
	require.Equal([]interface{}{int64(1), float64(3.14), "foo"}, result)

	tup = NewTuple(
		NewGetField(0, sql.Text, "text", true),
	)

	require.True(tup.IsNullable())
	require.True(tup.Resolved())
	require.Equal(sql.Text, tup.Type())

	result, err = tup.Eval(sql.NewRow("foo"))
	require.NoError(err)
	require.Equal("foo", result)

	tup = NewTuple(
		NewGetField(0, sql.Text, "text", true),
		NewLiteral("bar", sql.Text),
	)

	require.False(tup.IsNullable())
	require.True(tup.Resolved())
	require.Equal(sql.Tuple, tup.Type())

	result, err = tup.Eval(sql.NewRow("foo"))
	require.NoError(err)
	require.Equal([]interface{}{"foo", "bar"}, result)

	tup = NewTuple(
		NewUnresolvedColumn("bar"),
		NewLiteral("bar", sql.Text),
	)

	require.False(tup.Resolved())
	require.False(tup.IsNullable())
}
