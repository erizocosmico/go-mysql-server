package aggregation

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestCount_String(t *testing.T) {
	require := require.New(t)
	c := NewCount(expression.NewLiteral("foo", sql.Text))
	require.Equal(`COUNT("foo")`, c.String())
}

func TestCount_Eval_1(t *testing.T) {
	require := require.New(t)

	c := NewCount(expression.NewLiteral(1, sql.Int32))
	b := c.NewBuffer()
	require.Equal(int32(0), eval(t, c, b))

	require.NoError(c.Update(nil, b, nil))
	require.NoError(c.Update(nil, b, sql.NewRow("foo")))
	require.NoError(c.Update(nil, b, sql.NewRow(1)))
	require.NoError(c.Update(nil, b, sql.NewRow(nil)))
	require.NoError(c.Update(nil, b, sql.NewRow(1, 2, 3)))
	require.Equal(int32(5), eval(t, c, b))

	b2 := c.NewBuffer()
	require.NoError(c.Update(nil, b2, nil))
	require.NoError(c.Update(nil, b2, sql.NewRow("foo")))
	require.NoError(c.Merge(nil, b, b2))
	require.Equal(int32(7), eval(t, c, b))
}

func TestCount_Eval_Star(t *testing.T) {
	require := require.New(t)

	c := NewCount(expression.NewStar())
	b := c.NewBuffer()
	require.Equal(int32(0), eval(t, c, b))

	c.Update(nil, b, nil)
	c.Update(nil, b, sql.NewRow("foo"))
	c.Update(nil, b, sql.NewRow(1))
	c.Update(nil, b, sql.NewRow(nil))
	c.Update(nil, b, sql.NewRow(1, 2, 3))
	require.Equal(int32(5), eval(t, c, b))

	b2 := c.NewBuffer()
	c.Update(nil, b2, sql.NewRow())
	c.Update(nil, b2, sql.NewRow("foo"))
	c.Merge(nil, b, b2)
	require.Equal(int32(7), eval(t, c, b))
}

func TestCount_Eval_String(t *testing.T) {
	require := require.New(t)

	c := NewCount(expression.NewGetField(0, sql.Text, "", true))
	b := c.NewBuffer()
	require.Equal(int32(0), eval(t, c, b))

	c.Update(nil, b, sql.NewRow("foo"))
	require.Equal(int32(1), eval(t, c, b))

	c.Update(nil, b, sql.NewRow(nil))
	require.Equal(int32(1), eval(t, c, b))
}
