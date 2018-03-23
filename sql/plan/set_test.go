package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/config"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestSetGlobal(t *testing.T) {
	require := require.New(t)

	globalConf := config.New()
	sessionConf := config.FromConfig(globalConf)

	ctx := sql.NewContext(context.TODO(), sql.NewBaseSession(sessionConf))

	node := NewSet(GlobalScope, []Update{
		{"a", expression.NewLiteral(int64(1), sql.Int64)},
		{"b", expression.NewLiteral("foo", sql.Text)},
		{"c", expression.NewLiteral(float64(3.14), sql.Float64)},
		{"d", expression.NewLiteral(true, sql.Boolean)},
	})

	iter, err := node.RowIter(ctx)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Equal([]sql.Row{
		sql.NewRow("a", int64(1)),
		sql.NewRow("b", "foo"),
		sql.NewRow("c", float64(3.14)),
		sql.NewRow("d", true),
	}, rows)

	v, err := globalConf.Int("a", 0)
	require.NoError(err)
	require.Equal(int64(1), v)

	vs, err := globalConf.String("b", "")
	require.NoError(err)
	require.Equal("foo", vs)

	vf, err := globalConf.Float("c", 0.)
	require.NoError(err)
	require.Equal(float64(3.14), vf)

	vb, err := globalConf.Bool("d", false)
	require.NoError(err)
	require.True(vb)
}

func TestSetSession(t *testing.T) {
	require := require.New(t)

	globalConf := config.New()
	sessionConf := config.FromConfig(globalConf)

	ctx := sql.NewContext(context.TODO(), sql.NewBaseSession(sessionConf))

	node := NewSet(SessionScope, []Update{
		{"a", expression.NewLiteral(int64(3), sql.Int64)},
		{"c", expression.NewLiteral("foo", sql.Text)},
	})

	iter, err := node.RowIter(ctx)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Equal([]sql.Row{
		sql.NewRow("a", int64(3)),
		sql.NewRow("c", "foo"),
	}, rows)

	v, err := globalConf.Int("a", 0)
	require.NoError(err)
	require.Equal(int64(0), v)

	v, err = sessionConf.Int("a", 0)
	require.NoError(err)
	require.Equal(int64(3), v)

	vs, err := globalConf.String("c", "")
	require.NoError(err)
	require.Equal("", vs)

	vs, err = sessionConf.String("c", "")
	require.NoError(err)
	require.Equal("foo", vs)
}

func TestSetNoGlobal(t *testing.T) {
	require := require.New(t)

	conf := config.New()
	ctx := sql.NewContext(context.TODO(), sql.NewBaseSession(conf))

	node := NewSet(GlobalScope, []Update{
		{"a", expression.NewLiteral(int64(1), sql.Int64)},
		{"b", expression.NewLiteral("foo", sql.Text)},
	})

	_, err := node.RowIter(ctx)
	require.Error(err)
	require.True(ErrUnableToAccessGlobalConfig.Is(err))
}
