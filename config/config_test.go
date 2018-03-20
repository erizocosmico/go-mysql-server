package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	require := require.New(t)
	cfg := config()

	v, err := cfg.String("string", "default")
	require.NoError(err)
	require.Equal("foo", v)

	v, err = cfg.String("missing", "default")
	require.NoError(err)
	require.Equal("default", v)

	cfg.SetString("string", "overridden")
	v, err = cfg.String("string", "default")
	require.NoError(err)
	require.Equal("overridden", v)

	_, err = cfg.String("int", "default")
	require.Error(err)
	require.True(ErrInvalidType.Is(err))
}

func TestInt(t *testing.T) {
	require := require.New(t)
	cfg := config()

	v, err := cfg.Int("int", 3)
	require.NoError(err)
	require.Equal(int64(1), v)

	v, err = cfg.Int("missing", 3)
	require.NoError(err)
	require.Equal(int64(3), v)

	cfg.SetInt("int", 2)
	v, err = cfg.Int("int", 3)
	require.NoError(err)
	require.Equal(int64(2), v)

	_, err = cfg.Int("string", 1)
	require.Error(err)
	require.True(ErrInvalidType.Is(err))
}

func TestFloat(t *testing.T) {
	require := require.New(t)
	cfg := config()

	v, err := cfg.Float("float", 3.)
	require.NoError(err)
	require.Equal(float64(3.14), v)

	v, err = cfg.Float("missing", 3.)
	require.NoError(err)
	require.Equal(float64(3.), v)

	cfg.SetFloat("float", 2.)
	v, err = cfg.Float("float", 3.)
	require.NoError(err)
	require.Equal(float64(2.), v)

	_, err = cfg.Float("int", 3.15)
	require.Error(err)
	require.True(ErrInvalidType.Is(err))
}

func TestBool(t *testing.T) {
	require := require.New(t)
	cfg := config()

	v, err := cfg.Bool("bool", false)
	require.NoError(err)
	require.Equal(true, v)

	v, err = cfg.Bool("missing", false)
	require.NoError(err)
	require.Equal(false, v)

	cfg.SetBool("bool", false)
	v, err = cfg.Bool("bool", true)
	require.NoError(err)
	require.Equal(false, v)

	_, err = cfg.Bool("int", false)
	require.Error(err)
	require.True(ErrInvalidType.Is(err))
}

func TestStringSlice(t *testing.T) {
	var empty []string
	require := require.New(t)
	cfg := config()

	v, err := cfg.StringSlice("slice", empty)
	require.NoError(err)
	require.Equal([]string{"a", "b", "c"}, v)

	v, err = cfg.StringSlice("missing", empty)
	require.NoError(err)
	require.Equal(empty, v)

	cfg.SetStringSlice("slice", "a", "b")
	v, err = cfg.StringSlice("slice", empty)
	require.NoError(err)
	require.Equal([]string{"a", "b"}, v)

	_, err = cfg.StringSlice("int", empty)
	require.Error(err)
	require.True(ErrInvalidType.Is(err))
}

func config() *Config {
	parent := New()
	parent.SetString("string", "foo")
	parent.SetInt("int", 1)
	parent.SetFloat("float", 3.14)
	parent.SetBool("bool", true)
	parent.SetStringSlice("slice", "a", "b", "c")
	return FromConfig(parent)
}
