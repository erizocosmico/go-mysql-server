package config

import (
	"os"
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

func TestLoadFromEnv(t *testing.T) {
	require := require.New(t)

	os.Setenv("ENV_FLOAT1", ".1")
	os.Setenv("ENV_FLOAT2", "2.")
	os.Setenv("ENV_FLOAT3", "2.5")
	os.Setenv("ENV_INT", "25")
	os.Setenv("ENV_BOOL1", "true")
	os.Setenv("ENV_BOOL2", "false")
	os.Setenv("ENV_STR", "something")
	os.Setenv("ENV_UNUSED", "yada yada")

	c := New()
	require.NoError(c.LoadFromEnv(
		"ENV_FLOAT1",
		"ENV_FLOAT2",
		"ENV_FLOAT3",
		"ENV_INT",
		"ENV_BOOL1",
		"ENV_BOOL2",
		"ENV_STR",
	))

	require.Equal(map[string]interface{}{
		"ENV_FLOAT1": .1,
		"ENV_FLOAT2": 2.,
		"ENV_FLOAT3": 2.5,
		"ENV_INT":    int64(25),
		"ENV_BOOL1":  true,
		"ENV_BOOL2":  false,
		"ENV_STR":    "something",
	}, c.kv)
}

func TestIsBool(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
	}{
		{"true", true},
		{"false", true},
		{"TRUE", true},
		{"t", false},
		{"12345", false},
		{"fdsfjlsdj", false},
	}

	for _, tt := range testCases {
		t.Run(tt.input, func(t *testing.T) {
			require.Equal(t, tt.expected, isBool(tt.input))
		})
	}
}

func TestIsInt(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
	}{
		{"1", true},
		{"123", true},
		{"0123", true},
		{"t", false},
		{"123.45", false},
		{"1234f", false},
	}

	for _, tt := range testCases {
		t.Run(tt.input, func(t *testing.T) {
			require.Equal(t, tt.expected, isInt(tt.input))
		})
	}
}

func TestIsFloat(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
	}{
		{".1", true},
		{"1.", true},
		{"1.23", true},
		{"123", false},
		{"lkjfsldfjk", false},
		{"123.4f", false},
	}

	for _, tt := range testCases {
		t.Run(tt.input, func(t *testing.T) {
			require.Equal(t, tt.expected, isFloat(tt.input))
		})
	}
}
