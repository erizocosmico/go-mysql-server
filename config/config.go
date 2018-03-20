package config

import (
	"sync"

	errors "gopkg.in/src-d/go-errors.v0"
)

// Config is a thead-safe container for the application's configuration.
// The configuration can have a parent configuration to be able to inherit
// config values from it.
type Config struct {
	mu     sync.RWMutex
	parent *Config
	kv     map[string]interface{}
}

// New creates an empty configuration.
func New() *Config {
	return &Config{kv: make(map[string]interface{})}
}

// FromConfig creates a new configuration from the given parent. This parent
// will be used only to read and will never be modified.
func FromConfig(c *Config) *Config {
	return &Config{parent: c, kv: make(map[string]interface{})}
}

func (c *Config) set(k string, v interface{}) {
	c.mu.Lock()
	c.kv[k] = v
	c.mu.Unlock()
}

func (c *Config) get(k string) (interface{}, bool) {
	c.mu.RLock()
	v, ok := c.kv[k]
	c.mu.RUnlock()

	if !ok && c.parent != nil {
		return c.parent.get(k)
	}

	return v, ok
}

// SetString stores a string value at key k.
func (c *Config) SetString(k string, v string) {
	c.set(k, v)
}

// SetInt stores an int64 value at key k.
func (c *Config) SetInt(k string, v int64) {
	c.set(k, v)
}

// SetBool stores a bool value at key k.
func (c *Config) SetBool(k string, v bool) {
	c.set(k, v)
}

// SetFloat stores a float64 value at key k.
func (c *Config) SetFloat(k string, v float64) {
	c.set(k, v)
}

// SetStringSlice stores a []string value at key k.
func (c *Config) SetStringSlice(k string, v ...string) {
	c.set(k, v)
}

// ErrInvalidType is returned when the type of the config value does not
// match with the requested type.
var ErrInvalidType = errors.NewKind("config: value is of type %T instead of %T for key %q")

// String returns the string value for the given key.
func (c *Config) String(k string, defaultValue string) (string, error) {
	v, ok := c.get(k)
	if !ok {
		return defaultValue, nil
	}

	if _, ok := v.(string); !ok {
		return "", ErrInvalidType.New(v, defaultValue, k)
	}

	return v.(string), nil
}

// Int returns the int64 value for the given key.
func (c *Config) Int(k string, defaultValue int64) (int64, error) {
	v, ok := c.get(k)
	if !ok {
		return defaultValue, nil
	}

	if _, ok := v.(int64); !ok {
		return 0, ErrInvalidType.New(v, defaultValue, k)
	}

	return v.(int64), nil
}

// Bool returns the bool value for the given key.
func (c *Config) Bool(k string, defaultValue bool) (bool, error) {
	v, ok := c.get(k)
	if !ok {
		return defaultValue, nil
	}

	if _, ok := v.(bool); !ok {
		return false, ErrInvalidType.New(v, defaultValue, k)
	}

	return v.(bool), nil
}

// Float returns the float64 value for the given key.
func (c *Config) Float(k string, defaultValue float64) (float64, error) {
	v, ok := c.get(k)
	if !ok {
		return defaultValue, nil
	}

	if _, ok := v.(float64); !ok {
		return .0, ErrInvalidType.New(v, defaultValue, k)
	}

	return v.(float64), nil
}

// StringSlice returns the []string value for the given key.
func (c *Config) StringSlice(k string, defaultValue []string) ([]string, error) {
	v, ok := c.get(k)
	if !ok {
		return defaultValue, nil
	}

	if _, ok := v.([]string); !ok {
		return nil, ErrInvalidType.New(v, defaultValue, k)
	}

	return v.([]string), nil
}
