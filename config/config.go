// Package config manages the ehz configuration file at ~/.ehz/config.yaml.
// It provides types for loading, saving, and switching between named
// OpenShift environments.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	dirName  = ".ehz"
	fileName = "config.yaml"
)

// Config is the top-level structure of ~/.ehz/config.yaml.
type Config struct {
	APIVersion   string                 `yaml:"apiVersion"`
	Current      string                 `yaml:"current"`
	Environments map[string]Environment `yaml:"environments"`
}

// Environment holds the OpenShift settings for one named environment.
type Environment struct {
	Cluster   string    `yaml:"cluster"`
	Namespace string    `yaml:"namespace"`
	CreatedAt time.Time `yaml:"created-at,omitempty"`
}

// New returns an empty Config with the current API version set.
func New() *Config {
	return &Config{
		APIVersion:   "ehz/v1",
		Environments: make(map[string]Environment),
	}
}

// Load reads the config file from the default path. If the file does not exist
// it returns an empty Config rather than an error.
func Load() (*Config, error) {
	p, err := DefaultPath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return New(), nil
		}
		return nil, fmt.Errorf("reading config: %w", err)
	}

	c := New()
	if err := yaml.Unmarshal(data, c); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	if c.Environments == nil {
		c.Environments = make(map[string]Environment)
	}

	return c, nil
}

// Save writes the config to disk atomically via a temp-file rename.
func (c *Config) Save() error {
	p, err := DefaultPath()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return fmt.Errorf("creating config dir: %w", err)
	}

	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshalling config: %w", err)
	}

	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("writing config: %w", err)
	}
	defer os.Remove(tmp)

	if err := os.Rename(tmp, p); err != nil {
		return fmt.Errorf("finalising config: %w", err)
	}

	return nil
}

// Active returns the currently selected environment, or an error if no
// environment is active or the active name is not found in the config.
// It also validates that the required fields Cluster and Namespace are set.
func (c *Config) Active() (*Environment, error) {
	if c.Current == "" {
		return nil, errors.New("no active environment — run `ehz config use <env>` to set one up")
	}

	env, ok := c.Environments[c.Current]
	if !ok {
		return nil, fmt.Errorf("environment %q not found in config — run `ehz config view`", c.Current)
	}

	if env.Cluster == "" {
		return nil, fmt.Errorf("environment %q has no cluster URL — edit ~/.ehz/config.yaml", c.Current)
	}

	if env.Namespace == "" {
		return nil, fmt.Errorf("environment %q has no namespace — edit ~/.ehz/config.yaml", c.Current)
	}

	return &env, nil
}

// Upsert creates or updates the named environment and saves the config file.
// If no environment is currently active, the upserted name becomes active.
func (c *Config) Upsert(name string, env Environment) error {
	if env.CreatedAt.IsZero() {
		env.CreatedAt = time.Now()
	}

	c.Environments[name] = env
	if c.Current == "" {
		c.Current = name
	}

	return c.Save()
}

// Use switches the active environment to name and saves the config file.
func (c *Config) Use(name string) error {
	if _, ok := c.Environments[name]; !ok {
		return fmt.Errorf("environment %q does not exist — run `ehz config view` to see available environments", name)
	}

	c.Current = name

	return c.Save()
}

// DefaultPath returns the absolute path to the config file (~/.ehz/config.yaml).
func DefaultPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolving home dir: %w", err)
	}

	return filepath.Join(home, dirName, fileName), nil
}
