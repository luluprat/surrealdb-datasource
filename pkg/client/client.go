package client

import (
	"context"

	"github.com/surrealdb/surrealdb.go"
)

// SurrealConfig defines the configuration for the SurrealDB database.
type SurrealConfig struct {
	Database  string `json:"database,omitempty"`
	Endpoint  string `json:"endpoint,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Password  string `json:"password,omitempty"`
	Scope     string `json:"scope,omitempty"`
	Username  string `json:"username,omitempty"`
}

// SurrealDBClient defines the interface for the SurrealDB database.
type SurrealDBClient interface {
	Close()
	Create(thing string, data interface{}) (interface{}, error)
	Query(sql string, vars map[string]interface{}) (interface{}, error)
	Signin(auth *surrealdb.Auth) (interface{}, error)
	Use(namespace string, database string) error
}

// Client defines the client for the SurrealDB database.
type Client struct {
	db SurrealDBClient
}

// Use returns a new client for the SurrealDB database.
func Use(db SurrealDBClient) *Client {
	return &Client{db}
}

// Connect connects to the SurrealDB database.
func (c *Client) Connect(config *SurrealConfig) (bool, error) {
	authData := &surrealdb.Auth{
		Username: config.Username,
		Password: config.Password,
	}

	// Scope found in config but surrealdb.Auth for v0.3.0 does not have a Scope field. This needs review.
	// If config.Scope is essential for authentication, the method to include it needs to be determined
	// for surrealdb.go v0.3.0. It might be part of a DEFINE SCOPE statement on the server
	// or a specific JWT token.

	if _, err := c.db.Signin(authData); err != nil {
		return false, err
	}

	if err := c.db.Use(config.Namespace, config.Database); err != nil {
		return false, err
	}

	return true, nil
}

// QueryWithContext wraps the Query method to handle context for cancellation/timeout
func (c *Client) QueryWithContext(ctx context.Context, query string, args map[string]interface{}) (interface{}, error) {
	rc := make(chan interface{})
	ec := make(chan error)

	go func() {
		r, err := c.db.Query(query, args)
		if err != nil {
			ec <- err
			return
		}
		rc <- r
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-ec:
		return nil, err
	case result := <-rc:
		return result, nil
	}
}
