// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbcreds

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
)

// Settings is the resolved credential-provider configuration a binary supplies.
// Each binary reads it from its own source -- the API server from viper, the
// cache server from flags -- and everything after that is shared, so the two
// cannot drift apart.
//
// Nothing here logs or terminates: the two binaries disagree on both, so the
// methods return errors and strings and let the caller decide.
type Settings struct {
	// Enabled is the switch. When false the caller must build the connection
	// the way it did before credential providers existed.
	Enabled bool

	// ProviderName selects the provider. Required when Enabled.
	ProviderName string

	// Password is the configured database password, used by the static provider
	// and ignored by the others.
	Password string

	// CABundlePath verifies the database server certificate. Empty leaves the
	// connection unencrypted.
	CABundlePath string

	// ProviderSettings is provider-specific configuration, as a JSON object.
	ProviderSettings string
}

// ParseEnabled interprets a switch that reaches the process as a string. The
// value comes from a ConfigMap, so it can be empty or an unexpanded reference
// when the key is absent; neither may be fatal.
func ParseEnabled(value string) bool {
	enabled, err := strconv.ParseBool(strings.TrimSpace(value))
	return err == nil && enabled
}

// Validate rejects a configuration that cannot produce a provider. Callers run
// it before logging anything, so an operator reading the failure is not first
// shown a connection summary naming a provider that does not exist.
func (s Settings) Validate() error {
	if s.Enabled && s.ProviderName == "" {
		return fmt.Errorf("a database credential provider is enabled but none is named; set it to one of: %v", RegisteredNames())
	}
	return nil
}

// Describe renders how the connection is authenticated and protected. Without it
// the active mode is visible only by reading a pod's configuration, which is the
// wrong place to look when a rollout misbehaves.
func (s Settings) Describe(driverName string) string {
	if !s.Enabled {
		return fmt.Sprintf("DB connection: driver=%s, credentials=configured password, TLS=disabled", driverName)
	}
	transport := "disabled"
	if s.CABundlePath != "" {
		transport = "verified against " + s.CABundlePath
	}
	return fmt.Sprintf("DB connection: driver=%s, credentials=%q provider, TLS=%s", driverName, s.ProviderName, transport)
}

// IgnoredPasswordWarning reports a password that will not be used, or an empty
// string when there is nothing to say.
func (s Settings) IgnoredPasswordWarning() string {
	if s.Enabled && s.ProviderName != StaticProviderName && s.Password != "" {
		return fmt.Sprintf("a %q credential provider is enabled, so the configured database password is ignored; remove it from the database Secret", s.ProviderName)
	}
	return ""
}

// IgnoredTLSWarning reports a CA bundle that will not take effect.
func (s Settings) IgnoredTLSWarning() string {
	if !s.Enabled && s.CABundlePath != "" {
		return "a database CA bundle is configured but no credential provider is enabled, so it has no effect"
	}
	return ""
}

// TLSOptions returns nil when no CA bundle is configured, which leaves the
// connection unencrypted as it has always been.
func (s Settings) TLSOptions() *TLSOptions {
	if s.CABundlePath == "" {
		return nil
	}
	return &TLSOptions{CABundlePath: s.CABundlePath}
}

// NewProvider builds the configured provider.
func (s Settings) NewProvider() (Provider, error) {
	settings := map[string]string{}
	if raw := strings.TrimSpace(s.ProviderSettings); raw != "" {
		if err := json.Unmarshal([]byte(raw), &settings); err != nil {
			return nil, fmt.Errorf("the credential provider settings are not a JSON object: %w", err)
		}
	}
	return NewProvider(s.ProviderName, Config{Password: s.Password, Settings: settings})
}

// Open builds the connector for the target and wraps it in a pooled handle. As
// with sql.Open, no connection is made until the handle is first used.
func Open(ctx context.Context, provider Provider, target Target) (*sql.DB, error) {
	connector, err := provider.Connector(ctx, target)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}

// Probe reports whether the named database exists and is usable, using the same
// credentials the connection itself will use.
func Probe(ctx context.Context, provider Provider, target Target, dbName string) error {
	target.DBName = dbName
	db, err := Open(ctx, provider, target)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Ping()
}

// permissionDenied reports whether the database refused the statement for lack
// of privilege. Only then is a reachable database a reason to continue: any
// other failure -- a syntax error, a full disk, an unreachable server -- is not
// answered by the database merely existing.
func permissionDenied(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	// PostgreSQL: "permission denied to create database" (SQLSTATE 42501).
	// MySQL: "Access denied for user ... to database ..." (error 1044).
	return strings.Contains(message, "permission denied") || strings.Contains(message, "access denied")
}

// EnsureDatabase creates dbName, or accepts a database that already exists and
// is usable when creation is refused for lack of privilege. It returns a warning
// describing that second outcome, so the caller can record that it proceeded on
// a weaker guarantee than creating the database itself.
//
// The two engines report a refused creation differently. MySQL checks existence
// before privilege, so it says "database exists" and tolerate covers it.
// PostgreSQL checks privilege first, so it says "permission denied" whether or
// not the database is there -- the error alone cannot distinguish a
// pre-provisioned database from a missing one. Rather than guess from the
// message, ask the database: probe opens the target and runs a trivial query.
//
// Probing inside the retry keeps the timeout unchanged. A user without CREATE
// DATABASE on an existing database succeeds on the first attempt instead of
// retrying until the deadline, and a genuinely missing database still fails.
func EnsureDatabase(dbName string, timeout time.Duration, create func() error, tolerate func(error) error, probe func() error) (string, error) {
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = timeout

	var warning string
	err := backoff.Retry(func() error {
		warning = ""
		createErr := tolerate(create())
		if createErr == nil {
			return nil
		}
		// Reachability is a weaker guarantee than having created the database,
		// so it is only accepted for the failure it actually explains.
		if !permissionDenied(createErr) {
			return createErr
		}
		if probeErr := probe(); probeErr != nil {
			return fmt.Errorf("could not create database %q (%w) and could not reach it (%v); "+
				"create the database, or grant the user permission to create it", dbName, createErr, probeErr)
		}
		warning = fmt.Sprintf("database %q was not created (%v); continuing because it already exists and is reachable", dbName, createErr)
		return nil
	}, b)
	return warning, err
}
