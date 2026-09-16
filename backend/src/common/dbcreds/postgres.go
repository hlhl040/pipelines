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
	"database/sql/driver"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// PostgreSQLConfig builds the driver configuration KFP uses to reach
// PostgreSQL.
//
// Unlike MySQL, the driver resolves TLS itself from sslmode and sslrootcert, so
// no client tls.Config is assembled here. Target.Params overrides every setting,
// which is how operators reach connection options KFP does not expose directly.
//
// The returned configuration carries no password. Providers set the credential
// themselves, so it never passes through a connection string that could be
// logged.
func PostgreSQLConfig(t Target) (*pgx.ConnConfig, error) {
	if t.Driver != DriverPostgreSQL {
		return nil, fmt.Errorf("PostgreSQLConfig called with driver %q; use %q", t.Driver, DriverPostgreSQL)
	}
	if t.Host == "" {
		return nil, fmt.Errorf("database host is empty; set the host for driver %q", DriverPostgreSQL)
	}

	settings := map[string]string{
		"host":    t.Host,
		"user":    t.User,
		"sslmode": sslMode(t.TLS),
	}
	if t.Port != "" {
		settings["port"] = t.Port
	}
	if t.DBName != "" {
		settings["database"] = t.DBName
	}
	if t.TLS != nil && t.TLS.CABundlePath != "" {
		settings["sslrootcert"] = t.TLS.CABundlePath
	}
	for key, value := range t.Params {
		settings[key] = value
	}

	config, err := pgx.ParseConfig(keywordValueString(settings))
	if err != nil {
		return nil, fmt.Errorf("invalid PostgreSQL connection parameters for %s: %w", t.Host, err)
	}
	return config, nil
}

// PostgreSQLConnector applies opts and returns the connector.
//
// Options are the driver's extension point for behavior that is not a settable
// field: a provider that refreshes its credential per connection installs
// stdlib.OptionBeforeConnect here.
func PostgreSQLConnector(config *pgx.ConnConfig, opts ...stdlib.OptionOpenDB) driver.Connector {
	return stdlib.GetConnector(*config, opts...)
}

// sslMode maps the TLS options onto the driver's sslmode setting. A nil options
// value leaves the connection unencrypted, which is what KFP has always done.
func sslMode(options *TLSOptions) string {
	if options == nil {
		return "disable"
	}
	return "verify-full"
}

// keywordValueString renders settings in the keyword/value form the driver
// parses, quoting values so that empty strings and spaces survive.
func keywordValueString(settings map[string]string) string {
	keys := make([]string, 0, len(settings))
	for key := range settings {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var builder strings.Builder
	for _, key := range keys {
		if builder.Len() > 0 {
			builder.WriteByte(' ')
		}
		value := strings.ReplaceAll(settings[key], `\`, `\\`)
		value = strings.ReplaceAll(value, `'`, `\'`)
		fmt.Fprintf(&builder, "%s='%s'", key, value)
	}
	return builder.String()
}
