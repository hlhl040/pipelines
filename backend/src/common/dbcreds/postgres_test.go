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

package dbcreds_test

import (
	"testing"
	"time"

	"github.com/kubeflow/pipelines/backend/src/common/dbcreds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func postgresTarget() dbcreds.Target {
	return dbcreds.Target{
		Driver: dbcreds.DriverPostgreSQL,
		Host:   "postgresql",
		Port:   "5432",
		User:   "root",
		DBName: "mlpipeline",
	}
}

func TestPostgreSQLConfig(t *testing.T) {
	config, err := dbcreds.PostgreSQLConfig(postgresTarget())
	require.NoError(t, err)

	assert.Equal(t, "postgresql", config.Host)
	assert.Equal(t, uint16(5432), config.Port)
	assert.Equal(t, "root", config.User)
	assert.Equal(t, "mlpipeline", config.Database)

	// The credential is the provider's job, never part of the configuration
	// this function returns.
	assert.Empty(t, config.Password)
}

// TestPostgreSQLConfigDefaultsToUnencrypted pins today's behavior: the legacy
// connection string hardcodes sslmode=disable, and an installation that has not
// configured a CA bundle must keep getting exactly that.
func TestPostgreSQLConfigDefaultsToUnencrypted(t *testing.T) {
	config, err := dbcreds.PostgreSQLConfig(postgresTarget())
	require.NoError(t, err)
	assert.Nil(t, config.TLSConfig, "no CA bundle must leave the connection unencrypted")
}

func TestPostgreSQLConfigTLSModes(t *testing.T) {
	tests := []struct {
		name    string
		tls     *dbcreds.TLSOptions
		wantTLS bool
	}{
		{name: "no options stays unencrypted", tls: nil, wantTLS: false},
		{name: "options verify the server", tls: &dbcreds.TLSOptions{}, wantTLS: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := postgresTarget()
			target.TLS = test.tls

			config, err := dbcreds.PostgreSQLConfig(target)
			require.NoError(t, err)

			if !test.wantTLS {
				assert.Nil(t, config.TLSConfig)
				return
			}
			require.NotNil(t, config.TLSConfig)
			assert.False(t, config.TLSConfig.InsecureSkipVerify, "the server must always be verified")
		})
	}
}

func TestPostgreSQLConfigParams(t *testing.T) {
	target := postgresTarget()
	target.Params = map[string]string{
		"connect_timeout":  "7",
		"application_name": "kfp",
		"sslmode":          "require",
	}

	config, err := dbcreds.PostgreSQLConfig(target)
	require.NoError(t, err)

	// Recognized settings land on their typed field, unrecognized ones become
	// server runtime parameters.
	assert.Equal(t, 7*time.Second, config.ConnectTimeout)
	assert.Equal(t, "kfp", config.RuntimeParams["application_name"])

	// Params override the defaults this package sets, including sslmode.
	require.NotNil(t, config.TLSConfig, "sslmode=require must enable TLS")
}

func TestPostgreSQLConfigRejectsInvalidTargets(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*dbcreds.Target)
		wantErr string
	}{
		{
			name:    "wrong driver",
			mutate:  func(target *dbcreds.Target) { target.Driver = dbcreds.DriverMySQL },
			wantErr: `PostgreSQLConfig called with driver "mysql"`,
		},
		{
			name:    "empty host",
			mutate:  func(target *dbcreds.Target) { target.Host = "" },
			wantErr: "database host is empty",
		},
		{
			name:    "unparseable parameter",
			mutate:  func(target *dbcreds.Target) { target.Params = map[string]string{"port": "not-a-port"} },
			wantErr: "invalid PostgreSQL connection parameters",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := postgresTarget()
			test.mutate(&target)

			_, err := dbcreds.PostgreSQLConfig(target)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantErr)
		})
	}
}

func TestPostgreSQLConnectorIsUsable(t *testing.T) {
	config, err := dbcreds.PostgreSQLConfig(postgresTarget())
	require.NoError(t, err)
	assert.NotNil(t, dbcreds.PostgreSQLConnector(config))
}
