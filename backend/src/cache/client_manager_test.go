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

package main

import (
	"testing"

	mysqlStd "github.com/go-sql-driver/mysql"
	commonsql "github.com/kubeflow/pipelines/backend/src/apiserver/common/sql"
	"github.com/kubeflow/pipelines/backend/src/common/dbcreds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDBExtraParams(t *testing.T) {
	tests := []struct {
		name           string
		dbExtraParams  string
		expectedParams map[string]string
		expectError    bool
	}{
		{
			name:           "empty input yields empty map",
			dbExtraParams:  "",
			expectedParams: map[string]string{},
			expectError:    false,
		},
		{
			name:           "valid empty JSON object",
			dbExtraParams:  "{}",
			expectedParams: map[string]string{},
			expectError:    false,
		},
		{
			name:           "valid PostgreSQL TLS params",
			dbExtraParams:  `{"sslmode":"verify-full","sslrootcert":"/certs/ca.crt"}`,
			expectedParams: map[string]string{"sslmode": "verify-full", "sslrootcert": "/certs/ca.crt"},
			expectError:    false,
		},
		{
			name:           "valid MySQL TLS params",
			dbExtraParams:  `{"tls":"true","charset":"utf8mb4"}`,
			expectedParams: map[string]string{"tls": "true", "charset": "utf8mb4"},
			expectError:    false,
		},
		{
			name:          "malformed JSON with single quotes",
			dbExtraParams: `{'sslmode':'require'}`,
			expectError:   true,
		},
		{
			name:          "malformed JSON with key=value syntax",
			dbExtraParams: "{sslmode=verify-full}",
			expectError:   true,
		},
		{
			name:          "malformed JSON without braces",
			dbExtraParams: "sslmode=disable",
			expectError:   true,
		},
		{
			name:          "truncated JSON",
			dbExtraParams: `{"sslmode":`,
			expectError:   true,
		},
		{
			name:          "valid JSON with non-string value",
			dbExtraParams: `{"timeout": 30}`,
			expectError:   true,
		},
		{
			name:          "valid JSON array instead of object",
			dbExtraParams: `["sslmode","verify-full"]`,
			expectError:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			extraParams, err := parseDBExtraParams(test.dbExtraParams)
			if test.expectError {
				assert.Error(t, err)
				assert.Nil(t, extraParams)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expectedParams, extraParams)
			}
		})
	}
}

func cacheParams() WhSvrDBParameters {
	return WhSvrDBParameters{
		dbDriver:            mysqlDBDriverDefault,
		dbHost:              mysqlDBHostDefault,
		dbPort:              mysqlDBPortDefault,
		dbName:              "cachedb",
		dbUser:              "root",
		dbGroupConcatMaxLen: mysqlDBGroupConcatMaxLenDefault,
	}
}

// The adapter is the only part that is binary-specific; everything it produces
// is exercised by the dbcreds settings tests.
func TestDBSettingsFromParams(t *testing.T) {
	assert.False(t, dbSettings(cacheParams()).Enabled, "an installation that has not opted in must keep the existing path")

	params := cacheParams()
	params.dbProviderEnabled = "true"
	params.dbCredentialProvider = "aws-iam"
	params.dbTLSCAPath = "/etc/db-tls/ca.pem"
	params.dbProviderSettings = `{"region":"us-east-1"}`

	settings := dbSettings(params)
	assert.True(t, settings.Enabled)
	assert.Equal(t, "aws-iam", settings.ProviderName)
	assert.Equal(t, "/etc/db-tls/ca.pem", settings.CABundlePath)
	assert.Equal(t, `{"region":"us-east-1"}`, settings.ProviderSettings)
}

func TestTargetFromParams(t *testing.T) {
	params := cacheParams()
	params.dbHost = "aurora.example.com"
	params.dbPort = "3307"
	params.dbUser = "kfp"

	target := targetFromParams(params)
	assert.Equal(t, dbcreds.DriverMySQL, target.Driver)
	assert.Equal(t, "aurora.example.com", target.Host)
	assert.Equal(t, "3307", target.Port)
	assert.Equal(t, "kfp", target.User)
	assert.Equal(t, mysqlDBGroupConcatMaxLenDefault, target.Params["group_concat_max_len"])
	assert.Nil(t, target.TLS, "no CA bundle must leave the connection unencrypted, as it is today")
}

func TestTargetFromParamsExtraParams(t *testing.T) {
	tests := []struct {
		name        string
		extraParams string
		wantParams  map[string]string
	}{
		{
			name:        "empty is ignored",
			extraParams: "",
			wantParams:  map[string]string{"group_concat_max_len": mysqlDBGroupConcatMaxLenDefault},
		},
		{
			name:        "overrides the defaults",
			extraParams: `{"group_concat_max_len":"512","readTimeout":"30s"}`,
			wantParams:  map[string]string{"group_concat_max_len": "512", "readTimeout": "30s"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := cacheParams()
			params.dbExtraParams = test.extraParams
			assert.Equal(t, test.wantParams, targetFromParams(params).Params)
		})
	}
}

func TestTargetFromParamsTLS(t *testing.T) {
	params := cacheParams()
	params.dbTLSCAPath = "/etc/db-tls/ca.pem"

	target := targetFromParams(params)
	require.NotNil(t, target.TLS)
	assert.Equal(t, "/etc/db-tls/ca.pem", target.TLS.CABundlePath)
}

// TestMySQLTargetMatchesLegacyConfig pins the connection an existing
// installation gets. The provider path must produce the same DSN as
// client.CreateMySQLConfig, which the untouched legacy path still uses.
func TestMySQLTargetMatchesLegacyConfig(t *testing.T) {
	params := cacheParams()

	target := targetFromParams(params)
	target.DBName = params.dbName
	target.Params["clientFoundRows"] = "true"

	got, err := dbcreds.MySQLConfig(target)
	require.NoError(t, err)

	legacy := commonsql.CreateMySQLConfig(params.dbUser, "", params.dbHost, params.dbPort,
		params.dbName, params.dbGroupConcatMaxLen, map[string]string{})
	legacy.ClientFoundRows = true
	want, err := mysqlStd.ParseDSN(legacy.FormatDSN())
	require.NoError(t, err)

	assert.Equal(t, want.FormatDSN(), got.FormatDSN())
}

// TestCredentialProvidersAreRegistered guards the blank import of
// dbcreds/all. Without it the binary builds and starts, and only fails once an
// operator sets --db_credential_provider, so a compile-time check is not enough.
func TestCredentialProvidersAreRegistered(t *testing.T) {
	// The provider name is spelled out rather than imported from its package:
	// importing it here would run its init and register it, so the test would
	// pass even with the blank import gone.
	registered := dbcreds.RegisteredNames()
	assert.Contains(t, registered, dbcreds.StaticProviderName)
	assert.Contains(t, registered, "aws-iam")
}

// The values arrive from a ConfigMap through argv, so each can be empty or an
// unexpanded reference when the key is absent. Neither may be fatal.
//
// Kubernetes substitutes only variables that are defined, so an installation
// whose pipeline-install-config predates these keys leaves the $(VAR) reference
// in place. Left alone it would read as a configured value: a CA bundle the
// operator is then told is being ignored, or a provider name no registry holds.
func TestDiscardUnexpandedReferences(t *testing.T) {
	assert.Empty(t, discardUnexpanded("db_tls_ca_path", "$(DB_TLS_CA_PATH)"))
	assert.Empty(t, discardUnexpanded("db_credential_provider", " $(DB_CREDENTIAL_PROVIDER) "))

	// A real value survives, including one that merely contains a reference:
	// only a value that is nothing but a reference is a substitution that did
	// not happen.
	assert.Equal(t, "/etc/db-tls/ca.pem", discardUnexpanded("db_tls_ca_path", "/etc/db-tls/ca.pem"))
	assert.Equal(t, "aws-iam", discardUnexpanded("db_credential_provider", "aws-iam"))
	assert.Equal(t, "", discardUnexpanded("db_credential_provider", ""))
	assert.Equal(t, "/etc/$(DIR)/ca.pem", discardUnexpanded("db_tls_ca_path", "/etc/$(DIR)/ca.pem"))
}

// The unexpanded switch must not reach dbSettings as a truthy string, and the
// unexpanded CA path and provider name must not trip the warnings about
// settings that have no effect: an installation that configured none of this
// has nothing to be warned about.
func TestUnexpandedReferencesLeaveTheDefaultPath(t *testing.T) {
	params := cacheParams()
	params.dbProviderEnabled = discardUnexpanded("db_credential_provider_enabled", "$(DB_CREDENTIAL_PROVIDER_ENABLED)")
	params.dbCredentialProvider = discardUnexpanded("db_credential_provider", "$(DB_CREDENTIAL_PROVIDER)")
	params.dbTLSCAPath = discardUnexpanded("db_tls_ca_path", "$(DB_TLS_CA_PATH)")

	settings := dbSettings(params)
	assert.False(t, settings.Enabled)
	assert.Empty(t, settings.IgnoredProviderWarning())
	assert.Empty(t, settings.IgnoredTLSWarning())
}
