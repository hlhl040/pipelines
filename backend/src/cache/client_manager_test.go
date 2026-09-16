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
	"github.com/kubeflow/pipelines/backend/src/cache/client"
	"github.com/kubeflow/pipelines/backend/src/common/dbcreds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestMySQLTargetFromParams(t *testing.T) {
	params := cacheParams()
	params.dbHost = "aurora.example.com"
	params.dbPort = "3307"
	params.dbUser = "kfp"

	target := mysqlTargetFromParams(params)
	assert.Equal(t, dbcreds.DriverMySQL, target.Driver)
	assert.Equal(t, "aurora.example.com", target.Host)
	assert.Equal(t, "3307", target.Port)
	assert.Equal(t, "kfp", target.User)
	assert.Equal(t, mysqlDBGroupConcatMaxLenDefault, target.Params["group_concat_max_len"])
	assert.Nil(t, target.TLS, "no CA bundle must leave the connection unencrypted, as it is today")
}

func TestMySQLTargetFromParamsExtraParams(t *testing.T) {
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
		{
			name:        "malformed input is ignored rather than fatal",
			extraParams: "not json",
			wantParams:  map[string]string{"group_concat_max_len": mysqlDBGroupConcatMaxLenDefault},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := cacheParams()
			params.dbExtraParams = test.extraParams
			assert.Equal(t, test.wantParams, mysqlTargetFromParams(params).Params)
		})
	}
}

func TestMySQLTargetFromParamsTLS(t *testing.T) {
	params := cacheParams()
	params.dbTLSCAPath = "/etc/db-tls/ca.pem"

	target := mysqlTargetFromParams(params)
	require.NotNil(t, target.TLS)
	assert.Equal(t, "/etc/db-tls/ca.pem", target.TLS.CABundlePath)
}

// TestMySQLTargetMatchesLegacyConfig pins the connection an existing
// installation gets. The provider path must produce the same DSN as
// client.CreateMySQLConfig, which the untouched legacy path still uses.
func TestMySQLTargetMatchesLegacyConfig(t *testing.T) {
	params := cacheParams()

	target := mysqlTargetFromParams(params)
	target.DBName = params.dbName
	target.Params["clientFoundRows"] = "true"

	got, err := dbcreds.MySQLConfig(target)
	require.NoError(t, err)

	legacy := client.CreateMySQLConfig(params.dbUser, "", params.dbHost, params.dbPort,
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

// The value arrives from a ConfigMap through argv, so it can be empty or an
// unexpanded reference when the key is absent. Neither may be fatal.
