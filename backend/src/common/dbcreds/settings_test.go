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
	"fmt"
	"testing"
	"time"

	"github.com/kubeflow/pipelines/backend/src/common/dbcreds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The switch reaches the process as a string from a ConfigMap, so it can be
// empty or an unexpanded reference when the key is absent. Neither may be fatal.
func TestParseEnabled(t *testing.T) {
	tests := []struct {
		raw  string
		want bool
	}{
		{raw: "", want: false},
		{raw: "$(DB_CREDENTIAL_PROVIDER_ENABLED)", want: false},
		{raw: "  ", want: false},
		{raw: "nonsense", want: false},
		{raw: "true", want: true},
		{raw: " true ", want: true},
		{raw: "false", want: false},
	}

	for _, test := range tests {
		t.Run(test.raw, func(t *testing.T) {
			assert.Equal(t, test.want, dbcreds.ParseEnabled(test.raw))
		})
	}
}

func TestSettingsValidate(t *testing.T) {
	assert.NoError(t, dbcreds.Settings{}.Validate(), "an installation that has not opted in is always valid")
	assert.NoError(t, dbcreds.Settings{Enabled: true, ProviderName: dbcreds.StaticProviderName}.Validate())

	err := dbcreds.Settings{Enabled: true}.Validate()
	require.Error(t, err, "enabling a provider without naming one is a configuration error")
	assert.Contains(t, err.Error(), dbcreds.StaticProviderName, "the error must list what is available")
}

func TestSettingsDescribe(t *testing.T) {
	off := dbcreds.Settings{}.Describe(dbcreds.DriverMySQL)
	assert.Contains(t, off, "credentials=configured password")
	assert.Contains(t, off, "TLS=disabled")

	on := dbcreds.Settings{Enabled: true, ProviderName: "aws-iam", CABundlePath: "/etc/db-tls/ca.pem"}.Describe(dbcreds.DriverMySQL)
	assert.Contains(t, on, `credentials="aws-iam" provider`)
	assert.Contains(t, on, "verified against /etc/db-tls/ca.pem")
}

func TestSettingsWarnings(t *testing.T) {
	// A CA bundle with the switch off is inert, and silence would hide that.
	assert.NotEmpty(t, dbcreds.Settings{CABundlePath: "/etc/db-tls/ca.pem"}.IgnoredTLSWarning())
	assert.Empty(t, dbcreds.Settings{Enabled: true, CABundlePath: "/etc/db-tls/ca.pem"}.IgnoredTLSWarning())

	// A provider named with the switch off, which falls back to the password
	// the operator was moving away from. The name is quoted into the message,
	// so that the log says which provider was meant to be in force.
	assert.Contains(t, dbcreds.Settings{ProviderName: "aws-iam"}.IgnoredProviderWarning(), `"aws-iam"`)
	assert.Empty(t, dbcreds.Settings{Enabled: true, ProviderName: "aws-iam"}.IgnoredProviderWarning())
	assert.Empty(t, dbcreds.Settings{}.IgnoredProviderWarning(),
		"an installation that has not opted in must stay silent")

	// A password a provider will not use. It is an argument rather than a
	// field, so the same settings answer differently for different passwords.
	provider := dbcreds.Settings{Enabled: true, ProviderName: "aws-iam"}
	assert.NotEmpty(t, provider.IgnoredPasswordWarning("hunter2"))
	assert.Empty(t, provider.IgnoredPasswordWarning(""), "there is no password to remove")

	static := dbcreds.Settings{Enabled: true, ProviderName: dbcreds.StaticProviderName}
	assert.Empty(t, static.IgnoredPasswordWarning("hunter2"), "the static provider does use the password")

	assert.Empty(t, dbcreds.Settings{}.IgnoredPasswordWarning("hunter2"),
		"the pre-provider path uses the password, so there is nothing to warn about")
}

func TestSettingsTLSOptions(t *testing.T) {
	assert.Nil(t, dbcreds.Settings{}.TLSOptions(), "no CA bundle must leave the connection unencrypted")

	options := dbcreds.Settings{CABundlePath: "/etc/db-tls/ca.pem"}.TLSOptions()
	require.NotNil(t, options)
	assert.Equal(t, "/etc/db-tls/ca.pem", options.CABundlePath)
}

func TestSettingsNewProvider(t *testing.T) {
	provider, err := dbcreds.Settings{
		Enabled:      true,
		ProviderName: dbcreds.StaticProviderName,
	}.NewProvider("hunter2")
	require.NoError(t, err)
	assert.Equal(t, dbcreds.StaticProviderName, provider.Name())

	_, err = dbcreds.Settings{
		Enabled:          true,
		ProviderName:     dbcreds.StaticProviderName,
		ProviderSettings: "not json",
	}.NewProvider("hunter2")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a JSON object")
}

func TestEnsureDatabaseCreates(t *testing.T) {
	warning, err := dbcreds.EnsureDatabase("mlpipeline", time.Second,
		func() error { return nil },
		func(err error) error { return err },
		func() error { return fmt.Errorf("probe must not run") })
	require.NoError(t, err)
	assert.Empty(t, warning, "creating the database is the strong outcome and needs no warning")
}

func TestEnsureDatabaseAcceptsExistingWhenRefused(t *testing.T) {
	warning, err := dbcreds.EnsureDatabase("mlpipeline", time.Second,
		func() error { return fmt.Errorf("ERROR: permission denied to create database (SQLSTATE 42501)") },
		func(err error) error { return err },
		func() error { return nil })
	require.NoError(t, err)
	assert.Contains(t, warning, "was not created")
	assert.Contains(t, warning, "already exists and is reachable")
}

func TestEnsureDatabaseFailsWhenUnreachable(t *testing.T) {
	_, err := dbcreds.EnsureDatabase("mlpipeline", time.Second,
		func() error { return fmt.Errorf("permission denied to create database") },
		func(err error) error { return err },
		func() error { return fmt.Errorf(`database "mlpipeline" does not exist`) })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not create database")
	assert.Contains(t, err.Error(), "could not reach it")
}

// Reachability only answers a refusal for lack of privilege. Any other failure
// must not be excused by the database happening to exist.
func TestEnsureDatabaseDoesNotMaskOtherFailures(t *testing.T) {
	probed := false
	_, err := dbcreds.EnsureDatabase("mlpipeline", 200*time.Millisecond,
		func() error { return fmt.Errorf("ERROR: syntax error at or near \"DATABASE\"") },
		func(err error) error { return err },
		func() error { probed = true; return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "syntax error")
	assert.False(t, probed, "a reachable database must not excuse an unrelated failure")
}

// tlsRequiringFactory stands in for a provider whose credential is unsafe to
// send over an unverified connection.
type tlsRequiringFactory struct{ fakeFactory }

func (tlsRequiringFactory) RequiresTLS() bool { return true }

// A provider that requires a verified connection is rejected from configuration
// alone, before anything reaches the cloud. An operator who has configured
// neither the CA bundle nor the provider's own settings is told about the CA
// bundle immediately, rather than fixing one problem and meeting the other.
func TestSettingsValidateHonoursProviderTLSRequirement(t *testing.T) {
	restoreRegistry(t)
	dbcreds.RegisterFactory(tlsRequiringFactory{fakeFactory{name: "needs-tls"}})
	dbcreds.RegisterFactory(fakeFactory{name: "no-tls-needed"})

	err := dbcreds.Settings{Enabled: true, ProviderName: "needs-tls"}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires a verified connection")
	assert.Contains(t, err.Error(), "DB_TLS_CA_PATH", "the error must name the setting to add")

	assert.NoError(t, dbcreds.Settings{Enabled: true, ProviderName: "needs-tls", CABundlePath: "/etc/db-tls/ca.pem"}.Validate())

	// A provider that does not need TLS answers false, which it must do
	// explicitly -- RequiresTLS is part of the interface precisely so that the
	// answer is never inferred from silence. The requirement stays the
	// provider's own rather than a rule in shared code.
	assert.NoError(t, dbcreds.Settings{Enabled: true, ProviderName: "no-tls-needed"}.Validate())

	// An unregistered name is reported by provider construction, not here.
	assert.NoError(t, dbcreds.Settings{Enabled: true, ProviderName: "unknown"}.Validate())
}
