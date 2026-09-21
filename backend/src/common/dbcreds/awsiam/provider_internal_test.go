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

package awsiam

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/kubeflow/pipelines/backend/src/common/dbcreds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingCredentials records how often the SDK asked for credentials, which is
// how the tests observe token regeneration.
type countingCredentials struct {
	retrievals int
	err        error
}

func (c *countingCredentials) Retrieve(context.Context) (aws.Credentials, error) {
	c.retrievals++
	if c.err != nil {
		return aws.Credentials{}, c.err
	}
	return aws.Credentials{
		AccessKeyID:     "AKIAEXAMPLE",
		SecretAccessKey: "secret",
		Source:          "test",
	}, nil
}

func testProvider(credentials aws.CredentialsProvider, now func() time.Time) *provider {
	return &provider{
		credentials: credentials,
		region:      "us-east-1",
		now:         now,
		tokens:      map[string]cachedToken{},
	}
}

func TestTokenIsASignedConnectRequest(t *testing.T) {
	p := testProvider(&countingCredentials{}, time.Now)

	token, err := p.token(context.Background(), "aurora.example.com:3306", "kfp")
	require.NoError(t, err)

	// The token is a presigned URL without its scheme.
	parsed, err := url.Parse("https://" + token)
	require.NoError(t, err)
	assert.Equal(t, "aurora.example.com:3306", parsed.Host)
	assert.Equal(t, "connect", parsed.Query().Get("Action"))
	assert.Equal(t, "kfp", parsed.Query().Get("DBUser"))
	assert.Equal(t, "900", parsed.Query().Get("X-Amz-Expires"), "RDS tokens are valid for 15 minutes")
	assert.Contains(t, parsed.Query().Get("X-Amz-Credential"), "us-east-1")
	assert.NotEmpty(t, parsed.Query().Get("X-Amz-Signature"))
}

func TestTokenIsCachedUntilItExpires(t *testing.T) {
	credentials := &countingCredentials{}
	clock := time.Now()
	p := testProvider(credentials, func() time.Time { return clock })

	first, err := p.token(context.Background(), "aurora.example.com:3306", "kfp")
	require.NoError(t, err)

	second, err := p.token(context.Background(), "aurora.example.com:3306", "kfp")
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Equal(t, 1, credentials.retrievals, "a cached token must not be re-signed")

	// Past the cache lifetime the token is regenerated, which is what keeps
	// expired credentials out of new connections.
	clock = clock.Add(tokenTTL + time.Second)
	third, err := p.token(context.Background(), "aurora.example.com:3306", "kfp")
	require.NoError(t, err)
	assert.Equal(t, 2, credentials.retrievals)
	assert.NotEmpty(t, third)
}

func TestTokenIsCachedPerHostAndUser(t *testing.T) {
	credentials := &countingCredentials{}
	p := testProvider(credentials, time.Now)

	_, err := p.token(context.Background(), "aurora.example.com:3306", "kfp")
	require.NoError(t, err)
	_, err = p.token(context.Background(), "aurora.example.com:3306", "cache-server")
	require.NoError(t, err)
	_, err = p.token(context.Background(), "replica.example.com:3306", "kfp")
	require.NoError(t, err)

	assert.Equal(t, 3, credentials.retrievals, "tokens are scoped to a database user at an endpoint")
}

func TestTokenReportsCredentialFailure(t *testing.T) {
	p := testProvider(&countingCredentials{err: fmt.Errorf("no identity token file")}, time.Now)

	_, err := p.token(context.Background(), "aurora.example.com:3306", "kfp")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "generate an RDS IAM authentication token")
	assert.Contains(t, err.Error(), "kfp", "the error must name the database user that failed")
	assert.Contains(t, err.Error(), "no identity token file")
}

func TestConnectorRequiresTLS(t *testing.T) {
	p := testProvider(&countingCredentials{}, time.Now)

	_, err := p.Connector(context.Background(), dbcreds.Target{
		Driver: dbcreds.DriverMySQL,
		Host:   "aurora.example.com",
		Port:   "3306",
		User:   "kfp",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires TLS")
	assert.Contains(t, err.Error(), "DB_TLS_CA_PATH", "the error must name the setting an operator has to add")
}

func TestConnectorRejectsUnsupportedDriver(t *testing.T) {
	p := testProvider(&countingCredentials{}, time.Now)

	_, err := p.Connector(context.Background(), dbcreds.Target{
		Driver: "sqlite",
		Host:   "aurora.example.com",
		Port:   "5432",
		User:   "kfp",
		TLS:    &dbcreds.TLSOptions{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `does not support driver "sqlite"`)
}

func TestPostgresConnectorBuildsWithTLS(t *testing.T) {
	p := testProvider(&countingCredentials{}, time.Now)

	connector, err := p.Connector(context.Background(), dbcreds.Target{
		Driver: dbcreds.DriverPostgreSQL,
		Host:   "aurora.example.com",
		Port:   "5432",
		User:   "kfp",
		DBName: "mlpipeline",
		TLS:    &dbcreds.TLSOptions{},
	})
	require.NoError(t, err)
	assert.NotNil(t, connector)
}

func TestPostgresConnectorRequiresTLS(t *testing.T) {
	p := testProvider(&countingCredentials{}, time.Now)

	_, err := p.Connector(context.Background(), dbcreds.Target{
		Driver: dbcreds.DriverPostgreSQL,
		Host:   "aurora.example.com",
		Port:   "5432",
		User:   "kfp",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires TLS")
}

func TestConnectorBuildsWithTLS(t *testing.T) {
	p := testProvider(&countingCredentials{}, time.Now)

	connector, err := p.Connector(context.Background(), dbcreds.Target{
		Driver: dbcreds.DriverMySQL,
		Host:   "aurora.example.com",
		Port:   "3306",
		User:   "kfp",
		DBName: "mlpipeline",
		TLS:    &dbcreds.TLSOptions{},
	})
	require.NoError(t, err)
	assert.NotNil(t, connector)
}

func TestProviderIsRegistered(t *testing.T) {
	assert.Contains(t, dbcreds.RegisteredNames(), ProviderName)
}

// isolateAWSEnvironment removes any ambient AWS configuration so the factory
// tests do not depend on the developer's machine or on CI's instance role.
func isolateAWSEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{"AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE"} {
		t.Setenv(name, "")
	}
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "absent-config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "absent-credentials"))
	// Without this the SDK would try the instance metadata service to discover
	// a region, which is slow to fail off an instance.
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

func TestFactoryRequiresARegion(t *testing.T) {
	isolateAWSEnvironment(t)

	_, err := factory{}.New(dbcreds.Config{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no AWS region is configured")
	assert.Contains(t, err.Error(), "DB_CREDENTIAL_PROVIDER_SETTINGS",
		"the error must name the setting an operator has to add")
}

func TestFactoryUsesConfiguredRegion(t *testing.T) {
	isolateAWSEnvironment(t)

	built, err := factory{}.New(dbcreds.Config{Settings: map[string]string{dbcreds.SettingRegion: "eu-west-1"}})
	require.NoError(t, err)
	assert.Equal(t, ProviderName, built.Name())
	assert.Equal(t, "eu-west-1", built.(*provider).region)
}

func TestFactoryFallsBackToTheEnvironmentRegion(t *testing.T) {
	isolateAWSEnvironment(t)
	t.Setenv("AWS_REGION", "ap-south-1")

	built, err := factory{}.New(dbcreds.Config{})
	require.NoError(t, err)
	assert.Equal(t, "ap-south-1", built.(*provider).region,
		"an unset region must defer to the pod's AWS environment")
}

// blockingCredentials stalls on first use, standing in for a cold or expiring
// credential cache reaching STS.
type blockingCredentials struct {
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (c *blockingCredentials) Retrieve(context.Context) (aws.Credentials, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return aws.Credentials{AccessKeyID: "AKIAEXAMPLE", SecretAccessKey: "secret", Source: "test"}, nil
}

// A stalled credential refresh must not block connections that already have a
// valid token. Holding the lock across generation would serialize the whole
// pool behind one slow STS call, which presents as database latency.
func TestSlowCredentialRetrievalDoesNotBlockCachedLookups(t *testing.T) {
	credentials := &blockingCredentials{release: make(chan struct{}), entered: make(chan struct{})}
	p := testProvider(credentials, time.Now)
	p.tokens["cached.example.com:3306|kfp"] = cachedToken{token: "already-valid", expires: time.Now().Add(tokenTTL)}

	stalled := make(chan struct{})
	go func() {
		defer close(stalled)
		_, _ = p.token(context.Background(), "cold.example.com:3306", "kfp")
	}()
	<-credentials.entered // the generating goroutine is now inside Retrieve

	done := make(chan string, 1)
	go func() {
		token, err := p.token(context.Background(), "cached.example.com:3306", "kfp")
		require.NoError(t, err)
		done <- token
	}()

	select {
	case token := <-done:
		assert.Equal(t, "already-valid", token)
	case <-time.After(5 * time.Second):
		t.Fatal("a cached lookup blocked behind an in-flight credential retrieval")
	}

	close(credentials.release)
	<-stalled
}

// The cleartext plugin sends the token verbatim, so a connection that may fall
// back to plaintext must never carry one. The rejection currently comes from
// MySQLConfig, with the provider holding a second guard behind it; this asserts
// the behavior rather than which layer produces it.
func TestConnectorRefusesCleartextWithPlaintextFallback(t *testing.T) {
	p := testProvider(&countingCredentials{}, time.Now)

	target := dbcreds.Target{
		Driver: dbcreds.DriverMySQL,
		Host:   "aurora.example.com",
		Port:   "3306",
		User:   "kfp",
		Params: map[string]string{"allowFallbackToPlaintext": "true"},
		TLS:    &dbcreds.TLSOptions{},
	}

	_, err := p.Connector(context.Background(), target)
	require.Error(t, err, "a connection permitting a plaintext fallback must not carry a cleartext credential")
	assert.Contains(t, err.Error(), "plaintext fallback")
}

// Enabling this provider without a CA bundle must be rejected from
// configuration alone, before any AWS call. Going through Settings pins
// registration, registry lookup and the requirement together; asserting the
// method in isolation would pass even if nothing ever consulted it.
func TestValidateRejectsThisProviderWithoutTLS(t *testing.T) {
	err := dbcreds.Settings{Enabled: true, ProviderName: ProviderName}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires a verified connection")
	assert.Contains(t, err.Error(), "DB_TLS_CA_PATH")

	assert.NoError(t, dbcreds.Settings{
		Enabled:      true,
		ProviderName: ProviderName,
		CABundlePath: "/etc/db-tls/ca.pem",
	}.Validate())
}
