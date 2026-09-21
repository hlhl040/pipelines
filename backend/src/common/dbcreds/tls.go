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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSOptions configures verification of the database server certificate.
type TLSOptions struct {
	// CABundlePath is a PEM bundle used to verify the server certificate, in
	// addition to the system roots. Managed databases publish their own CA, so
	// this is normally required to reach them.
	CABundlePath string
}

// TLSConfig builds the client TLS configuration for a database server reached
// at serverName.
//
// The result is assigned to mysql.Config.TLS rather than registered with
// mysql.RegisterTLSConfig, which mutates a process-global registry keyed by
// name and would collide if a binary ever opened two databases.
func TLSConfig(options *TLSOptions, serverName string) (*tls.Config, error) {
	if options == nil {
		return nil, nil
	}
	config := &tls.Config{ServerName: serverName}
	if options.CABundlePath == "" {
		return config, nil
	}

	caBundle, err := os.ReadFile(options.CABundlePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read DB_TLS_CA_PATH %q: %w", options.CABundlePath, err)
	}
	// Fall back to an empty pool when the system roots are unavailable, so a
	// bundle that is complete on its own still works.
	certPool, err := x509.SystemCertPool()
	if err != nil {
		certPool = x509.NewCertPool()
	}
	if !certPool.AppendCertsFromPEM(caBundle) {
		return nil, fmt.Errorf("DB_TLS_CA_PATH %q did not contain valid PEM certificates; point it at the database CA bundle", options.CABundlePath)
	}
	config.RootCAs = certPool
	return config, nil
}
