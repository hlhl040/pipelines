// Copyright 2020 The Kubeflow Authors
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
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"encoding/json"

	"github.com/cenkalti/backoff"
	"github.com/golang/glog"
	"github.com/kubeflow/pipelines/backend/src/cache/client"
	"github.com/kubeflow/pipelines/backend/src/cache/model"
	"github.com/kubeflow/pipelines/backend/src/cache/storage"
	"github.com/kubeflow/pipelines/backend/src/common/dbcreds"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	DefaultConnectionTimeout = "6m"
)

type ClientManager struct {
	db            *storage.DB
	cacheStore    storage.ExecutionCacheStoreInterface
	k8sCoreClient client.KubernetesCoreInterface
	time          util.TimeInterface
}

func (c *ClientManager) CacheStore() storage.ExecutionCacheStoreInterface {
	return c.cacheStore
}

func (c *ClientManager) KubernetesCoreClient() client.KubernetesCoreInterface {
	return c.k8sCoreClient
}

func (c *ClientManager) Close() {
	sqlDB, err := c.db.DB.DB()
	if err != nil {
		log.Printf("Failed to retrieve underlying sql.DB: %v", err)
		return
	}
	sqlDB.Close()
}

func (c *ClientManager) init(params WhSvrDBParameters, clientParams util.ClientParameters) {
	timeoutDuration, _ := time.ParseDuration(DefaultConnectionTimeout)
	db := initDBClient(params, timeoutDuration)

	c.time = util.NewRealTime()
	c.db = db
	c.cacheStore = storage.NewExecutionCacheStore(db, c.time)
	c.k8sCoreClient = client.CreateKubernetesCoreOrFatal(timeoutDuration, clientParams)
}

func initDBClient(params WhSvrDBParameters, initConnectionTimeout time.Duration) *storage.DB {
	driverName := params.dbDriver
	settings := dbSettings(params)
	util.TerminateIfError(settings.Validate())
	if warning := settings.IgnoredTLSWarning(); warning != "" {
		log.Print(warning)
	}
	// The standard logger, not glog: the cache server's entrypoint passes no
	// -logtostderr, and glog writes only ERROR and above to stderr.
	log.Print(settings.Describe(params.dbDriver))

	var dialector gorm.Dialector
	if settings.Enabled {
		dialector = initMysqlWithProvider(params, initConnectionTimeout)
	} else {
		var arg string

		switch driverName {
		case mysqlDBDriverDefault:
			arg = initMysql(params, initConnectionTimeout)
		default:
			glog.Fatalf("Driver %v is not supported", driverName)
		}
		dialector = mysql.Open(arg)
	}

	// db is safe for concurrent use by multiple goroutines
	// and maintains its own pool of idle connections.
	db, err := gorm.Open(dialector, &gorm.Config{})
	util.TerminateIfError(err)

	// Create table
	err = db.AutoMigrate(&model.ExecutionCache{})
	if err != nil {
		glog.Fatalf("Failed to initialize the databases.")
	}

	err = db.Migrator().AlterColumn(&model.ExecutionCache{}, "ExecutionOutput")
	if err != nil {
		glog.Fatalf("Failed to update the execution output type. Error: %s", err)
	}
	err = db.Migrator().AlterColumn(&model.ExecutionCache{}, "ExecutionTemplate")
	if err != nil {
		glog.Fatalf("Failed to update the execution template type. Error: %s", err)
	}

	var tableNames []string
	db.Raw(`show tables`).Pluck("Tables_in_caches", &tableNames)
	for _, tableName := range tableNames {
		log.Printf("%s", tableName)
	}

	return storage.NewDB(db)
}

func initMysql(params WhSvrDBParameters, initConnectionTimeout time.Duration) string {

	var mysqlExtraParams = map[string]string{}
	data := []byte(params.dbExtraParams)
	json.Unmarshal(data, &mysqlExtraParams)
	mysqlConfig := client.CreateMySQLConfig(
		params.dbUser,
		params.dbPwd,
		params.dbHost,
		params.dbPort,
		"",
		params.dbGroupConcatMaxLen,
		mysqlExtraParams,
	)

	var db *sql.DB
	var err error
	var operation = func() error {
		db, err = sql.Open(params.dbDriver, mysqlConfig.FormatDSN())
		if err != nil {
			return err
		}
		return nil
	}
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = initConnectionTimeout
	err = backoff.Retry(operation, b)

	defer db.Close()
	util.TerminateIfError(err)

	// Create database if not exist
	dbName := params.dbName
	operation = func() error {
		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
		if err != nil {
			return err
		}
		log.Printf("Database created")
		return nil
	}
	b = backoff.NewExponentialBackOff()
	b.MaxElapsedTime = initConnectionTimeout
	err = backoff.Retry(operation, b)

	operation = func() error {
		_, err = db.Exec(fmt.Sprintf("USE %s", dbName))
		if err != nil {
			return err
		}
		return nil
	}
	b = backoff.NewExponentialBackOff()
	b.MaxElapsedTime = initConnectionTimeout
	err = backoff.Retry(operation, b)

	util.TerminateIfError(err)
	mysqlConfig.DBName = dbName
	// When updating, return rows matched instead of rows affected. This counts rows that are being
	// set as the same values as before. If updating using a primary key and rows matched is 0, then
	// it means this row is not found.
	// Config reference: https://github.com/go-sql-driver/mysql#clientfoundrows
	mysqlConfig.ClientFoundRows = true
	return mysqlConfig.FormatDSN()
}

// dbSettings resolves the credential-provider configuration from flags.
// Everything after this point is shared with the API server.
func dbSettings(params WhSvrDBParameters) dbcreds.Settings {
	return dbcreds.Settings{
		Enabled:          dbcreds.ParseEnabled(params.dbProviderEnabled),
		ProviderName:     params.dbCredentialProvider,
		Password:         params.dbPwd,
		CABundlePath:     params.dbTLSCAPath,
		ProviderSettings: params.dbProviderSettings,
	}
}

// initMysqlWithProvider is the opt-in counterpart of initMysql. It obtains the
// connection from a credential provider, which allows a credential that is
// regenerated per connection and a verified TLS connection.
func initMysqlWithProvider(params WhSvrDBParameters, initConnectionTimeout time.Duration) gorm.Dialector {
	if params.dbDriver != mysqlDBDriverDefault {
		glog.Fatalf("Driver %v is not supported", params.dbDriver)
	}

	settings := dbSettings(params)
	provider, err := settings.NewProvider()
	util.TerminateIfError(err)
	if warning := settings.IgnoredPasswordWarning(); warning != "" {
		log.Print(warning)
	}

	target := mysqlTargetFromParams(params)
	ctx := context.Background()

	// The bootstrap connection names no database, because the database may not
	// exist yet.
	bootstrap, err := dbcreds.Open(ctx, provider, target)
	util.TerminateIfError(err)
	defer bootstrap.Close()

	warning, err := dbcreds.EnsureDatabase(params.dbName, initConnectionTimeout,
		func() error {
			_, execErr := bootstrap.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", params.dbName))
			return execErr
		},
		// IF NOT EXISTS already suppresses the "database exists" error.
		func(err error) error { return err },
		func() error { return dbcreds.Probe(ctx, provider, target, params.dbName) })
	util.TerminateIfError(err)
	if warning != "" {
		log.Print(warning)
	}

	target.DBName = params.dbName
	// When updating, return rows matched instead of rows affected. This counts rows that are being
	// set as the same values as before. If updating using a primary key and rows matched is 0, then
	// it means this row is not found.
	// Config reference: https://github.com/go-sql-driver/mysql#clientfoundrows
	target.Params["clientFoundRows"] = "true"

	db, err := dbcreds.Open(ctx, provider, target)
	util.TerminateIfError(err)
	return mysql.New(mysql.Config{Conn: db})
}

// mysqlTargetFromParams describes the MySQL endpoint to connect to. The
// resulting parameters match those the cache server has always used, so the
// connection is configured identically whichever provider supplies the
// credential.
func mysqlTargetFromParams(params WhSvrDBParameters) dbcreds.Target {
	connectionParams := map[string]string{
		"group_concat_max_len": params.dbGroupConcatMaxLen,
	}
	if params.dbExtraParams != "" {
		extraParams := map[string]string{}
		if err := json.Unmarshal([]byte(params.dbExtraParams), &extraParams); err != nil {
			log.Printf("Ignoring --db_extra_params because it is not a JSON object: %v", err)
		}
		for key, value := range extraParams {
			connectionParams[key] = value
		}
	}

	return dbcreds.Target{
		Driver: dbcreds.DriverMySQL,
		Host:   params.dbHost,
		Port:   params.dbPort,
		User:   params.dbUser,
		Params: connectionParams,
		TLS:    dbSettings(params).TLSOptions(),
	}
}

func NewClientManager(params WhSvrDBParameters, clientParams util.ClientParameters) ClientManager {
	clientManager := ClientManager{}
	clientManager.init(params, clientParams)

	return clientManager
}
