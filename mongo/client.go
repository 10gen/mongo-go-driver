// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongo

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/internal/httputil"
	"go.mongodb.org/mongo-driver/v2/internal/logger"
	"go.mongodb.org/mongo-driver/v2/internal/mongoutil"
	"go.mongodb.org/mongo-driver/v2/internal/ptrutil"
	"go.mongodb.org/mongo-driver/v2/internal/serverselector"
	"go.mongodb.org/mongo-driver/v2/internal/uuid"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/auth"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/mongocrypt"
	mcopts "go.mongodb.org/mongo-driver/v2/x/mongo/driver/mongocrypt/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/operation"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/session"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

const (
	defaultLocalThreshold = 15 * time.Millisecond
	defaultMaxPoolSize    = 100
)

var (
	// keyVaultCollOpts specifies options used to communicate with the key vault collection
	keyVaultCollOpts = options.Collection().SetReadConcern(readconcern.Majority()).
				SetWriteConcern(writeconcern.Majority())

	endSessionsBatchSize = 10000
)

// Client is a handle representing a pool of connections to a MongoDB deployment. It is safe for concurrent use by
// multiple goroutines.
//
// The Client type opens and closes connections automatically and maintains a pool of idle connections. For
// connection pool configuration options, see documentation for the ClientOptions type in the mongo/options package.
type Client struct {
	id             uuid.UUID
	deployment     driver.Deployment
	localThreshold time.Duration
	retryWrites    bool
	retryReads     bool
	clock          *session.ClusterClock
	readPreference *readpref.ReadPref
	readConcern    *readconcern.ReadConcern
	writeConcern   *writeconcern.WriteConcern
	bsonOpts       *options.BSONOptions
	registry       *bson.Registry
	monitor        *event.CommandMonitor
	serverAPI      *driver.ServerAPIOptions
	serverMonitor  *event.ServerMonitor
	sessionPool    *session.Pool
	timeout        *time.Duration
	httpClient     *http.Client
	logger         *logger.Logger

	// in-use encryption fields
	isAutoEncryptionSet bool
	keyVaultClientFLE   *Client
	keyVaultCollFLE     *Collection
	mongocryptdFLE      *mongocryptdClient
	cryptFLE            driver.Crypt
	metadataClientFLE   *Client
	internalClientFLE   *Client
	encryptedFieldsMap  map[string]interface{}
	authenticator       driver.Authenticator
}

// Connect creates a new Client and then initializes it using the Connect method.
//
// When creating an options.ClientOptions, the order the methods are called matters. Later Set*
// methods will overwrite the values from previous Set* method invocations. This includes the
// ApplyURI method. This allows callers to determine the order of precedence for option
// application. For instance, if ApplyURI is called before SetAuth, the Credential from
// SetAuth will overwrite the values from the connection string. If ApplyURI is called
// after SetAuth, then its values will overwrite those from SetAuth.
//
// The opts parameter is processed using options.MergeClientOptions, which will overwrite entire
// option fields of previous options, there is no partial overwriting. For example, if Username is
// set in the Auth field for the first option, and Password is set for the second but with no
// Username, after the merge the Username field will be empty.
//
// The NewClient function does not do any I/O and returns an error if the given options are invalid.
// The Client.Connect method starts background goroutines to monitor the state of the deployment and does not do
// any I/O in the main goroutine to prevent the main goroutine from blocking. Therefore, it will not error if the
// deployment is down.
//
// The Client.Ping method can be used to verify that the deployment is successfully connected and the
// Client was correctly configured.
func Connect(opts ...*options.ClientOptions) (*Client, error) {
	c, err := newClient(opts...)
	if err != nil {
		return nil, err
	}
	err = c.connect()
	if err != nil {
		return nil, err
	}
	return c, nil
}

// newClient creates a new client to connect to a deployment specified by the uri.
//
// When creating an options.ClientOptions, the order the methods are called matters. Later Set*
// methods will overwrite the values from previous Set* method invocations. This includes the
// ApplyURI method. This allows callers to determine the order of precedence for option
// application. For instance, if ApplyURI is called before SetAuth, the Credential from
// SetAuth will overwrite the values from the connection string. If ApplyURI is called
// after SetAuth, then its values will overwrite those from SetAuth.
//
// The opts parameter is processed using options.MergeClientOptions, which will overwrite entire
// option fields of previous options, there is no partial overwriting. For example, if Username is
// set in the Auth field for the first option, and Password is set for the second but with no
// Username, after the merge the Username field will be empty.
func newClient(opts ...*options.ClientOptions) (*Client, error) {
	clientOpts := options.MergeClientOptions(opts...)

	id, err := uuid.New()
	if err != nil {
		return nil, err
	}
	client := &Client{id: id}

	// ClusterClock
	client.clock = new(session.ClusterClock)

	// LocalThreshold
	client.localThreshold = defaultLocalThreshold
	if clientOpts.LocalThreshold != nil {
		client.localThreshold = *clientOpts.LocalThreshold
	}
	// Monitor
	if clientOpts.Monitor != nil {
		client.monitor = clientOpts.Monitor
	}
	// ServerMonitor
	if clientOpts.ServerMonitor != nil {
		client.serverMonitor = clientOpts.ServerMonitor
	}
	// ReadConcern
	client.readConcern = &readconcern.ReadConcern{}
	if clientOpts.ReadConcern != nil {
		client.readConcern = clientOpts.ReadConcern
	}
	// ReadPreference
	client.readPreference = readpref.Primary()
	if clientOpts.ReadPreference != nil {
		client.readPreference = clientOpts.ReadPreference
	}
	// BSONOptions
	if clientOpts.BSONOptions != nil {
		client.bsonOpts = clientOpts.BSONOptions
	}
	// Registry
	client.registry = defaultRegistry
	if clientOpts.Registry != nil {
		client.registry = clientOpts.Registry
	}
	// RetryWrites
	client.retryWrites = true // retry writes on by default
	if clientOpts.RetryWrites != nil {
		client.retryWrites = *clientOpts.RetryWrites
	}
	client.retryReads = true
	if clientOpts.RetryReads != nil {
		client.retryReads = *clientOpts.RetryReads
	}
	// Timeout
	client.timeout = clientOpts.Timeout
	client.httpClient = clientOpts.HTTPClient
	// WriteConcern
	if clientOpts.WriteConcern != nil {
		client.writeConcern = clientOpts.WriteConcern
	}
	// AutoEncryptionOptions
	if clientOpts.AutoEncryptionOptions != nil {
		client.isAutoEncryptionSet = true
		if err := client.configureAutoEncryption(clientOpts); err != nil {
			return nil, err
		}
	} else {
		client.cryptFLE = clientOpts.Crypt
	}

	// Deployment
	if clientOpts.Deployment != nil {
		client.deployment = clientOpts.Deployment
	}

	// Set default options
	if clientOpts.MaxPoolSize == nil {
		defaultMaxPoolSize := uint64(defaultMaxPoolSize)
		clientOpts.MaxPoolSize = &defaultMaxPoolSize
	}

	if clientOpts.Auth != nil {
		client.authenticator, err = auth.CreateAuthenticator(
			clientOpts.Auth.AuthMechanism,
			topology.ConvertCreds(clientOpts.Auth),
			clientOpts.HTTPClient,
		)
		if err != nil {
			return nil, fmt.Errorf("error creating authenticator: %w", err)
		}
	}

	cfg, err := topology.NewConfigFromOptionsWithAuthenticator(clientOpts, client.clock, client.authenticator)
	if err != nil {
		return nil, err
	}

	var connectTimeout time.Duration
	if clientOpts.ConnectTimeout != nil {
		connectTimeout = *clientOpts.ConnectTimeout
	}

	client.serverAPI = topology.ServerAPIFromServerOptions(connectTimeout, cfg.ServerOpts)

	if client.deployment == nil {
		client.deployment, err = topology.New(cfg)
		if err != nil {
			return nil, replaceErrors(err)
		}
	}

	// Create a logger for the client.
	client.logger, err = newLogger(clientOpts.LoggerOptions)
	if err != nil {
		return nil, fmt.Errorf("invalid logger options: %w", err)
	}

	return client, nil
}

// Connect initializes the Client by starting background monitoring goroutines.
// If the Client was created using the NewClient function, this method must be called before a Client can be used.
//
// Connect starts background goroutines to monitor the state of the deployment and does not do any I/O in the main
// goroutine. The Client.Ping method can be used to verify that the connection was created successfully.
func (c *Client) connect() error {
	if connector, ok := c.deployment.(driver.Connector); ok {
		err := connector.Connect()
		if err != nil {
			return replaceErrors(err)
		}
	}

	if c.mongocryptdFLE != nil {
		if err := c.mongocryptdFLE.connect(); err != nil {
			return err
		}
	}

	if c.internalClientFLE != nil {
		if err := c.internalClientFLE.connect(); err != nil {
			return err
		}
	}

	if c.keyVaultClientFLE != nil && c.keyVaultClientFLE != c.internalClientFLE && c.keyVaultClientFLE != c {
		if err := c.keyVaultClientFLE.connect(); err != nil {
			return err
		}
	}

	if c.metadataClientFLE != nil && c.metadataClientFLE != c.internalClientFLE && c.metadataClientFLE != c {
		if err := c.metadataClientFLE.connect(); err != nil {
			return err
		}
	}

	var updateChan <-chan description.Topology
	if subscriber, ok := c.deployment.(driver.Subscriber); ok {
		sub, err := subscriber.Subscribe()
		if err != nil {
			return replaceErrors(err)
		}
		updateChan = sub.Updates
	}
	c.sessionPool = session.NewPool(updateChan)
	return nil
}

// Disconnect closes sockets to the topology referenced by this Client. It will
// shut down any monitoring goroutines, close the idle connection pool, and will
// wait until all the in use connections have been returned to the connection
// pool and closed before returning. If the context expires via cancellation,
// deadline, or timeout before the in use connections have returned, the in use
// connections will be closed, resulting in the failure of any in flight read
// or write operations. If this method returns with no errors, all connections
// associated with this Client have been closed.
func (c *Client) Disconnect(ctx context.Context) error {
	if c.logger != nil {
		defer c.logger.Close()
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if c.httpClient == httputil.DefaultHTTPClient {
		defer httputil.CloseIdleHTTPConnections(c.httpClient)
	}

	c.endSessions(ctx)
	if c.mongocryptdFLE != nil {
		if err := c.mongocryptdFLE.disconnect(ctx); err != nil {
			return err
		}
	}

	if c.internalClientFLE != nil {
		if err := c.internalClientFLE.Disconnect(ctx); err != nil {
			return err
		}
	}

	if c.keyVaultClientFLE != nil && c.keyVaultClientFLE != c.internalClientFLE && c.keyVaultClientFLE != c {
		if err := c.keyVaultClientFLE.Disconnect(ctx); err != nil {
			return err
		}
	}
	if c.metadataClientFLE != nil && c.metadataClientFLE != c.internalClientFLE && c.metadataClientFLE != c {
		if err := c.metadataClientFLE.Disconnect(ctx); err != nil {
			return err
		}
	}
	if c.cryptFLE != nil {
		c.cryptFLE.Close()
	}

	if disconnector, ok := c.deployment.(driver.Disconnector); ok {
		return replaceErrors(disconnector.Disconnect(ctx))
	}

	return nil
}

// Ping sends a ping command to verify that the client can connect to the deployment.
//
// The rp parameter is used to determine which server is selected for the operation.
// If it is nil, the client's read preference is used.
//
// If the server is down, Ping will try to select a server until the client's server selection timeout expires.
// This can be configured through the ClientOptions.SetServerSelectionTimeout option when creating a new Client.
// After the timeout expires, a server selection error is returned.
//
// Using Ping reduces application resilience because applications starting up will error if the server is temporarily
// unavailable or is failing over (e.g. during autoscaling due to a load spike).
func (c *Client) Ping(ctx context.Context, rp *readpref.ReadPref) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if rp == nil {
		rp = c.readPreference
	}

	db := c.Database("admin")
	res := db.RunCommand(ctx, bson.D{
		{"ping", 1},
	}, options.RunCmd().SetReadPreference(rp))

	return replaceErrors(res.Err())
}

// StartSession starts a new session configured with the given options.
//
// StartSession does not actually communicate with the server and will not error if the client is
// disconnected.
//
// StartSession is safe to call from multiple goroutines concurrently. However, Sessions returned by StartSession are
// not safe for concurrent use by multiple goroutines.
//
// If the DefaultReadConcern, DefaultWriteConcern, or DefaultReadPreference options are not set, the client's read
// concern, write concern, or read preference will be used, respectively.
func (c *Client) StartSession(opts ...options.Lister[options.SessionOptions]) (*Session, error) {
	sessArgs, err := mongoutil.NewOptions(opts...)
	if err != nil {
		return nil, err
	}
	if sessArgs.CausalConsistency == nil && (sessArgs.Snapshot == nil || !*sessArgs.Snapshot) {
		sessArgs.CausalConsistency = &options.DefaultCausalConsistency
	}
	coreOpts := &session.ClientOptions{
		DefaultReadConcern:    c.readConcern,
		DefaultReadPreference: c.readPreference,
		DefaultWriteConcern:   c.writeConcern,
	}
	if sessArgs.CausalConsistency != nil {
		coreOpts.CausalConsistency = sessArgs.CausalConsistency
	}
	if bldr := sessArgs.DefaultTransactionOptions; bldr != nil {
		txnOpts, err := mongoutil.NewOptions[options.TransactionOptions](bldr)
		if err != nil {
			return nil, err
		}

		if rc := txnOpts.ReadConcern; rc != nil {
			coreOpts.DefaultReadConcern = rc
		}

		if wc := txnOpts.WriteConcern; wc != nil {
			coreOpts.DefaultWriteConcern = wc
		}

		if rp := txnOpts.ReadPreference; rp != nil {
			coreOpts.DefaultReadPreference = rp
		}
	}
	if sessArgs.Snapshot != nil {
		coreOpts.Snapshot = sessArgs.Snapshot
	}

	sess, err := session.NewClientSession(c.sessionPool, c.id, coreOpts)
	if err != nil {
		return nil, replaceErrors(err)
	}

	return &Session{
		clientSession: sess,
		client:        c,
		deployment:    c.deployment,
	}, nil
}

func (c *Client) endSessions(ctx context.Context) {
	sessionIDs := c.sessionPool.IDSlice()
	op := operation.NewEndSessions(nil).ClusterClock(c.clock).Deployment(c.deployment).
		ServerSelector(&serverselector.ReadPref{ReadPref: readpref.PrimaryPreferred()}).
		CommandMonitor(c.monitor).Database("admin").Crypt(c.cryptFLE).ServerAPI(c.serverAPI)

	totalNumIDs := len(sessionIDs)
	var currentBatch []bsoncore.Document
	for i := 0; i < totalNumIDs; i++ {
		currentBatch = append(currentBatch, sessionIDs[i])

		// If we are at the end of a batch or the end of the overall IDs array, execute the operation.
		if ((i+1)%endSessionsBatchSize) == 0 || i == totalNumIDs-1 {
			// Ignore all errors when ending sessions.
			_, marshalVal, err := bson.MarshalValue(currentBatch)
			if err == nil {
				_ = op.SessionIDs(marshalVal).Execute(ctx)
			}

			currentBatch = currentBatch[:0]
		}
	}
}

func (c *Client) configureAutoEncryption(args *options.ClientOptions) error {
	c.encryptedFieldsMap = args.AutoEncryptionOptions.EncryptedFieldsMap
	if err := c.configureKeyVaultClientFLE(args); err != nil {
		return err
	}

	if err := c.configureMetadataClientFLE(args); err != nil {
		return err
	}

	mc, err := c.newMongoCrypt(args.AutoEncryptionOptions)
	if err != nil {
		return err
	}

	// If the crypt_shared library was not loaded, try to spawn and connect to mongocryptd.
	if mc.CryptSharedLibVersionString() == "" {
		mongocryptdFLE, err := newMongocryptdClient(args.AutoEncryptionOptions)
		if err != nil {
			return err
		}
		c.mongocryptdFLE = mongocryptdFLE
	}

	c.configureCryptFLE(mc, args.AutoEncryptionOptions)
	return nil
}

func (c *Client) getOrCreateInternalClient(args *options.ClientOptions) (*Client, error) {
	if c.internalClientFLE != nil {
		return c.internalClientFLE, nil
	}

	argsCopy := *args

	argsCopy.AutoEncryptionOptions = nil
	argsCopy.MinPoolSize = ptrutil.Ptr[uint64](0)

	var err error
	c.internalClientFLE, err = newClient(&argsCopy)

	return c.internalClientFLE, err
}

func (c *Client) configureKeyVaultClientFLE(clientOpts *options.ClientOptions) error {
	aeOpts := clientOpts.AutoEncryptionOptions

	var err error

	switch {
	case aeOpts.KeyVaultClientOptions != nil:
		c.keyVaultClientFLE, err = newClient(aeOpts.KeyVaultClientOptions)
	case clientOpts.MaxPoolSize != nil && *clientOpts.MaxPoolSize == 0:
		c.keyVaultClientFLE = c
	default:
		c.keyVaultClientFLE, err = c.getOrCreateInternalClient(clientOpts)
	}

	if err != nil {
		return err
	}

	dbName, collName := splitNamespace(aeOpts.KeyVaultNamespace)
	c.keyVaultCollFLE = c.keyVaultClientFLE.Database(dbName).Collection(collName, keyVaultCollOpts)
	return nil
}

func (c *Client) configureMetadataClientFLE(clientOpts *options.ClientOptions) error {
	aeOpts := clientOpts.AutoEncryptionOptions

	if aeOpts.BypassAutoEncryption != nil && *aeOpts.BypassAutoEncryption {
		// no need for a metadata client.
		return nil
	}
	if clientOpts.MaxPoolSize != nil && *clientOpts.MaxPoolSize == 0 {
		c.metadataClientFLE = c
		return nil
	}

	var err error
	c.metadataClientFLE, err = c.getOrCreateInternalClient(clientOpts)

	return err
}

func (c *Client) newMongoCrypt(opts *options.AutoEncryptionOptions) (*mongocrypt.MongoCrypt, error) {
	// convert schemas in SchemaMap to bsoncore documents
	cryptSchemaMap := make(map[string]bsoncore.Document)
	for k, v := range opts.SchemaMap {
		schema, err := marshal(v, c.bsonOpts, c.registry)
		if err != nil {
			return nil, err
		}
		cryptSchemaMap[k] = schema
	}

	// convert schemas in EncryptedFieldsMap to bsoncore documents
	cryptEncryptedFieldsMap := make(map[string]bsoncore.Document)
	for k, v := range opts.EncryptedFieldsMap {
		encryptedFields, err := marshal(v, c.bsonOpts, c.registry)
		if err != nil {
			return nil, err
		}
		cryptEncryptedFieldsMap[k] = encryptedFields
	}

	kmsProviders, err := marshal(opts.KmsProviders, c.bsonOpts, c.registry)
	if err != nil {
		return nil, fmt.Errorf("error creating KMS providers document: %w", err)
	}

	// Set the crypt_shared library override path from the "cryptSharedLibPath" extra option if one
	// was set.
	cryptSharedLibPath := ""
	if val, ok := opts.ExtraOptions["cryptSharedLibPath"]; ok {
		str, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf(
				`expected AutoEncryption extra option "cryptSharedLibPath" to be a string, but is a %T`, val)
		}
		cryptSharedLibPath = str
	}

	// Explicitly disable loading the crypt_shared library if requested. Note that this is ONLY
	// intended for use from tests; there is no supported public API for explicitly disabling
	// loading the crypt_shared library.
	cryptSharedLibDisabled := false
	if v, ok := opts.ExtraOptions["__cryptSharedLibDisabledForTestOnly"]; ok {
		cryptSharedLibDisabled = v.(bool)
	}

	bypassAutoEncryption := opts.BypassAutoEncryption != nil && *opts.BypassAutoEncryption
	bypassQueryAnalysis := opts.BypassQueryAnalysis != nil && *opts.BypassQueryAnalysis

	mc, err := mongocrypt.NewMongoCrypt(mcopts.MongoCrypt().
		SetKmsProviders(kmsProviders).
		SetLocalSchemaMap(cryptSchemaMap).
		SetBypassQueryAnalysis(bypassQueryAnalysis).
		SetEncryptedFieldsMap(cryptEncryptedFieldsMap).
		SetCryptSharedLibDisabled(cryptSharedLibDisabled || bypassAutoEncryption).
		SetCryptSharedLibOverridePath(cryptSharedLibPath).
		SetHTTPClient(opts.HTTPClient).
		SetKeyExpiration(opts.KeyExpiration))
	if err != nil {
		return nil, err
	}

	var cryptSharedLibRequired bool
	if val, ok := opts.ExtraOptions["cryptSharedLibRequired"]; ok {
		b, ok := val.(bool)
		if !ok {
			return nil, fmt.Errorf(
				`expected AutoEncryption extra option "cryptSharedLibRequired" to be a bool, but is a %T`, val)
		}
		cryptSharedLibRequired = b
	}

	// If the "cryptSharedLibRequired" extra option is set to true, check the MongoCrypt version
	// string to confirm that the library was successfully loaded. If the version string is empty,
	// return an error indicating that we couldn't load the crypt_shared library.
	if cryptSharedLibRequired && mc.CryptSharedLibVersionString() == "" {
		return nil, errors.New(
			`AutoEncryption extra option "cryptSharedLibRequired" is true, but we failed to load the crypt_shared library`)
	}

	return mc, nil
}

//nolint:unused // the unused linter thinks that this function is unreachable because "c.newMongoCrypt" always panics without the "cse" build tag set.
func (c *Client) configureCryptFLE(mc *mongocrypt.MongoCrypt, opts *options.AutoEncryptionOptions) {
	bypass := opts.BypassAutoEncryption != nil && *opts.BypassAutoEncryption
	kr := keyRetriever{coll: c.keyVaultCollFLE}
	var cir collInfoRetriever
	// If bypass is true, c.metadataClientFLE is nil and the collInfoRetriever
	// will not be used. If bypass is false, to the parent client or the
	// internal client.
	if !bypass {
		cir = collInfoRetriever{client: c.metadataClientFLE}
	}

	c.cryptFLE = driver.NewCrypt(&driver.CryptOptions{
		MongoCrypt:           mc,
		CollInfoFn:           cir.cryptCollInfo,
		KeyFn:                kr.cryptKeys,
		MarkFn:               c.mongocryptdFLE.markCommand,
		TLSConfig:            opts.TLSConfig,
		BypassAutoEncryption: bypass,
	})
}

// validSession returns an error if the session doesn't belong to the client
func (c *Client) validSession(sess *session.Client) error {
	if sess != nil && sess.ClientID != c.id {
		return ErrWrongClient
	}
	return nil
}

// Database returns a handle for a database with the given name configured with the given DatabaseOptions.
func (c *Client) Database(name string, opts ...options.Lister[options.DatabaseOptions]) *Database {
	return newDatabase(c, name, opts...)
}

// ListDatabases executes a listDatabases command and returns the result.
//
// The filter parameter must be a document containing query operators and can be used to select which
// databases are included in the result. It cannot be nil. An empty document (e.g. bson.D{}) should be used to include
// all databases.
//
// The opts parameter can be used to specify options for this operation (see the options.ListDatabasesOptions documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/listDatabases/.
func (c *Client) ListDatabases(ctx context.Context, filter interface{}, opts ...options.Lister[options.ListDatabasesOptions]) (ListDatabasesResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	sess := sessionFromContext(ctx)

	err := c.validSession(sess)
	if err != nil {
		return ListDatabasesResult{}, err
	}
	if sess == nil && c.sessionPool != nil {
		sess = session.NewImplicitClientSession(c.sessionPool, c.id)
		defer sess.EndSession()
	}

	err = c.validSession(sess)
	if err != nil {
		return ListDatabasesResult{}, err
	}

	filterDoc, err := marshal(filter, c.bsonOpts, c.registry)
	if err != nil {
		return ListDatabasesResult{}, err
	}

	var selector description.ServerSelector

	selector = &serverselector.Composite{
		Selectors: []description.ServerSelector{
			&serverselector.ReadPref{ReadPref: readpref.Primary()},
			&serverselector.Latency{Latency: c.localThreshold},
		},
	}

	selector = makeReadPrefSelector(sess, selector, c.localThreshold)

	lda, err := mongoutil.NewOptions(opts...)
	if err != nil {
		return ListDatabasesResult{}, err
	}
	op := operation.NewListDatabases(filterDoc).
		Session(sess).ReadPreference(c.readPreference).CommandMonitor(c.monitor).
		ServerSelector(selector).ClusterClock(c.clock).Database("admin").Deployment(c.deployment).Crypt(c.cryptFLE).
		ServerAPI(c.serverAPI).Timeout(c.timeout).Authenticator(c.authenticator)

	if lda.NameOnly != nil {
		op = op.NameOnly(*lda.NameOnly)
	}
	if lda.AuthorizedDatabases != nil {
		op = op.AuthorizedDatabases(*lda.AuthorizedDatabases)
	}

	retry := driver.RetryNone
	if c.retryReads {
		retry = driver.RetryOncePerCommand
	}
	op.Retry(retry)

	err = op.Execute(ctx)
	if err != nil {
		return ListDatabasesResult{}, replaceErrors(err)
	}

	return newListDatabasesResultFromOperation(op.Result()), nil
}

// ListDatabaseNames executes a listDatabases command and returns a slice containing the names of all of the databases
// on the server.
//
// The filter parameter must be a document containing query operators and can be used to select which databases
// are included in the result. It cannot be nil. An empty document (e.g. bson.D{}) should be used to include all
// databases.
//
// The opts parameter can be used to specify options for this operation (see the options.ListDatabasesOptions
// documentation.)
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/listDatabases/.
func (c *Client) ListDatabaseNames(ctx context.Context, filter interface{}, opts ...options.Lister[options.ListDatabasesOptions]) ([]string, error) {
	opts = append(opts, options.ListDatabases().SetNameOnly(true))

	res, err := c.ListDatabases(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0)
	for _, spec := range res.Databases {
		names = append(names, spec.Name)
	}

	return names, nil
}

// WithSession creates a new session context from the ctx and sess parameters
// and uses it to call the fn callback.
//
// WithSession is safe to call from multiple goroutines concurrently. However,
// the context passed to the WithSession callback function is not safe for
// concurrent use by multiple goroutines.
//
// If the ctx parameter already contains a Session, that Session will be
// replaced with the one provided.
//
// Any error returned by the fn callback will be returned without any
// modifications.
func WithSession(ctx context.Context, sess *Session, fn func(context.Context) error) error {
	return fn(NewSessionContext(ctx, sess))
}

// UseSession creates a new Session and uses it to create a new session context,
// which is used to call the fn callback. After the callback returns, the
// created Session is ended, meaning that any in-progress transactions started
// by fn will be aborted even if fn returns an error.
//
// UseSession is safe to call from multiple goroutines concurrently. However,
// the context passed to the UseSession callback function is not safe for
// concurrent use by multiple goroutines.
//
// If the ctx parameter already contains a Session, that Session will be
// replaced with the newly created one.
//
// Any error returned by the fn callback will be returned without any
// modifications.
func (c *Client) UseSession(ctx context.Context, fn func(context.Context) error) error {
	return c.UseSessionWithOptions(ctx, options.Session(), fn)
}

// UseSessionWithOptions operates like UseSession but uses the given
// SessionOptions to create the Session.
//
// UseSessionWithOptions is safe to call from multiple goroutines concurrently.
// However, the context passed to the UseSessionWithOptions callback function is
// not safe for concurrent use by multiple goroutines.
func (c *Client) UseSessionWithOptions(
	ctx context.Context,
	opts *options.SessionOptionsBuilder,
	fn func(context.Context) error,
) error {
	defaultSess, err := c.StartSession(opts)
	if err != nil {
		return err
	}

	defer defaultSess.EndSession(ctx)
	return fn(NewSessionContext(ctx, defaultSess))
}

// Watch returns a change stream for all changes on the deployment. See
// https://www.mongodb.com/docs/manual/changeStreams/ for more information about change streams.
//
// The client must be configured with read concern majority or no read concern for a change stream to be created
// successfully.
//
// The pipeline parameter must be an array of documents, each representing a pipeline stage. The pipeline cannot be
// nil or empty. The stage documents must all be non-nil. See https://www.mongodb.com/docs/manual/changeStreams/ for a list
// of pipeline stages that can be used with change streams. For a pipeline of bson.D documents, the mongo.Pipeline{}
// type can be used.
//
// The opts parameter can be used to specify options for change stream creation (see the options.ChangeStreamOptions
// documentation).
func (c *Client) Watch(ctx context.Context, pipeline interface{},
	opts ...options.Lister[options.ChangeStreamOptions]) (*ChangeStream, error) {
	csConfig := changeStreamConfig{
		readConcern:    c.readConcern,
		readPreference: c.readPreference,
		client:         c,
		bsonOpts:       c.bsonOpts,
		registry:       c.registry,
		streamType:     ClientStream,
		crypt:          c.cryptFLE,
	}

	return newChangeStream(ctx, csConfig, pipeline, opts...)
}

// NumberSessionsInProgress returns the number of sessions that have been started for this client but have not been
// closed (i.e. EndSession has not been called).
func (c *Client) NumberSessionsInProgress() int {
	// The underlying session pool uses an int64 for checkedOut to allow atomic
	// access. We convert to an int here to maintain backward compatibility with
	// older versions of the driver that did not atomically access checkedOut.
	return int(c.sessionPool.CheckedOut())
}

func (c *Client) createBaseCursorOptions() driver.CursorOptions {
	return driver.CursorOptions{
		CommandMonitor: c.monitor,
		Crypt:          c.cryptFLE,
		ServerAPI:      c.serverAPI,
	}
}

// ClientBulkWrite is a struct that can be used in a client-level BulkWrite operation.
type ClientBulkWrite struct {
	Database   string
	Collection string
	Model      ClientWriteModel
}

// BulkWrite performs a client-level bulk write operation.
func (c *Client) BulkWrite(ctx context.Context, writes []ClientBulkWrite,
	opts ...options.Lister[options.ClientBulkWriteOptions]) (*ClientBulkWriteResult, error) {
	// TODO(GODRIVER-3403): Remove after support for QE with Client.bulkWrite.
	if c.isAutoEncryptionSet {
		return nil, errors.New("bulkWrite does not currently support automatic encryption")
	}

	if len(writes) == 0 {
		return nil, fmt.Errorf("error from writes: %w", ErrEmptySlice)
	}
	bwo, err := mongoutil.NewOptions(opts...)
	if err != nil {
		return nil, err
	}

	if ctx == nil {
		ctx = context.Background()
	}

	sess := sessionFromContext(ctx)
	if sess == nil && c.sessionPool != nil {
		sess = session.NewImplicitClientSession(c.sessionPool, c.id)
		defer sess.EndSession()
	}

	err = c.validSession(sess)
	if err != nil {
		return nil, err
	}

	transactionRunning := sess.TransactionRunning()
	wc := c.writeConcern
	if transactionRunning {
		wc = nil
	}
	if bwo.WriteConcern != nil {
		if transactionRunning {
			return nil, errors.New("cannot set write concern after starting a transaction")
		}
		wc = bwo.WriteConcern
	}
	acknowledged := wc.Acknowledged()
	if !acknowledged {
		if bwo.Ordered == nil || *bwo.Ordered {
			return nil, errors.New("cannot request unacknowledged write concern and ordered writes")
		}
		sess = nil
	}

	writeSelector := &serverselector.Composite{
		Selectors: []description.ServerSelector{
			&serverselector.Write{},
			&serverselector.Latency{Latency: c.localThreshold},
		},
	}
	selector := makePinnedSelector(sess, writeSelector)

	writePairs := make([]clientBulkWritePair, len(writes))
	for i, w := range writes {
		writePairs[i] = clientBulkWritePair{
			namespace: fmt.Sprintf("%s.%s", w.Database, w.Collection),
			model:     w.Model,
		}
	}

	op := clientBulkWrite{
		writePairs:               writePairs,
		ordered:                  bwo.Ordered,
		bypassDocumentValidation: bwo.BypassDocumentValidation,
		comment:                  bwo.Comment,
		let:                      bwo.Let,
		session:                  sess,
		client:                   c,
		selector:                 selector,
		writeConcern:             wc,
	}
	if bwo.VerboseResults == nil || !(*bwo.VerboseResults) {
		op.errorsOnly = true
	} else if !acknowledged {
		return nil, errors.New("cannot request unacknowledged write concern and verbose results")
	}
	op.result.Acknowledged = acknowledged
	op.result.HasVerboseResults = !op.errorsOnly
	err = op.execute(ctx)
	return &op.result, replaceErrors(err)
}

// newLogger will use the LoggerOptions to create an internal logger and publish
// messages using a LogSink.
func newLogger(opts *options.LoggerOptions) (*logger.Logger, error) {
	// If there are no logger options, then create a default logger.
	if opts == nil {
		opts = options.Logger()
	}

	// If there are no component-level options and the environment does not
	// contain component variables, then do nothing.
	if len(opts.ComponentLevels) == 0 && !logger.EnvHasComponentVariables() {
		return nil, nil
	}

	// Otherwise, collect the component-level options and create a logger.
	componentLevels := make(map[logger.Component]logger.Level)
	for component, level := range opts.ComponentLevels {
		componentLevels[logger.Component(component)] = logger.Level(level)
	}

	return logger.New(opts.Sink, opts.MaxDocumentLength, componentLevels)
}
