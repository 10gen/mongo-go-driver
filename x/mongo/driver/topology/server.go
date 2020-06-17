// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package topology

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/address"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/operation"
)

const minHeartbeatInterval = 500 * time.Millisecond

// ErrServerClosed occurs when an attempt to Get a connection is made after
// the server has been closed.
var ErrServerClosed = errors.New("server is closed")

// ErrServerConnected occurs when at attempt to Connect is made after a server
// has already been connected.
var ErrServerConnected = errors.New("server is connected")

// SelectedServer represents a specific server that was selected during server selection.
// It contains the kind of the topology it was selected from.
type SelectedServer struct {
	*Server

	Kind description.TopologyKind
}

// Description returns a description of the server as of the last heartbeat.
func (ss *SelectedServer) Description() description.SelectedServer {
	sdesc := ss.Server.Description()
	return description.SelectedServer{
		Server: sdesc,
		Kind:   ss.Kind,
	}
}

// These constants represent the connection states of a server.
const (
	disconnected int32 = iota
	disconnecting
	connected
	connecting
	initialized
)

func connectionStateString(state int32) string {
	switch state {
	case 0:
		return "Disconnected"
	case 1:
		return "Disconnecting"
	case 2:
		return "Connected"
	case 3:
		return "Connecting"
	case 4:
		return "Initialized"
	}

	return ""
}

// Server is a single server within a topology.
type Server struct {
	cfg             *serverConfig
	address         address.Address
	connectionstate int32

	// connection related fields
	pool *pool

	// goroutine management fields
	done          chan struct{}
	checkNow      chan struct{}
	disconnecting chan struct{}
	closewg       sync.WaitGroup

	// description related fields
	desc                   atomic.Value // holds a description.Server
	updateTopologyCallback atomic.Value
	averageRTTSet          bool
	averageRTT             time.Duration

	// subscriber related fields
	subLock             sync.Mutex
	subscribers         map[uint64]chan description.Server
	currentSubscriberID uint64
	subscriptionsClosed bool

	processErrorLock sync.Mutex
}

// updateTopologyCallback is a callback used to create a server that should be called when the parent Topology instance
// should be updated based on a new server description. The callback must return the server description that should be
// stored by the server.
type updateTopologyCallback func(description.Server) description.Server

// ConnectServer creates a new Server and then initializes it using the
// Connect method.
func ConnectServer(addr address.Address, updateCallback updateTopologyCallback, opts ...ServerOption) (*Server, error) {
	srvr, err := NewServer(addr, opts...)
	if err != nil {
		return nil, err
	}
	err = srvr.Connect(updateCallback)
	if err != nil {
		return nil, err
	}
	return srvr, nil
}

// NewServer creates a new server. The mongodb server at the address will be monitored
// on an internal monitoring goroutine.
func NewServer(addr address.Address, opts ...ServerOption) (*Server, error) {
	cfg, err := newServerConfig(opts...)
	if err != nil {
		return nil, err
	}

	s := &Server{
		cfg:     cfg,
		address: addr,

		done:          make(chan struct{}),
		checkNow:      make(chan struct{}, 1),
		disconnecting: make(chan struct{}),

		subscribers: make(map[uint64]chan description.Server),
	}
	s.desc.Store(description.NewDefaultServer(addr))

	pc := poolConfig{
		Address:     addr,
		MinPoolSize: cfg.minConns,
		MaxPoolSize: cfg.maxConns,
		MaxIdleTime: cfg.connectionPoolMaxIdleTime,
		PoolMonitor: cfg.poolMonitor,
	}

	connectionOpts := append(cfg.connectionOpts, withErrorHandlingCallback(s.ProcessHandshakeError))
	s.pool, err = newPool(pc, connectionOpts...)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Connect initializes the Server by starting background monitoring goroutines.
// This method must be called before a Server can be used.
func (s *Server) Connect(updateCallback updateTopologyCallback) error {
	if !atomic.CompareAndSwapInt32(&s.connectionstate, disconnected, connected) {
		return ErrServerConnected
	}
	s.desc.Store(description.NewDefaultServer(s.address))
	s.updateTopologyCallback.Store(updateCallback)
	go s.update()
	s.closewg.Add(1)
	return s.pool.connect()
}

// Disconnect closes sockets to the server referenced by this Server.
// Subscriptions to this Server will be closed. Disconnect will shutdown
// any monitoring goroutines, closeConnection the idle connection pool, and will
// wait until all the in use connections have been returned to the connection
// pool and are closed before returning. If the context expires via
// cancellation, deadline, or timeout before the in use connections have been
// returned, the in use connections will be closed, resulting in the failure of
// any in flight read or write operations. If this method returns with no
// errors, all connections associated with this Server have been closed.
func (s *Server) Disconnect(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&s.connectionstate, connected, disconnecting) {
		return ErrServerClosed
	}

	s.updateTopologyCallback.Store((updateTopologyCallback)(nil))

	// For every call to Connect there must be at least 1 goroutine that is
	// waiting on the done channel.
	select {
	case <-ctx.Done():
		// signal a disconnect and still wait for receiver of done
		// to finish.
		close(s.disconnecting)
		s.done <- struct{}{}
	case s.done <- struct{}{}:
	}
	err := s.pool.disconnect(ctx)
	if err != nil {
		return err
	}

	s.closewg.Wait()
	atomic.StoreInt32(&s.connectionstate, disconnected)

	return nil
}

// Connection gets a connection to the server.
func (s *Server) Connection(ctx context.Context) (driver.Connection, error) {

	if s.pool.monitor != nil {
		s.pool.monitor.Event(&event.PoolEvent{
			Type:    "ConnectionCheckOutStarted",
			Address: s.pool.address.String(),
		})
	}

	if atomic.LoadInt32(&s.connectionstate) != connected {
		return nil, ErrServerClosed
	}

	poolGen := s.pool.getGeneration()
	connImpl, err := s.pool.get(ctx)
	if err != nil {
		// The error has already been handled by connection.connect, which calls Server.ProcessHandshakeError.
		return nil, err
	}

	return &Connection{connection: connImpl}, nil
}

// ProcessHandshakeError implements SDAM error handling for errors that occur before a connection finishes handshaking.
func (s *Server) ProcessHandshakeError(err error) {
	if err == nil {
		return
	}
	wrappedConnErr := unwrapConnectionError(err)
	if wrappedConnErr == nil {
		return
	}

	// Since the only kind of ConnectionError we receive from pool.Get will be an initialization error, we should set
	// the description.Server appropriately.
	desc := description.NewServerFromError(s.address, wrappedConnErr)
	s.updateDescription(desc)
	s.pool.clear()
}

// Description returns a description of the server as of the last heartbeat.
func (s *Server) Description() description.Server {
	return s.desc.Load().(description.Server)
}

// SelectedDescription returns a description.SelectedServer with a Kind of
// Single. This can be used when performing tasks like monitoring a batch
// of servers and you want to run one off commands against those servers.
func (s *Server) SelectedDescription() description.SelectedServer {
	sdesc := s.Description()
	return description.SelectedServer{
		Server: sdesc,
		Kind:   description.Single,
	}
}

// Subscribe returns a ServerSubscription which has a channel on which all
// updated server descriptions will be sent. The channel will have a buffer
// size of one, and will be pre-populated with the current description.
func (s *Server) Subscribe() (*ServerSubscription, error) {
	if atomic.LoadInt32(&s.connectionstate) != connected {
		return nil, ErrSubscribeAfterClosed
	}
	ch := make(chan description.Server, 1)
	ch <- s.desc.Load().(description.Server)

	s.subLock.Lock()
	defer s.subLock.Unlock()
	if s.subscriptionsClosed {
		return nil, ErrSubscribeAfterClosed
	}
	id := s.currentSubscriberID
	s.subscribers[id] = ch
	s.currentSubscriberID++

	ss := &ServerSubscription{
		C:  ch,
		s:  s,
		id: id,
	}

	return ss, nil
}

// RequestImmediateCheck will cause the server to send a heartbeat immediately
// instead of waiting for the heartbeat timeout.
func (s *Server) RequestImmediateCheck() {
	select {
	case s.checkNow <- struct{}{}:
	default:
	}
}

// ProcessHandshakeError handles connection errors during the handshake.
func (s *Server) ProcessHandshakeError(err error, conn driver.Connection) {
	// ignore nil or stale error
	if err == nil || conn.Stale() {
		return
	}

	wrappedConnErr := unwrapConnectionError(err)
	if wrappedConnErr == nil {
		return
	}

	// Since the only kind of ConnectionError we receive from pool.Get will be an initialization
	// error, we should set the description.Server appropriately.
	s.updateDescription(description.NewServerFromError(s.address, wrappedConnErr, s.Description().TopologyVersion))
	s.pool.clear()
}

// ProcessError handles SDAM error handling and implements driver.ErrorProcessor.
func (s *Server) ProcessError(err error, conn driver.Connection) {
	s.processErrorLock.Lock()
	defer s.processErrorLock.Unlock()

	// ignore nil or stale error
	if err == nil || conn.Stale() {
		return
	}
	// Invalidate server description if not master or node recovering error occurs.
	// These errors can be reported as a command error or a write concern error.
	desc := conn.Description()
	if cerr, ok := err.(driver.Error); ok && (cerr.NodeIsRecovering() || cerr.NotMaster()) {
		// ignore stale error
		if description.CompareTopologyVersion(desc.TopologyVersion, cerr.TopologyVersion) >= 0 {
			return
		}

		// updates description to unknown
		s.updateDescription(description.NewServerFromError(s.address, err, cerr.TopologyVersion))
		s.RequestImmediateCheck()

		// If the node is shutting down or is older than 4.2, we synchronously clear the pool
		if cerr.NodeIsShuttingDown() || desc.WireVersion == nil || desc.WireVersion.Max < 8 {
			s.pool.clear()
		}
		return
	}
	if wcerr, ok := err.(driver.WriteConcernError); ok && (wcerr.NodeIsRecovering() || wcerr.NotMaster()) {
		// ignore stale error
		if description.CompareTopologyVersion(desc.TopologyVersion, wcerr.TopologyVersion) >= 0 {
			return
		}

		// updates description to unknown
		s.updateDescription(description.NewServerFromError(s.address, err, wcerr.TopologyVersion))
		s.RequestImmediateCheck()

		// If the node is shutting down or is older than 4.2, we synchronously clear the pool
		if wcerr.NodeIsShuttingDown() || desc.WireVersion == nil || desc.WireVersion.Max < 8 {
			s.pool.clear()
		}
		return
	}

	wrappedConnErr := unwrapConnectionError(err)
	if wrappedConnErr == nil {
		return
	}

	// Ignore transient timeout errors.
	if netErr, ok := wrappedConnErr.(net.Error); ok && netErr.Timeout() {
		return
	}
	if wrappedConnErr == context.Canceled || wrappedConnErr == context.DeadlineExceeded {
		return
	}

	// updates description to unknown
	s.updateDescription(description.NewServerFromError(s.address, err, desc.TopologyVersion))
	s.pool.clear()
}

// update handles performing heartbeats and updating any subscribers of the
// newest description.Server retrieved.
func (s *Server) update() {
	defer s.closewg.Done()
	heartbeatTicker := time.NewTicker(s.cfg.heartbeatInterval)
	rateLimiter := time.NewTicker(minHeartbeatInterval)
	defer heartbeatTicker.Stop()
	defer rateLimiter.Stop()
	checkNow := s.checkNow
	done := s.done

	var doneOnce bool
	defer func() {
		if r := recover(); r != nil {
			if doneOnce {
				return
			}
			// We keep this goroutine alive attempting to read from the done channel.
			<-done
		}
	}()

	var conn *connection
	var desc description.Server

	desc, conn = s.heartbeat(nil)
	s.updateDescription(desc)

	closeServer := func() {
		doneOnce = true
		s.subLock.Lock()
		for id, c := range s.subscribers {
			close(c)
			delete(s.subscribers, id)
		}
		s.subscriptionsClosed = true
		s.subLock.Unlock()
		if conn == nil || conn.nc == nil {
			return
		}
		conn.nc.Close()
	}
	for {
		select {
		case <-done:
			closeServer()
			return
		default:
		}

		select {
		case <-heartbeatTicker.C:
		case <-checkNow:
		case <-done:
			closeServer()
			return
		}

		select {
		case <-rateLimiter.C:
		case <-done:
			closeServer()
			return
		}

		desc, conn = s.heartbeat(conn)
		s.updateDescription(desc)
	}
}

// updateDescription handles updating the description on the Server, notifying
// subscribers, and potentially draining the connection pool. The initial
// parameter is used to determine if this is the first description from the
// server.
func (s *Server) updateDescription(desc description.Server) {
	defer func() {
		//  ¯\_(ツ)_/¯
		_ = recover()
	}()

	// Use the updateTopologyCallback to update the parent Topology and get the description that should be stored.
	callback, ok := s.updateTopologyCallback.Load().(updateTopologyCallback)
	if ok && callback != nil {
		desc = callback(desc)
	}
	s.desc.Store(desc)

	s.subLock.Lock()
	for _, c := range s.subscribers {
		select {
		// drain the channel if it isn't empty
		case <-c:
		default:
		}
		c <- desc
	}
	s.subLock.Unlock()
}

// heartbeat sends a heartbeat to the server using the given connection. The connection can be nil.
func (s *Server) heartbeat(conn *connection) (description.Server, *connection) {
	const maxRetry = 2
	var saved error
	var desc description.Server
	var set bool
	var err error
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-ctx.Done():
		case <-s.disconnecting:
			cancel()
		}
	}()

	for i := 1; i <= maxRetry; i++ {
		var now time.Time
		var descPtr *description.Server

		if conn != nil && conn.expired() {
			if conn.nc != nil {
				conn.nc.Close()
			}
			conn = nil
		}

		if conn == nil {
			opts := []ConnectionOption{
				WithConnectTimeout(func(time.Duration) time.Duration { return s.cfg.heartbeatTimeout }),
				WithReadTimeout(func(time.Duration) time.Duration { return s.cfg.heartbeatTimeout }),
				WithWriteTimeout(func(time.Duration) time.Duration { return s.cfg.heartbeatTimeout }),
			}
			opts = append(opts, s.cfg.connectionOpts...)
			// We override whatever handshaker is currently attached to the options with a basic
			// one because need to make sure we don't do auth.
			opts = append(opts, WithHandshaker(func(h Handshaker) Handshaker {
				now = time.Now()
				return operation.NewIsMaster().AppName(s.cfg.appname).Compressors(s.cfg.compressionOpts)
			}))

			// Override any command monitors specified in options with nil to avoid monitoring heartbeats.
			opts = append(opts, WithMonitor(func(*event.CommandMonitor) *event.CommandMonitor {
				return nil
			}))

			conn, err = newConnection(ctx, s.address, opts...)

			conn.connect(ctx)

			err = conn.wait()
			if err == nil {
				descPtr = &conn.desc
			}
		}

		// do a heartbeat because a new connection wasn't created so a handshake was not performed
		if descPtr == nil && err == nil {
			now = time.Now()
			op := operation.
				NewIsMaster().
				ClusterClock(s.cfg.clock).
				Deployment(driver.SingleConnectionDeployment{initConnection{conn}})
			err = op.Execute(ctx)
			if err == nil {
				tmpDesc := op.Result(s.address)
				descPtr = &tmpDesc
			} else {
				// close the connection here rather than in the error check below to avoid calling Close on a net.Conn
				// that wasn't successfully created
				_ = conn.close()
			}
		}

		// we do a retry if the server is connected, if succeed return new server desc (see below)
		if err != nil {
			saved = err
			conn = nil
			if wrappedConnErr := unwrapConnectionError(err); wrappedConnErr != nil {
				s.pool.clear()
				// If the server is not connected, give up and exit loop
				if s.Description().Kind == description.Unknown {
					break
				}
			}
			continue
		}

		desc = *descPtr
		delay := time.Since(now)
		desc = desc.SetAverageRTT(s.updateAverageRTT(delay))
		desc.HeartbeatInterval = s.cfg.heartbeatInterval
		set = true

		break
	}

	if !set {
		desc = description.NewServerFromError(s.address, saved, s.Description().TopologyVersion)
	}

	return desc, conn
}

func (s *Server) updateAverageRTT(delay time.Duration) time.Duration {
	if !s.averageRTTSet {
		s.averageRTT = delay
		s.averageRTTSet = true
	} else {
		alpha := 0.2
		s.averageRTT = time.Duration(alpha*float64(delay) + (1-alpha)*float64(s.averageRTT))
	}
	return s.averageRTT
}

// String implements the Stringer interface.
func (s *Server) String() string {
	desc := s.Description()
	connState := atomic.LoadInt32(&s.connectionstate)
	str := fmt.Sprintf("Addr: %s, Type: %s, State: %s",
		s.address, desc.Kind, connectionStateString(connState))
	if len(desc.Tags) != 0 {
		str += fmt.Sprintf(", Tag sets: %s", desc.Tags)
	}
	if connState == connected {
		str += fmt.Sprintf(", Average RTT: %d", desc.AverageRTT)
	}
	if desc.LastError != nil {
		str += fmt.Sprintf(", Last error: %s", desc.LastError)
	}

	return str
}

// ServerSubscription represents a subscription to the description.Server updates for
// a specific server.
type ServerSubscription struct {
	C  <-chan description.Server
	s  *Server
	id uint64
}

// Unsubscribe unsubscribes this ServerSubscription from updates and closes the
// subscription channel.
func (ss *ServerSubscription) Unsubscribe() error {
	ss.s.subLock.Lock()
	defer ss.s.subLock.Unlock()
	if ss.s.subscriptionsClosed {
		return nil
	}

	ch, ok := ss.s.subscribers[ss.id]
	if !ok {
		return nil
	}

	close(ch)
	delete(ss.s.subscribers, ss.id)

	return nil
}

// unwrapConnectionError returns the connection error wrapped by err, or nil if err does not wrap a connection error.
func unwrapConnectionError(err error) error {
	// This is essentially an implementation of errors.As to unwrap this error until we get a ConnectionError and then
	// return ConnectionError.Wrapped.

	connErr, ok := err.(ConnectionError)
	if ok {
		return connErr.Wrapped
	}

	driverErr, ok := err.(driver.Error)
	if !ok || !driverErr.NetworkError() {
		return nil
	}

	connErr, ok = driverErr.Wrapped.(ConnectionError)
	if ok {
		return connErr.Wrapped
	}

	return nil
}
