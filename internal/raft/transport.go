// internal/raft/transport.go
package raft

import (
	// "errors"
	"net"
	"sync"
	"time"

	"github.com/bisheshops/bitcask-go/internal/wire"
)

// PeerClient manages a single thread-safe persistent connection to a remote Raft peer.
type PeerClient struct {
	mu   sync.Mutex
	addr string
	conn net.Conn
}

func NewPeerClient(addr string) *PeerClient {
	return &PeerClient{addr: addr}
}

// getConn implements lazy auto-reconnection mechanics.
func (pc *PeerClient) getConn() (net.Conn, error) {
	if pc.conn != nil {
		return pc.conn, nil
	}
	conn, err := net.DialTimeout("tcp", pc.addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	pc.conn = conn
	return pc.conn, nil
}

func (pc *PeerClient) Close() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
	}
}

// ExecRPC serializes a raw binary command frame over the socket and reads the correlated response.
func (pc *PeerClient) ExecRPC(cmd uint8, payload []byte) (*wire.Frame, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	conn, err := pc.getConn()
	if err != nil {
		return nil, err
	}

	// Set operational deadlines to prevent stalled network Goroutines
	conn.SetDeadline(time.Now().Add(3 * time.Second))

	if err := wire.WriteFrame(conn, cmd, nil, payload); err != nil {
		pc.conn.Close() // Force drop stale socket on write failure
		pc.conn = nil
		return nil, err
	}

	respFrame, err := wire.ReadFrame(conn)
	if err != nil {
		pc.conn.Close()
		pc.conn = nil
		return nil, err
	}

	// Reset socket timeout to idle state
	conn.SetDeadline(time.Time{})
	return respFrame, nil
}

// Transport Layer manages all remote outbound connections.
type Transport struct {
	mu    sync.RWMutex
	peers map[string]*PeerClient
}

func NewTransport() *Transport {
	return &Transport{
		peers: make(map[string]*PeerClient),
	}
}

func (t *Transport) GetPeer(addr string) *PeerClient {
	t.mu.RLock()
	client, exists := t.peers[addr]
	t.mu.RUnlock()

	if exists {
		return client
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	// Double-check pattern to prevent connection pool race conditions
	if client, exists := t.peers[addr]; exists {
		return client
	}

	client = NewPeerClient(addr)
	t.peers[addr] = client
	return client
}
