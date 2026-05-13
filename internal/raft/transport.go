package raft

import (
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

// connect establishes a socket connection with lazy initialization mechanics.
func (pc *PeerClient) connect() error {
	if pc.conn != nil {
		return nil
	}
	conn, err := net.DialTimeout("tcp", pc.addr, 2*time.Second)
	if err != nil {
		return err
	}
	pc.conn = conn
	return nil
}

// ExecRPC executes a synchronized Frame exchange across the network socket.
func (pc *PeerClient) ExecRPC(cmd uint8, payload []byte) (*wire.Frame, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if err := pc.connect(); err != nil {
		return nil, err
	}

	// Set deadlines to unblock threads if a network partition stalls TCP frames
	pc.conn.SetDeadline(time.Now().Add(3 * time.Second))

	if err := wire.WriteFrame(pc.conn, cmd, nil, payload); err != nil {
		pc.conn.Close()
		pc.conn = nil
		return nil, err
	}

	respFrame, err := wire.ReadFrame(pc.conn)
	if err != nil {
		pc.conn.Close()
		pc.conn = nil
		return nil, err
	}

	// Clear deadlines for next persistent interaction over this connection
	pc.conn.SetDeadline(time.Time{})
	return respFrame, nil
}

// Close drops the active TCP connection cleanly.
func (pc *PeerClient) Close() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
	}
}

// Transport manages dynamic outbound node pools to prevent ephemeral port starvation.
type Transport struct {
	mu    sync.RWMutex
	peers map[string]*PeerClient
}

func NewTransport() *Transport {
	return &Transport{
		peers: make(map[string]*PeerClient),
	}
}

// GetPeer retrieves an existing persistent client descriptor or spawns one safely.
func (t *Transport) GetPeer(addr string) *PeerClient {
	t.mu.RLock()
	client, exists := t.peers[addr]
	t.mu.RUnlock()

	if exists {
		return client
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	// Double-checked locking to avoid race-condition overrides during high concurrent access
	if client, exists := t.peers[addr]; exists {
		return client
	}

	client = NewPeerClient(addr)
	t.peers[addr] = client
	return client
}
