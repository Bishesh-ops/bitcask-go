package raft

import (
	"sync"
)

// TransportPool maintains thread-safe connection wrappers for all cluster peers.
type TransportPool struct {
	mu    sync.RWMutex
	peers map[string]*PeerClient
}

func NewTransportPool() *TransportPool {
	return &TransportPool{
		peers: make(map[string]*PeerClient),
	}
}

// Get retrieves an existing connection wrapper or initializes a new one safely.
func (tp *TransportPool) Get(peerAddr string) *PeerClient {
	tp.mu.RLock()
	client, exists := tp.peers[peerAddr]
	tp.mu.RUnlock()

	if exists {
		return client
	}

	tp.mu.Lock()
	defer tp.mu.Unlock()
	// Double-check locking pattern to prevent race conditions during pool warmup
	if client, exists := tp.peers[peerAddr]; exists {
		return client
	}

	client = NewPeerClient(peerAddr)
	tp.peers[peerAddr] = client
	return client
}

func (tp *TransportPool) CloseAll() {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	for _, client := range tp.peers {
		_ = client.Close()
	}
}
