package raft

import (
	"sync"
)

// TransportPool maintains thread-safe connection wrappers for all cluster peers.
// It ensures that concurrent Raft RPCs reuse established TCP sessions efficiently.
type TransportPool struct {
	mu    sync.RWMutex
	peers map[string]*PeerClient
}

// NewTransportPool initializes an empty peer connection pool.
func NewTransportPool() *TransportPool {
	return &TransportPool{
		peers: make(map[string]*PeerClient),
	}
}

// Get retrieves an existing connection wrapper or safely initializes a new one.
// It uses a highly performant double-checked locking pattern to avoid global write locks on hot paths.
func (tp *TransportPool) Get(peerAddr string) *PeerClient {
	// First pass: highly concurrent read lock
	tp.mu.RLock()
	client, exists := tp.peers[peerAddr]
	tp.mu.RUnlock()

	if exists {
		return client
	}

	tp.mu.Lock()
	defer tp.mu.Unlock()

	if client, exists := tp.peers[peerAddr]; exists {
		return client
	}

	client = NewPeerClient(peerAddr)
	tp.peers[peerAddr] = client
	return client
}

func (tp *TransportPool) Remove(peerAddr string) {
	tp.mu.Lock()
	client, exists := tp.peers[peerAddr]
	if exists {
		delete(tp.peers, peerAddr)
	}
	tp.mu.Unlock()

	if exists && client != nil {
		client.Close()
	}
}

func (tp *TransportPool) CloseAll() {
	tp.mu.Lock()
	clients := make([]*PeerClient, 0, len(tp.peers))
	for _, client := range tp.peers {
		clients = append(clients, client)
	}
	tp.peers = make(map[string]*PeerClient)
	tp.mu.Unlock()

	for _, client := range clients {
		if client != nil {
			client.Close()
		}
	}
}
