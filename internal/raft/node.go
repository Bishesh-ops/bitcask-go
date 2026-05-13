package raft

import (
	"github.com/bisheshops/bitcask-go/internal/engine"
	"github.com/bisheshops/bitcask-go/internal/wire"
	"math/rand"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Node struct {
	mu    sync.Mutex
	id    int
	peers []string //TCP addresses of the other cluster nodes
	state State

	// Persistent state on all servers
	currentTerm int
	votedFor    int      // CandidateID that received the vote in the current term (-1 if none)
	log         [][]byte // The Raft log

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Underlying storage engine
	db        *engine.DB
	transport *Transport
	// Timers and triggers
	heartbeatTimer *time.Timer
	electionTimer  *time.Timer
}

func NewNode(id int, peers []string, db *engine.DB) *Node {
	n := &Node{
		id:          id,
		peers:       peers,
		state:       Follower,
		votedFor:    -1,
		currentTerm: 0,
		db:          db,
		transport:   NewTransport(),
	}
	n.resetElectionTimeout()

	return n
}

func (n *Node) resetElectionTimeout() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()

	}
	d := time.Duration(150+rand.Intn(150)) * time.Millisecond
	n.electionTimer = time.AfterFunc(d, func() {
		n.startElection()
	})
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id // Vote for self
	term := n.currentTerm
	// Request votes from all peers concurrently using Goroutines
	n.resetElectionTimeout() // Reset timer in case this election results in a tie
	n.mu.Unlock()

	// Track votes received using a thread-safe counter
	votesReceived := 1
	var voteMu sync.Mutex

	for _, peerAddr := range n.peers {
		go func(peer string) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: len(n.log) - 1,
				LastLogTerm:  0, // Simplified for step 1
			}

			reply := n.sendRequestVoteRPC(peer, args)
			if reply == nil {
				return // Network Drop
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// If the cluster term moved on while we were waiting over the network, step down
			if n.state != Candidate || n.currentTerm != term {
				return
			}

			if reply.Term > n.currentTerm {
				n.state = Follower
				n.currentTerm = reply.Term
				n.votedFor = -1
				return
			}

			if reply.VoteGranted {
				voteMu.Lock()
				votesReceived++
				if votesReceived > (len(n.peers)+1)/2 { // Majority Quorum achieved!
					n.state = Leader
					n.startHeartbeats()
				}
				voteMu.Unlock()
			}
		}(peerAddr)
	}
}
func (n *Node) startHeartbeats() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	// Instantly broadcast empty AppendEntries packets to assert dominance
	/*for _, peerAddr := range n.peers {
		go func(peer string) {
			// Send periodic heartbeats every 50ms
		}(peerAddr)
	}*/
}

func (n *Node) sendRequestVoteRPC(peer string, args RequestVoteArgs) *RequestVoteReply {
	client := n.transport.GetPeer(peer)
	payload := args.Encode()

	respFrame, err := client.ExecRPC(wire.CmdRaftRequestVote, payload)
	if err != nil {
		return nil
	}
	if respFrame.Cmd != wire.CmdRaftRequestVoteReply {
		return nil
	}
	reply := DecodeRequestVoteReply(respFrame.Value)
	return &reply
}
