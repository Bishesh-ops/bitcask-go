package raft

import (
	"math/rand"
	"sync"
	"time"

	"github.com/bisheshops/bitcask-go/internal/engine"
	"github.com/bisheshops/bitcask-go/internal/wire"
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
	peers []string // TCP addresses of the other cluster nodes
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

	leaderID int // Tracks the current active leader (-1 if unknown)
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
		leaderID:    -1,
	}
	n.resetElectionTimeout()
	return n
}

func (n *Node) resetElectionTimeout() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	// Randomized timeout between 150ms and 300ms to prevent split votes
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
				LastLogTerm:  0,
			}

			reply := n.sendRequestVoteRPC(peer, args)
			if reply == nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// If the cluster term moved on while waiting over the network, step down
			if n.state != Candidate || n.currentTerm != term {
				return
			}

			if reply.Term > n.currentTerm {
				n.state = Follower
				n.currentTerm = reply.Term
				n.votedFor = -1
				n.leaderID = -1
				return
			}

			if reply.VoteGranted {
				voteMu.Lock()
				votesReceived++
				// Majority Quorum achieved! ((N/2) + 1)
				if votesReceived > (len(n.peers)+1)/2 {
					n.state = Leader
					n.leaderID = n.id
					n.startHeartbeats()
				}
				voteMu.Unlock()
			}
		}(peerAddr)
	}
}

// startHeartbeats asserts dominance by continuously broadcasting keep-alive frames.
func (n *Node) startHeartbeats() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	term := n.currentTerm
	leaderID := n.id

	go func(heartbeatTerm int, id int) {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			n.mu.Lock()

			if n.state != Leader || n.currentTerm != heartbeatTerm {
				n.mu.Unlock()
				return
			}

			commitIdx := n.commitIndex
			logLen := len(n.log)
			n.mu.Unlock()

			for _, peerAddr := range n.peers {
				go func(peer string) {
					args := AppendEntriesArgs{
						Term:         heartbeatTerm,
						LeaderID:     id,
						PrevLogIndex: logLen - 1,
						PrevLogTerm:  0,   // Simplified for heartbeat keep-alive phase
						Entries:      nil, // Empty slice denotes pure Keep-Alive heartbeat
						LeaderCommit: commitIdx,
					}

					client := n.transport.GetPeer(peer)
					payload := args.Encode()

					respFrame, err := client.ExecRPC(wire.CmdRaftAppendEntries, payload)
					if err != nil || respFrame == nil {
						return // Drop packet on network jitter
					}

					if respFrame.Cmd == wire.CmdRaftAppendEntriesReply {
						reply := DecodeAppendEntriesReply(respFrame.Value)
						n.mu.Lock()
						defer n.mu.Unlock()

						// Demote back to Follower if a peer reports a higher authoritative term
						if reply.Term > n.currentTerm {
							n.currentTerm = reply.Term
							n.state = Follower
							n.votedFor = -1
							n.leaderID = -1
							n.resetElectionTimeout()
						}
					}
				}(peerAddr)
			}
		}
	}(term, leaderID)
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

// HandleRequestVote processes incoming consensus network frames targeted at this node.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	if args.Term < n.currentTerm {
		return reply
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = -1
		n.leaderID = -1
	}

	if n.votedFor == -1 || n.votedFor == args.CandidateID {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.resetElectionTimeout() // Granting vote restarts idle timeout bounds
	}

	return reply
}

// HandleAppendEntries evaluates inbound leader traffic to guarantee state consistency.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{
		Term:    n.currentTerm,
		Success: false,
	}

	if args.Term < n.currentTerm {
		return reply
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = -1
	}

	n.state = Follower
	n.leaderID = args.LeaderID

	n.resetElectionTimeout()

	reply.Success = true
	return reply
}
