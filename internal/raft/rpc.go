package raft

import (
	"encoding/binary"
)

// Request sent by candidates to request for votes.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// Encode packs fixed integer fields into a contiguous byte slice
func (args *RequestVoteArgs) Encode() []byte {
	buf := make([]byte, 32)
	binary.BigEndian.PutUint64(buf[0:8], uint64(args.Term))
	binary.BigEndian.PutUint64(buf[8:16], uint64(args.CandidateID))
	binary.BigEndian.PutUint64(buf[16:24], uint64(args.LastLogIndex))
	binary.BigEndian.PutUint64(buf[24:32], uint64(args.LastLogTerm))
	return buf
}

func DecodeRequestVoteArgs(buf []byte) RequestVoteArgs {
	return RequestVoteArgs{
		Term:         int(binary.BigEndian.Uint64(buf[0:8])),
		CandidateID:  int(binary.BigEndian.Uint64(buf[8:16])),
		LastLogIndex: int(binary.BigEndian.Uint64(buf[16:24])),
		LastLogTerm:  int(binary.BigEndian.Uint64(buf[24:32])),
	}
}

// Response sent by the peers
type RequestVoteReply struct {
	Term        int  // This Term of candidate currently so it can update it
	VoteGranted bool // Wheather or not the candidate was voted for True meaning it received the vote.
}

func (reply *RequestVoteReply) Encode() []byte {
	buf := make([]byte, 9) // 8 bytes for Term + 1 byte boolean flag
	binary.BigEndian.PutUint64(buf[0:8], uint64(reply.Term))
	if reply.VoteGranted {
		buf[8] = 1
	} else {
		buf[8] = 0
	}
	return buf
}

func DecodeRequestVoteReply(buf []byte) RequestVoteReply {
	granted := false
	if buf[8] == 1 {
		granted = true
	}
	return RequestVoteReply{
		Term:        int(binary.BigEndian.Uint64(buf[0:8])),
		VoteGranted: granted,
	}
}

// AppendEntriesArgs  serves two purposes: transmitting log entries, and acting as Heartbeats.
type AppendEntriesArgs struct {
	Term         int // Leader's term
	LeaderID     int // So followers can redirect the clients
	PrevLogTerm  int
	PrevLogIndex int
	Entries      [][]byte
	LeaderCommit int
}

// AppendEntriesReply is retured by followers after processing relication.
type AppendEntriesReply struct {
	Term    int
	Success bool
}
