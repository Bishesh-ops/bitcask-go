package raft

import (
	"encoding/binary"
)

// RequestVoteArgs is sent by candidates to request votes.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// Encode packs integer fields into a contiguous 32-byte Little Endian buffer.
func (args *RequestVoteArgs) Encode() []byte {
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(args.Term))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(args.CandidateID))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(args.LastLogIndex))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(args.LastLogTerm))
	return buf
}

func DecodeRequestVoteArgs(buf []byte) RequestVoteArgs {
	return RequestVoteArgs{
		Term:         int(binary.LittleEndian.Uint64(buf[0:8])),
		CandidateID:  int(binary.LittleEndian.Uint64(buf[8:16])),
		LastLogIndex: int(binary.LittleEndian.Uint64(buf[16:24])),
		LastLogTerm:  int(binary.LittleEndian.Uint64(buf[24:32])),
	}
}

// RequestVoteReply is sent by peers in response to a vote request.
type RequestVoteReply struct {
	Term        int  // Current Term of candidate so it can update itself
	VoteGranted bool // True means the candidate received the vote
}

func (reply *RequestVoteReply) Encode() []byte {
	buf := make([]byte, 9) // 8 bytes for Term + 1 byte boolean flag
	binary.LittleEndian.PutUint64(buf[0:8], uint64(reply.Term))
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
		Term:        int(binary.LittleEndian.Uint64(buf[0:8])),
		VoteGranted: granted,
	}
}

// AppendEntriesArgs serves log transmission and Heartbeats.
type AppendEntriesArgs struct {
	Term         int // Leader's term
	LeaderID     int // So followers can redirect clients
	PrevLogTerm  int
	PrevLogIndex int
	Entries      [][]byte
	LeaderCommit int
}

// AppendEntriesReply is returned by followers after processing replication.
type AppendEntriesReply struct {
	Term    int
	Success bool
}
